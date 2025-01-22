package sqala.macros

import sqala.static.statement.query.Query

import scala.quoted.*
import scala.language.experimental.namedTuples

private[sqala] object AnalysisClauseMacro:
    class QueryInfo(using val q: Quotes)(
        val createAtFrom: Boolean, 
        val groups: List[q.reflect.Term], 
        val isOneRow: Boolean, 
        val inGroup: Boolean, 
        val currentOrders: List[q.reflect.Term],
        val currentSelect: List[q.reflect.Term]
    )

    inline def analysis[T](inline x: T): Unit =
        ${ analysisMacro[T]('x) }

    def removeInlined(using q: Quotes)(term: q.reflect.Term): q.reflect.Term =
        import q.reflect.*

        term match
            case Inlined(None, Nil, t) => removeInlined(t)
            case _ => term

    def analysisMacro[T: Type](x: Expr[T])(using q: Quotes): Expr[Unit] =
        import q.reflect.*

        val term = removeInlined(x.asTerm) match
            case Block(_, t) => t

        analysisQueryMacro(term, Nil)

        '{ () }

    def analysisQueryMacro(using q: Quotes)(
        term: q.reflect.Term,
        groups: List[q.reflect.Term]
    ): (groups: List[q.reflect.Term], isOneRow: Boolean) =
        import q.reflect.*

        val info = analysisQueryClauseMacro(term, groups)
        if !info.createAtFrom then
            report.warning("The query contains multiple query contexts.")

        (groups = info.groups.asInstanceOf[List[q.reflect.Term]], isOneRow = info.isOneRow)

    def analysisQueryClauseMacro(using q: Quotes)(
        term: q.reflect.Term,
        groups: List[q.reflect.Term]
    ): QueryInfo =
        import q.reflect.*

        // TODO whereIf qualify groupBySets pivot join on limit offset connectBy union
        // TODO join的内联可能有成员方法和父类方法两种情况
        // TODO join的on
        removeInlined(term) match
            case Apply(Select(query, "filter" | "where"), filter :: Nil) => 
                val queryInfo = 
                    analysisQueryClauseMacro(query, groups)
                val (args, body) = analysisLambda(filter)
                val exprInfo = AnalysisExprMacro
                    .treeInfoMacro(body, args, queryInfo.groups.asInstanceOf[List[q.reflect.Term]], false)
                if exprInfo.hasAgg then
                    report.warning(
                        "Aggregate functions are not allowed in WHERE/ON.",
                        body.pos
                    )
                if exprInfo.hasWindow then
                    report.warning(
                        "Window functions are not allowed in WHERE/ON.",
                        body.pos
                    )
                queryInfo
            case Apply(Select(query, "having"), filter :: Nil) => 
                val queryInfo = 
                    analysisQueryClauseMacro(query, groups)
                val (args, body) = analysisLambda(filter)
                val exprInfo = AnalysisExprMacro
                    .treeInfoMacro(body, args, queryInfo.groups.asInstanceOf[List[q.reflect.Term]], false)
                if exprInfo.hasWindow then
                    report.warning(
                        "Window functions are not allowed in HAVING.",
                        body.pos
                    )
                if exprInfo.ungroupedPaths.nonEmpty then
                    val c = exprInfo.ungroupedPaths.head.mkString(".")
                    report.warning(
                        s"Column \"$c\" must appear in the GROUP BY clause or be used in an aggregate function.",
                        body.asExpr
                    )
                QueryInfo(
                    createAtFrom = queryInfo.createAtFrom, 
                    groups = queryInfo.groups.asInstanceOf[List[q.reflect.Term]], 
                    isOneRow = false, 
                    inGroup = true, 
                    currentOrders = Nil, 
                    currentSelect = Nil
                )
            case Apply(Apply(TypeApply(Select(query, "groupBy" | "groupByCube" | "groupByRollup"), _), group :: Nil), _) =>
                val queryInfo = 
                    analysisQueryClauseMacro(query, groups)
                val (args, body) = analysisLambda(group)
                val (terms, exprInfoList) =
                    body match
                        case Apply(TypeApply(Select(Ident(n), "apply"), _), terms) 
                            if n.startsWith("Tuple")
                        =>
                            terms -> 
                            terms.map: t => 
                                AnalysisExprMacro.treeInfoMacro(t, args, queryInfo.groups.asInstanceOf[List[q.reflect.Term]], false)
                        case _ =>
                            (body :: Nil) -> 
                            (AnalysisExprMacro.treeInfoMacro(body, args, queryInfo.groups.asInstanceOf[List[q.reflect.Term]], false) :: Nil)
                val hasAgg = 
                    exprInfoList.map(_.hasAgg).fold(false)(_ || _)
                val hasWindow =
                    exprInfoList.map(_.hasAgg).fold(false)(_ || _)
                val hasValue = 
                    exprInfoList.map(_.isValue).fold(false)(_ || _)
                if hasAgg then
                    report.warning(
                        "Aggregate functions are not allowed in GROUP BY.",
                        body.pos
                    )
                if hasWindow then
                    report.warning(
                        "Window functions are not allowed in GROUP BY.",
                        body.pos
                    )
                if hasValue then
                    report.warning(
                        "Value expressions are not allowed in GROUP BY.",
                        body.pos
                    )
                QueryInfo(
                    createAtFrom = queryInfo.createAtFrom, 
                    groups = terms ++ queryInfo.groups.asInstanceOf[List[q.reflect.Term]], 
                    isOneRow = false, 
                    inGroup = true, 
                    currentOrders = Nil, 
                    currentSelect = Nil
                )
            case Apply(Apply(TypeApply(Select(query, "map" | "select"), _), map :: Nil), _) =>
                val queryInfo = 
                    analysisQueryClauseMacro(query, groups)
                val (args, body) = analysisLambda(map)
                val (terms, exprInfoList) =
                    body match
                        case Apply(TypeApply(Select(Ident(n), "apply"), _), terms) 
                            if n.startsWith("Tuple")
                        =>
                            terms ->
                            terms.map: t => 
                                AnalysisExprMacro.treeInfoMacro(t, args, queryInfo.groups.asInstanceOf[List[q.reflect.Term]], false)
                        case Inlined(Some(Apply(_, Apply(TypeApply(Select(Ident(n), "apply"), _), terms) :: Nil)), _, _)
                            if n.startsWith("Tuple")
                        =>
                            terms ->
                            terms.map: t => 
                                AnalysisExprMacro.treeInfoMacro(t, args, queryInfo.groups.asInstanceOf[List[q.reflect.Term]], false)
                        case _ =>
                            (body :: Nil) ->
                            (AnalysisExprMacro.treeInfoMacro(body, args, queryInfo.groups.asInstanceOf[List[q.reflect.Term]], false) :: Nil)
                val hasAgg = 
                    exprInfoList.map(_.hasAgg).fold(false)(_ || _)
                val ungrouped =
                    exprInfoList.flatMap(_.ungroupedPaths).map(_.mkString("."))
                if !queryInfo.inGroup && hasAgg && ungrouped.nonEmpty then
                    val c = ungrouped.head
                    report.warning(
                        s"Column \"$c\" must appear in the GROUP BY clause or be used in an aggregate function.",
                        body.asExpr
                    )
                if queryInfo.inGroup && ungrouped.nonEmpty then
                    val c = ungrouped.head
                    report.warning(
                        s"Column \"$c\" must appear in the GROUP BY clause or be used in an aggregate function.",
                        body.asExpr
                    )
                QueryInfo(
                    createAtFrom = queryInfo.createAtFrom, 
                    groups = Nil, 
                    isOneRow = hasAgg, 
                    inGroup = false, 
                    currentOrders = queryInfo.currentOrders.asInstanceOf[List[q.reflect.Term]], 
                    currentSelect = terms
                )
            case Apply(Apply(TypeApply(Select(query, "sortBy" | "orderBy"), _), sort :: Nil), _) =>
                val queryInfo = 
                    analysisQueryClauseMacro(query, groups)
                val (args, body) = analysisLambda(sort)
                val terms =
                    body match
                        case Apply(TypeApply(Select(Ident(n), "apply"), _), terms) 
                            if n.startsWith("Tuple")
                        =>
                            terms
                        case _ =>
                            body :: Nil
                val orders = (queryInfo.currentOrders ++ terms).asInstanceOf[List[q.reflect.Term]]
                val exprInfoList = orders.map: t => 
                    AnalysisExprMacro.treeInfoMacro(t, args, queryInfo.groups.asInstanceOf[List[q.reflect.Term]], false)
                val hasAgg = 
                    exprInfoList.map(_.hasAgg).fold(false)(_ || _)
                val hasWindow =
                    exprInfoList.map(_.hasWindow).fold(false)(_ || _)
                val hasValue = 
                    exprInfoList.map(_.isValue).fold(false)(_ || _)
                val ungrouped =
                    exprInfoList.flatMap(_.ungroupedPaths).map(_.mkString("."))
                if !queryInfo.inGroup && hasAgg && ungrouped.nonEmpty then
                    val c = ungrouped.head
                    report.warning(
                        s"Column \"$c\" must appear in the GROUP BY clause or be used in an aggregate function.",
                        body.asExpr
                    )
                if queryInfo.inGroup && ungrouped.nonEmpty then
                    val c = ungrouped.head
                    report.warning(
                        s"Column \"$c\" must appear in the GROUP BY clause or be used in an aggregate function.",
                        body.asExpr
                    )
                if hasWindow then
                    report.warning(
                        "Window functions are not allowed in ORDER BY.",
                        body.pos
                    )
                if hasValue then
                    report.warning(
                        "Value expressions are not allowed in ORDER BY.",
                        body.pos
                    )
                QueryInfo(
                    createAtFrom = queryInfo.createAtFrom, 
                    groups = queryInfo.groups.asInstanceOf[List[q.reflect.Term]], 
                    isOneRow = false, 
                    inGroup = queryInfo.inGroup, 
                    currentOrders = orders.asInstanceOf[List[q.reflect.Term]], 
                    currentSelect = Nil
                )
            case Select(query, "distinct") =>
                val queryInfo = 
                    analysisQueryClauseMacro(query, groups)
                val orderExprs = queryInfo
                    .currentOrders
                    .map(o => AnalysisExprMacro.fetchOrderExprMacro(o.asInstanceOf[q.reflect.Term]))
                    .map(_.show)
                if !orderExprs.forall(queryInfo.currentSelect.map(_.show).contains) then
                    report.warning(
                        "For SELECT DISTINCT, ORDER BY expressions must appear in select list.",
                        query.pos
                    )
                QueryInfo(
                    createAtFrom = queryInfo.createAtFrom, 
                    groups = Nil, 
                    isOneRow = queryInfo.isOneRow, 
                    inGroup = false, 
                    currentOrders = Nil, 
                    currentSelect = Nil
                )
            case Inlined(Some(Apply(TypeApply(Ident("from"), _), _)), _, _) =>
                QueryInfo(true, groups, false, false, Nil, Nil)
            case t =>
                report.error(s"other$t")
                t.tpe.asType match
                    case '[Query[_]] =>
                        QueryInfo(false, groups, false, false, Nil, Nil)
                    case _ => QueryInfo(true, groups, false, false, Nil, Nil)

    def analysisLambda(using q: Quotes)(term: q.reflect.Term): (List[String], q.reflect.Term) =
        import q.reflect.*

        term match
            case Block(DefDef(_, _, _, Some(Block(DefDef(_, _, _, Some(Block(args, body))) :: Nil, _))) :: Nil, _) =>
                val argNames = args.asInstanceOf[List[ValDef]].map:
                    case ValDef(n, _, _) => n
                (argNames, body)
            case Block(DefDef(_, _, _, Some(Block(DefDef(_, args :: Nil, _, Some(body)) :: Nil, _))) :: Nil, _) =>
                val argNames = args.asInstanceOf[List[ValDef]].map:
                    case ValDef(n, _, _) => n
                (argNames, body)