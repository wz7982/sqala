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

        analysisQuery(term, Nil)

        '{ () }

    def analysisQuery(using q: Quotes)(
        term: q.reflect.Term,
        groups: List[q.reflect.Term]
    ): (groups: List[q.reflect.Term], isOneRow: Boolean) =
        import q.reflect.*

        val info = analysisQueryClause(term, groups)
        if !info.createAtFrom then
            report.warning("The query contains multiple query contexts.")

        (groups = info.groups.asInstanceOf[List[q.reflect.Term]], isOneRow = info.isOneRow)

    def analysisQueryClause(using q: Quotes)(
        term: q.reflect.Term,
        groups: List[q.reflect.Term]
    ): QueryInfo =
        import q.reflect.*

        removeInlined(term) match
            case Apply(Select(query, "filter" | "where" | "on" | "startWith"), filter :: Nil) =>
                analysisFilter(query, filter, groups, false)
            case Apply(Apply(Select(query, "filterIf" | "whereIf"), _), filter :: Nil) =>
                analysisFilter(query, filter, groups, false)
            case Apply(Apply(TypeApply(Select(_, "connectBy"), _), query :: Nil), filter :: Nil) =>
                analysisFilter(query, filter, groups, true)
            case Apply(Select(query, "having"), filter :: Nil) => 
                val queryInfo = 
                    analysisQueryClause(query, groups)
                val (args, body) = analysisLambda(filter)
                val exprInfo = AnalysisExprMacro
                    .analysisTreeInfo(body, args, queryInfo.groups.asInstanceOf[List[q.reflect.Term]], false)
                if exprInfo.hasWindow then
                    report.warning(
                        "Window functions are not allowed in HAVING.",
                        body.pos
                    )
                QueryInfo(
                    createAtFrom = queryInfo.createAtFrom, 
                    groups = queryInfo.groups.asInstanceOf[List[q.reflect.Term]], 
                    isOneRow = false, 
                    inGroup = true, 
                    currentOrders = Nil, 
                    currentSelect = Nil
                )
            case Apply(Apply(TypeApply(Select(query, "groupBy"), _), group :: Nil), _) =>
                analysisGroup(query, group, groups)
            case Apply(Apply(Apply(TypeApply(Select(query, n), _), _), group :: Nil), _) 
                if n.startsWith("groupBy")
            =>
                analysisGroup(query, group, groups)
            case Apply(Apply(TypeApply(Select(query, "map" | "select"), _), map :: Nil), _) =>
                val queryInfo = 
                    analysisQueryClause(query, groups)
                val (args, body) = analysisLambda(map)
                val (terms, exprInfoList) =
                    body match
                        case Apply(TypeApply(Select(Ident(n), "apply"), _), terms) 
                            if n.startsWith("Tuple")
                        =>
                            terms ->
                            terms.map: t => 
                                AnalysisExprMacro.analysisTreeInfo(t, args, queryInfo.groups.asInstanceOf[List[q.reflect.Term]], false)
                        case Inlined(Some(Apply(_, Apply(TypeApply(Select(Ident(n), "apply"), _), terms) :: Nil)), _, _)
                            if n.startsWith("Tuple")
                        =>
                            terms ->
                            terms.map: t => 
                                AnalysisExprMacro.analysisTreeInfo(t, args, queryInfo.groups.asInstanceOf[List[q.reflect.Term]], false)
                        case _ =>
                            (body :: Nil) ->
                            (AnalysisExprMacro.analysisTreeInfo(body, args, queryInfo.groups.asInstanceOf[List[q.reflect.Term]], false) :: Nil)
                val hasAgg = 
                    exprInfoList.map(_.hasAgg).fold(false)(_ || _)
                val notInAgg =
                    exprInfoList.flatMap(_.notInAggPaths).map(_.mkString("."))
                if !queryInfo.inGroup && hasAgg && notInAgg.nonEmpty then
                    val c = notInAgg.head
                    report.warning(
                        s"Column \"$c\" must appear in the GROUP BY clause or be used in an aggregate function.",
                        body.pos
                    )
                QueryInfo(
                    createAtFrom = queryInfo.createAtFrom, 
                    groups = Nil, 
                    isOneRow = !queryInfo.inGroup && hasAgg, 
                    inGroup = false, 
                    currentOrders = queryInfo.currentOrders.asInstanceOf[List[q.reflect.Term]], 
                    currentSelect = terms
                )
            case Apply(Apply(TypeApply(Select(query, n), _), sort :: Nil), _) 
                if n.startsWith("sort") || n.startsWith("order")
            =>
                val queryInfo = 
                    analysisQueryClause(query, groups)
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
                    AnalysisExprMacro.analysisTreeInfo(t, args, queryInfo.groups.asInstanceOf[List[q.reflect.Term]], false)
                val hasAgg = 
                    exprInfoList.map(_.hasAgg).fold(false)(_ || _)
                val hasWindow =
                    exprInfoList.map(_.hasWindow).fold(false)(_ || _)
                val hasValue = 
                    exprInfoList.map(_.isValue).fold(false)(_ || _)
                val notInAgg =
                    exprInfoList.flatMap(_.notInAggPaths).map(_.mkString("."))
                if !queryInfo.inGroup && hasAgg && notInAgg.nonEmpty then
                    val c = notInAgg.head
                    report.warning(
                        s"Column \"$c\" must appear in the GROUP BY clause or be used in an aggregate function.",
                        body.pos
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
                    analysisQueryClause(query, groups)
                val orderExprs = queryInfo
                    .currentOrders
                    .map(o => AnalysisExprMacro.fetchOrderExpr(o.asInstanceOf[q.reflect.Term]))
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
            case Apply(Select(query, "drop" | "offset"), _) =>
                val queryInfo = 
                    analysisQueryClause(query, groups)
                QueryInfo(
                    createAtFrom = queryInfo.createAtFrom, 
                    groups = Nil, 
                    isOneRow = queryInfo.isOneRow, 
                    inGroup = false, 
                    currentOrders = Nil, 
                    currentSelect = Nil
                )
            case Apply(Select(query, "take" | "limit"), n :: Nil) =>
                val queryInfo = 
                    analysisQueryClause(query, groups)
                val isOneRow = n match
                    case Literal(IntConstant(1)) => true
                    case _ => queryInfo.isOneRow
                QueryInfo(
                    createAtFrom = queryInfo.createAtFrom, 
                    groups = Nil, 
                    isOneRow = isOneRow, 
                    inGroup = false, 
                    currentOrders = Nil, 
                    currentSelect = Nil
                )
            case Apply(Select(query, "maxDepth"), _) =>
                analysisQueryClause(query, groups)
            case Inlined(Some(Apply(TypeApply(Ident("from"), _), _)), _, _) =>
                QueryInfo(true, groups, false, false, Nil, Nil)
            case Inlined(
                Some(
                    Apply(
                        Apply(TypeApply(Ident("fromQuery"), _), Block(DefDef(_, _, _, Some(query)) :: Nil, _) :: Nil), 
                        _
                    )
                ), 
                _, 
                _
            ) =>
                val queryInfo = analysisQueryClause(query, groups)
                QueryInfo(queryInfo.createAtFrom, groups, false, false, Nil, Nil)
            case Inlined(
                Some(
                    Apply(
                        Apply(TypeApply(Ident("fromValues" | "fromFunction"), _), _), 
                        _
                    )
                ), 
                _, 
                _
            ) =>
                QueryInfo(true, groups, false, false, Nil, Nil)
            case Inlined(Some(Apply(TypeApply(Select(query, n), _), _)), _, _) 
                if n.toLowerCase.endsWith("join")
            =>
                analysisQueryClause(query, groups)
            case Block(_, Inlined(Some(Apply(TypeApply(Select(query, n), _), _)), _, _)) 
                if n.toLowerCase.endsWith("join")
            =>
                analysisQueryClause(query, groups)
            case Apply(Apply(TypeApply(Select(query, n), _), Block(DefDef(_, _, _, Some(join)) :: Nil, _) :: Nil), _) 
                if n == "joinQuery" || n == "leftJoinQuery" || n == "rightJoinQuery"
            =>
                val queryInfo = analysisQueryClause(query, groups)
                analysisQuery(join, groups)
                queryInfo
            case Apply(
                Apply(
                    TypeApply(Select(query, n), _), 
                    Block(DefDef(_, _, _, Some(Block(DefDef(_, _, _, Some(join)) :: Nil, _))) :: Nil, _) :: Nil
                ), 
                _
            )
                if n.endsWith("Lateral")
            =>
                val queryInfo = analysisQueryClause(query, groups)
                analysisQuery(join, groups)
                queryInfo
            case Apply(Apply(TypeApply(Select(left, n), _), right :: Nil), _) 
                if n.startsWith("union") || n.startsWith("except") || n.startsWith("intersect") || n == "++"
            =>
                val leftQueryInfo = 
                    analysisQueryClause(left, groups)
                val rightQueryInfo = 
                    analysisQueryClause(right, groups)
                QueryInfo(
                    createAtFrom = leftQueryInfo.createAtFrom && rightQueryInfo.createAtFrom, 
                    groups = Nil, 
                    isOneRow = false, 
                    inGroup = false, 
                    currentOrders = Nil, 
                    currentSelect = Nil
                )
            case t =>
                t.tpe.asType match
                    case '[Query[_]] =>
                        QueryInfo(false, groups, false, false, Nil, Nil)
                    case _ => QueryInfo(true, groups, false, false, Nil, Nil)

    def analysisFilter(using q: Quotes)(
        query: q.reflect.Term, 
        filter: q.reflect.Term, 
        groups: List[q.reflect.Term],
        inConnectBy: Boolean
    ): QueryInfo =
        import q.reflect.*

        val queryInfo = 
            analysisQueryClause(query, groups)
        val (args, body) = analysisLambda(filter)
        val exprInfo = AnalysisExprMacro
            .analysisTreeInfo(body, args, queryInfo.groups.asInstanceOf[List[q.reflect.Term]], inConnectBy)
        val clauseName = if inConnectBy then "CONNECT BY" else "WHERE/ON"
        if exprInfo.hasAgg then
            report.warning(
                s"Aggregate functions are not allowed in $clauseName.",
                body.pos
            )
        if exprInfo.hasWindow then
            report.warning(
                s"Window functions are not allowed in $clauseName.",
                body.pos
            )
        queryInfo

    def analysisGroup(using q: Quotes)(
        query: q.reflect.Term, 
        group: q.reflect.Term, 
        groups: List[q.reflect.Term]
    ): QueryInfo =
        import q.reflect.*

        val queryInfo = 
            analysisQueryClause(query, groups)
        val (args, body) = analysisLambda(group)
        def fetchGroups(term: Term): List[(Term, AnalysisExprMacro.ExprInfo)] =
            term match
                case Apply(TypeApply(Select(Ident(n), "apply"), _), terms) 
                    if n.startsWith("Tuple")
                =>
                    terms.flatMap(t => fetchGroups(t))
                case _ =>
                    (
                        term,
                        AnalysisExprMacro.treeInfo(term, args, queryInfo.groups.asInstanceOf[List[q.reflect.Term]], false)
                    ) :: Nil
        val currentGroups = fetchGroups(body)
        val terms = currentGroups.map(_._1)
        val exprInfoList = currentGroups.map(_._2)
        val ungrouped = 
            exprInfoList.flatMap(_.ungroupedPaths)
        for u <- ungrouped if !args.contains(u.head) do
            val c = u.mkString(".")
            report.warning(
                s"Column \"$c\" must appear in the GROUP BY clause or be used in an aggregate function.",
                body.pos
            )
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