package sqala.macros

import sqala.static.statement.query.Query

import scala.language.experimental.namedTuples
import scala.quoted.*

private[sqala] object AnalysisClauseMacro:
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
        groups: List[String]
    ): (groups: List[String], isOneRow: Boolean) =
        import q.reflect.*

        val info = analysisQueryClauseMacro(term, groups)
        if !info.createAtFrom then
            report.warning("The query contains multiple query contexts.")

        (groups = info.groups, isOneRow = info.isOneRow)

    def analysisQueryClauseMacro(using q: Quotes)(
        term: q.reflect.Term,
        groups: List[String]
    ): (createAtFrom: Boolean, groups: List[String], isOneRow: Boolean, inGroup: Boolean) =
        import q.reflect.*

        removeInlined(term) match
            case Apply(Select(query, "filter" | "where"), filter :: Nil) => 
                val queryInfo = 
                    analysisQueryClauseMacro(query, groups)
                val (args, body) = analysisLambda(filter)
                val exprInfo = AnalysisExprMacro.treeInfoMacro(body, args, queryInfo.groups, false)
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
                val exprInfo = AnalysisExprMacro.treeInfoMacro(body, args, queryInfo.groups, false)
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
                (queryInfo.createAtFrom, queryInfo.groups, false, true)
            case Apply(Apply(TypeApply(Select(query, "groupBy"), _), group :: Nil), _) =>
                val queryInfo = 
                    analysisQueryClauseMacro(query, groups)
                val (args, body) = analysisLambda(group)
                val (terms, exprInfoList) =
                    body match
                        case Apply(TypeApply(Select(Ident(n), "apply"), _), terms) 
                            if n.startsWith("Tuple")
                        =>
                            terms -> 
                            terms.map(t => AnalysisExprMacro.treeInfoMacro(t, args, queryInfo.groups, false))
                        case _ =>
                            (body :: Nil) -> 
                            (AnalysisExprMacro.treeInfoMacro(body, args, queryInfo.groups, false) :: Nil)
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
                (queryInfo.createAtFrom, terms.map(_.show) ++ queryInfo.groups, false, true)
            case Apply(Apply(TypeApply(Select(query, "map" | "select"), _), map :: Nil), _) =>
                val queryInfo = 
                    analysisQueryClauseMacro(query, groups)
                val (args, body) = analysisLambda(map)
                val exprInfoList =
                    body match
                        case Apply(TypeApply(Select(Ident(n), "apply"), _), terms) 
                            if n.startsWith("Tuple")
                        =>
                            terms.map(t => AnalysisExprMacro.treeInfoMacro(t, args, queryInfo.groups, false))
                        case Inlined(Some(Apply(_, Apply(TypeApply(Select(Ident(n), "apply"), _), terms) :: Nil)), _, _)
                            if n.startsWith("Tuple")
                        =>
                            terms.map(t => AnalysisExprMacro.treeInfoMacro(t, args, queryInfo.groups, false))
                        case _ =>
                            (AnalysisExprMacro.treeInfoMacro(body, args, queryInfo.groups, false) :: Nil)
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
                (queryInfo.createAtFrom, Nil, hasAgg, false)
            case Inlined(Some(Apply(TypeApply(Ident("from"), _), _)), _, _) =>
                (true, groups, false, false)
            case t =>
                report.error(s"other$t")
                t.tpe.asType match
                    case '[Query[_]] =>
                        (false, groups, false, false)
                    case _ => (true, groups, false, false)

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