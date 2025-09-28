package sqala.static.dsl.analysis

import sqala.static.dsl.statement.query.Query
import sqala.static.metadata.AsSqlExpr

import scala.quoted.{Expr, Quotes, Type}

private[sqala] object AnalysisMacro:
    inline def analysis[T](inline x: T): Unit =
        ${ AnalysisMacroImpl.analysis[T]('x) }

private[sqala] object AnalysisMacroImpl:
    case class ExprInfo(
        expr: Expr[?],
        isUngrouped: Boolean,
        isColumn: Boolean,
        isValue: Boolean,
        isAgg: Boolean,
        hasAgg: Boolean,
        hasWindow: Boolean,
        notInAggColumns: List[String]
    )

    enum ResultSize:
        case ZeroOrOne
        case Many

    case class QueryInfo(
        resultSize: ResultSize,
        grouping: Boolean,
        sortExprs: List[ExprInfo]
    )

    def binaryOperators: List[String] =
        List(
            "==", "!=", "===", "<>", ">", ">=", "<", "<=", "<=>",
            "+", "-", "*", "/", "%", "&&", "||",
            "like", "contains", "startsWith", "endsWith", "overlaps"
        )

    def unaryOperators: List[String] =
        List("unary_+", "unary_-", "unary_!")

    def removeEmptyInline(using q: Quotes)(term: q.reflect.Term): q.reflect.Term =
        import q.reflect.*

        term match
            case Inlined(_, Nil, t) =>
                removeEmptyInline(t)
            case Typed(t, _) => t
            case _ => term

    def splitFunc(using q: Quotes)(term: q.reflect.Term): (List[String], q.reflect.Term) =
        import q.reflect.*

        removeEmptyInline(term) match
            case Block(DefDef(_, _, _, Some(Block(DefDef(_, _, _, Some(Block(args, body))) :: Nil, _))) :: Nil, _) =>
                val valDefList = args.asInstanceOf[List[ValDef]]
                (valDefList.map(_.name), body)
            case Block(DefDef(_, _, _, Some(Block(args, body))) :: Nil, _) =>
                val valDefList = args.asInstanceOf[List[ValDef]]
                (valDefList.map(_.name), body)
            case Block(DefDef(_, args :: Nil, _, Some(body)) :: Nil, _) =>
                val valDefList = args.asInstanceOf[List[ValDef]]
                (valDefList.map(_.name), body)

    def analysis[T: Type](x: Expr[T])(using q: Quotes): Expr[Unit] =
        import q.reflect.*

        try
            Type.of[T] match
                case '[Query[?]] =>
                    val term = 
                        removeEmptyInline(x.asTerm) match
                            case Block(_, t) => removeEmptyInline(t)
                    analysisQuery(term)
        catch
            case _ =>
        
        '{ () }

    def analysisQuery(using q: Quotes)(term: q.reflect.Term): QueryInfo = 
        import q.reflect.*

        term match
            case Apply(Apply(TypeApply(Ident("from"), _), t :: Nil), _) =>
                analysisFrom(t)
                QueryInfo(ResultSize.Many, false, Nil)
            case Apply(Apply(Apply(TypeApply(Apply(TypeApply(Select(_, "filter" | "where" | "withFilter"), _), query :: Nil), _), cond :: Nil), _), _) =>
                analysisQuery(query)
                val (_, body) = splitFunc(cond)
                val info = createExprInfo(body)
                analysisUngrouped(info)
                if info.hasAgg then
                    report.error("Aggregate functions are not allowed in WHERE.", body.asExpr)
                if info.hasWindow then
                    report.error("Window functions are not allowed in WHERE.", body.asExpr)
                QueryInfo(ResultSize.Many, false, Nil)
            case Apply(Apply(TypeApply(Apply(TypeApply(Select(_, name@("map" | "select" | "mapDistinct" | "selectDistinct")), _), query :: Nil), _), select :: Nil), _) =>
                val queryInfo = analysisQuery(query)
                val (_, body) = splitFunc(select)
                val infoList = body match
                    case Apply(TypeApply(Select(Ident(typeName), "apply"), _), terms) 
                        if typeName.startsWith("Tuple")
                    =>
                        terms.map(createExprInfo)
                    case Inlined(Some(Apply(_, Apply(TypeApply(Select(Ident(n), "apply"), _), terms) :: Nil)), _, _)
                        if n.startsWith("Tuple")
                    =>
                        terms.map(createExprInfo)
                    case _ =>
                        createExprInfo(body) :: Nil
                infoList.foreach(analysisUngrouped)
                if name.endsWith("Distinct") then
                    val selectExprs = infoList.map(_.expr.show)
                    for s <- queryInfo.sortExprs do
                        if !selectExprs.contains(s.expr.show) then
                            report.error("For SELECT DISTINCT, ORDER BY expressions must appear in select list.", s.expr)
                val totalExprs = queryInfo.sortExprs ++ infoList
                val hasAgg = totalExprs.map(_.hasAgg).fold(false)(_ || _)
                if hasAgg then
                    for info <- totalExprs do
                        if info.notInAggColumns.nonEmpty then
                            report.error(
                                s"""Column "${info.notInAggColumns.head}" must appear in the GROUP BY clause or be used in an aggregate function.""",
                                info.expr
                            )
                    val size = 
                        if queryInfo.grouping then ResultSize.Many
                        else ResultSize.ZeroOrOne
                    QueryInfo(size, queryInfo.grouping, queryInfo.sortExprs)
                else
                    QueryInfo(ResultSize.Many, queryInfo.grouping, queryInfo.sortExprs)
            case Apply(Apply(TypeApply(Apply(TypeApply(Select(_, n), _), query :: Nil), _), group :: Nil), _) if n.startsWith("groupBy") =>
                analysisQuery(query)
                val (_, body) = splitFunc(group)
                val infoList = body match
                    case Apply(TypeApply(Select(Ident(typeName), "apply"), _), terms) 
                        if typeName.startsWith("Tuple")
                    =>
                        terms.map(createExprInfo)
                    case Inlined(Some(Apply(_, Apply(TypeApply(Select(Ident(n), "apply"), _), terms) :: Nil)), _, _)
                        if n.startsWith("Tuple")
                    =>
                        terms.map(createExprInfo)
                    case _ =>
                        createExprInfo(body) :: Nil
                infoList.foreach(analysisUngrouped)
                for info <- infoList do
                    if info.hasAgg then
                        report.error("Aggregate functions are not allowed in GROUP BY.", info.expr)
                    if info.hasWindow then
                        report.error("Window functions are not allowed in GROUP BY.", info.expr)
                    if info.isValue then
                        report.error("Values are not allowed in GROUP BY.", info.expr)
                QueryInfo(ResultSize.Many, true, Nil)
            case Apply(Apply(Apply(TypeApply(Apply(TypeApply(Select(_, "having"), _), query :: Nil), _), cond :: Nil), _), _) =>
                val queryInfo = analysisQuery(query)
                val (_, body) = splitFunc(cond)
                val info = createExprInfo(body)
                analysisUngrouped(info)
                if info.hasWindow then
                    report.error("Window functions are not allowed in HAVING.", body.asExpr)
                QueryInfo(ResultSize.Many, true, queryInfo.sortExprs)
            case Apply(Apply(TypeApply(Apply(TypeApply(Select(_, "sortBy" | "orderBy"), _), query :: Nil), _), order :: Nil), _) =>
                val queryInfo = analysisQuery(query)
                val (_, body) = splitFunc(order)
                val infoList = body match
                    case Apply(TypeApply(Select(Ident(typeName), "apply"), _), terms) 
                        if typeName.startsWith("Tuple")
                    =>
                        terms.map(createSortInfo)
                    case _ =>
                        createSortInfo(body) :: Nil
                infoList.foreach(analysisUngrouped)
                val totalSortExprs = queryInfo.sortExprs ++ infoList
                val hasAgg = totalSortExprs.map(_.hasAgg).fold(false)(_ || _)
                if hasAgg then
                    for info <- totalSortExprs do
                        if info.notInAggColumns.nonEmpty then
                            report.error(
                                s"""Column "${info.notInAggColumns.head}" must appear in the GROUP BY clause or be used in an aggregate function.""",
                                info.expr
                            )
                for info <- infoList do
                    if info.isValue then
                        report.error("Values are not allowed in ORDER BY.", info.expr)
                QueryInfo(ResultSize.Many, queryInfo.grouping, totalSortExprs)
            case Apply(Apply(TypeApply(Select(_, name@("take" | "limit" | "takeWithTies" | "limitWithTies")), _), query :: Nil), n :: Nil) =>
                val queryInfo = analysisQuery(query)
                if name.endsWith("WithTies") && queryInfo.sortExprs.isEmpty then
                    report.error(s"WITH TIES cannot be specified without ORDER BY clause.", n.asExpr)
                val size = n match
                    case Literal(IntConstant(0 | 1)) => ResultSize.ZeroOrOne
                    case _ => queryInfo.resultSize
                QueryInfo(size, queryInfo.grouping, queryInfo.sortExprs)
            case Apply(Apply(TypeApply(Select(_, "drop" | "offset"), _), query :: Nil), _) =>
                analysisQuery(query)
            case Apply(TypeApply(Select(_, "forUpdate" | "forUpdateNoWait" | "forUpdateSkipLocked" | "forShare" | "forShareNoWait" | "forShareSkipLocked"), _), query :: Nil) =>
                analysisQuery(query)
            case Apply(Apply(TypeApply(Apply(TypeApply(Select(Ident(_), "union" | "unionAll" | "except" | "exceptAll" | "intersect" | "intersectAll"), _), left :: Nil), _), right :: Nil), _) =>
                analysisQuery(left)
                analysisQuery(right)
                QueryInfo(ResultSize.Many, false, Nil)

    def analysisFrom(using q: Quotes)(term: q.reflect.Term): Unit =
        import q.reflect.*

        term match
            case Apply(Apply(Apply(TypeApply(Select(Apply(Apply(TypeApply(Apply(Apply(TypeApply(Ident("join" | "leftJoin" | "rightJoin" | "fullJoin"), _), left :: Nil), _), _), right :: Nil), _), "on"), _), cond :: Nil), _), _) =>
                analysisFrom(left)
                analysisFrom(right)
                val (_, body) = splitFunc(cond)
                val info = createExprInfo(body)
                analysisUngrouped(info)
                if info.hasAgg then
                    report.error("Aggregate functions are not allowed in ON.", body.asExpr)
                if info.hasWindow then
                    report.error("Window functions are not allowed in ON.", body.asExpr)
            case Apply(Apply(TypeApply(Apply(Apply(TypeApply(Ident("crossJoin"), _), left :: Nil), _), _), right :: Nil), _) =>
                analysisFrom(left)
                analysisFrom(right)
            case _ =>
                term.tpe.asType match
                    case '[Query[?]] =>
                        analysisQuery(term)
                    case _ =>

    def analysisUngrouped(info: ExprInfo)(using q: Quotes): Unit =
        import q.reflect.*
        
        if info.isUngrouped && info.notInAggColumns.nonEmpty then
            report.error(
                s"""Column "${info.notInAggColumns.head}" must appear in the GROUP BY clause or be used in an aggregate function.""",
                info.expr
            )

    def createExprInfo(using q: Quotes)(term: q.reflect.Term): ExprInfo =
        import q.reflect.*

        term match
            case TypeApply(
                Select(
                    Apply(
                        Select(o@Ident(objectName), "selectDynamic"), 
                        Literal(StringConstant(valName)) :: Nil
                    ),
                    "$asInstanceOf$"
                ),
                _
            ) =>
                val isUngrouped =
                    o.tpe.widen.show.contains("Ungrouped")
                ExprInfo(
                    expr = term.asExpr,
                    isUngrouped = isUngrouped,
                    isColumn = true,
                    isValue = false,
                    isAgg = false,
                    hasAgg = false,
                    hasWindow = false,
                    notInAggColumns = s"$objectName.$valName" :: Nil
                )
            case TypeApply(
                Select(
                    Inlined(
                        Some(
                            Apply(
                                Select(o@Ident(objectName), "selectDynamic"), 
                                Literal(StringConstant(valName)) :: Nil
                            )
                        ),
                        _,
                        _
                    ),
                    "$asInstanceOf$"
                ),
                _
            ) =>
                val isUngrouped =
                    o.tpe.widen.show.contains("Ungrouped")
                ExprInfo(
                    expr = term.asExpr,
                    isUngrouped = isUngrouped,
                    isColumn = true,
                    isValue = false,
                    isAgg = false,
                    hasAgg = false,
                    hasWindow = false,
                    notInAggColumns = s"$objectName.$valName" :: Nil
                )
            case Apply(Apply(TypeApply(Select(left, op), _), right :: Nil), _) 
                if binaryOperators.contains(op)
            =>
                createBinaryExprInfo(term, left, right)
            case Apply(Apply(Apply(TypeApply(Apply(TypeApply(Ident(op), _), left :: Nil), _), right :: Nil), _), _) 
                if binaryOperators.contains(op)
            =>
                createBinaryExprInfo(term, left, right)
            case Apply(Apply(Apply(Apply(TypeApply(Ident(op), _), left :: Nil), right :: Nil), _), _) 
                if binaryOperators.contains(op)
            =>
                createBinaryExprInfo(term, left, right)
            case Apply(Apply(Apply(TypeApply(Ident(op), _), expr :: Nil), _), _) 
                if unaryOperators.contains(op)
            =>
                createExprInfo(expr)
            case Apply(Apply(TypeApply(Ident("isNull"), _), expr :: Nil), _) =>
                createExprInfo(expr)
            case Apply(Apply(Apply(TypeApply(Ident("isJson" | "isJsonArray" | "isJsonObject" | "isJsonScalar"), _), expr :: Nil), _), _) =>
                createExprInfo(expr)
            case Apply(Apply(Apply(TypeApply(Apply(TypeApply(Ident("between"), _), expr :: Nil), _), start :: end :: Nil), _), _) =>
                createBetweenExprInfo(term, expr, start, end)
            case Apply(Apply(Apply(TypeApply(Apply(TypeApply(Ident("in"), _), left :: Nil), _), right :: Nil), _), _) =>
                createInExprInfo(term, left, right)
            case Apply(Apply(TypeApply(Apply(TypeApply(Ident("as"), _), expr :: Nil), _), _), _) =>
                createExprInfo(expr)
            case Apply(TypeApply(Select(Ident(typeName), "apply"), _), terms) 
                if typeName.startsWith("Tuple")
            =>
                createTupleExprInfo(term, terms)
            case t@Apply(Apply(Apply(TypeApply(Select(_, "else"), _), _), _), _) =>
                createCaseExprInfo(t)
            case Apply(Apply(Apply(TypeApply(Ident("coalesce" | "ifNull" | "nullIf"), _), x :: y :: Nil), _), _) =>
                createBinaryExprInfo(term, x, y)
            case Apply(Apply(Apply(TypeApply(Ident("extract"), _), expr :: _ :: Nil), _), _) =>
                createExprInfo(expr)
            case Apply(Apply(Ident("jsonObject"), x :: Nil), _) =>
                createJsonObjectExprInfo(term, x)
            case Apply(Apply(TypeApply(Ident("grouping"), _), expr :: Nil), _) =>
                createGroupingExprInfo(term, expr)
            case _ 
                if term.symbol.annotations.exists:
                    case Apply(Select(New(TypeIdent("aggFunction")), _), _) => true
                    case _ => false
            =>
                createAggExprInfo(term)
            case _ 
                if term.symbol.annotations.exists:
                    case Apply(Select(New(TypeIdent("function")), _), _) => true
                    case _ => false
            =>
                createFuncExprInfo(term)
            case _ 
                if term.symbol.annotations.exists:
                    case Apply(Select(New(TypeIdent("windowFunction")), _), _) => true
                    case _ => false
            =>
                report.errorAndAbort("Window function requires an OVER clause.", term.asExpr)
            case Apply(Select(window, "over"), Nil) =>
                createOverExprInfo(term, window, None)
            case Apply(Apply(Select(window, "over"), Block(DefDef(_, _, _, Some(over)) :: Nil, _) :: Nil), _) =>
                createOverExprInfo(term, window, Some(over))
            case Apply(Apply(TypeApply(Ident(op), _), query :: Nil), _) 
                if List("exists", "any", "all").contains(op)
            =>
                analysisQuery(query)
                ExprInfo(
                    expr = term.asExpr,
                    isUngrouped = false,
                    isColumn = false,
                    isValue = false,
                    isAgg = false,
                    hasAgg = false,
                    hasWindow = false,
                    notInAggColumns = Nil
                )
            case Apply(Apply(TypeApply(Ident("asExpr"), _), expr :: Nil), _) =>
                createExprInfo(expr)
            case _ =>
                term.tpe.widen.asType match
                    case '[Query[?]] =>
                        val queryInfo = analysisQuery(term)
                        if queryInfo.resultSize == ResultSize.Many then
                            report.error("More than one row returned by a subquery used as an expression.", term.asExpr)
                        ExprInfo(
                            expr = term.asExpr,
                            isUngrouped = false,
                            isColumn = false,
                            isValue = false,
                            isAgg = false,
                            hasAgg = false,
                            hasWindow = false,
                            notInAggColumns = Nil
                        )
                    case '[Unit] =>
                        ExprInfo(
                            expr = term.asExpr,
                            isUngrouped = false,
                            isColumn = false,
                            isValue = false,
                            isAgg = false,
                            hasAgg = false,
                            hasWindow = false,
                            notInAggColumns = Nil
                        )
                    case '[t] =>
                        val asSqlExpr = Expr.summon[AsSqlExpr[t]]
                        ExprInfo(
                            expr = term.asExpr,
                            isUngrouped = false,
                            isColumn = false,
                            isValue = asSqlExpr.isDefined,
                            isAgg = false,
                            hasAgg = false,
                            hasWindow = false,
                            notInAggColumns = Nil
                        )

    def createBinaryExprInfo(using q: Quotes)(
        term: q.reflect.Term,
        left: q.reflect.Term, 
        right: q.reflect.Term
    ): ExprInfo =
        val leftInfo = createExprInfo(left)
        val rightInfo = createExprInfo(right)
        ExprInfo(
            expr = term.asExpr,
            isUngrouped = leftInfo.isUngrouped || rightInfo.isUngrouped,
            isColumn = false,
            isValue = false,
            isAgg = false,
            hasAgg = leftInfo.hasAgg || rightInfo.hasAgg,
            hasWindow = leftInfo.hasWindow || rightInfo.hasWindow,
            notInAggColumns = leftInfo.notInAggColumns ++ rightInfo.notInAggColumns
        )

    def createBetweenExprInfo(using q: Quotes)(
        term: q.reflect.Term,
        expr: q.reflect.Term,
        start: q.reflect.Term, 
        end: q.reflect.Term
    ): ExprInfo =
        val exprInfo = createExprInfo(expr)
        val startInfo = createExprInfo(start)
        val endInfo = createExprInfo(end)
        ExprInfo(
            expr = term.asExpr,
            isUngrouped = exprInfo.isUngrouped || startInfo.isUngrouped || endInfo.isUngrouped,
            isColumn = false,
            isValue = false,
            isAgg = false,
            hasAgg = exprInfo.hasAgg || startInfo.hasAgg || endInfo.hasAgg,
            hasWindow = exprInfo.hasWindow || startInfo.hasWindow || endInfo.hasWindow,
            notInAggColumns = exprInfo.notInAggColumns ++ startInfo.notInAggColumns ++ endInfo.notInAggColumns
        )

    def createInExprInfo(using q: Quotes)(
        term: q.reflect.Term,
        left: q.reflect.Term, 
        right: q.reflect.Term
    ): ExprInfo =
        val leftInfo = createExprInfo(left)
        val rightInfo = right.tpe.widen.asType match
            case '[type t <: Seq[?] | Array[?]; t] =>
                ExprInfo(
                    expr = right.asExpr,
                    isUngrouped = false,
                    isColumn = false,
                    isValue = false,
                    isAgg = false,
                    hasAgg = false,
                    hasWindow = false,
                    notInAggColumns = Nil
                )
            case '[Query[?]] =>
                analysisQuery(right)
                ExprInfo(
                    expr = right.asExpr,
                    isUngrouped = false,
                    isColumn = false,
                    isValue = false,
                    isAgg = false,
                    hasAgg = false,
                    hasWindow = false,
                    notInAggColumns = Nil
                )
            case _ =>
                createExprInfo(right)
        ExprInfo(
            expr = term.asExpr,
            isUngrouped = leftInfo.isUngrouped || rightInfo.isUngrouped,
            isColumn = false,
            isValue = false,
            isAgg = false,
            hasAgg = leftInfo.hasAgg || rightInfo.hasAgg,
            hasWindow = leftInfo.hasWindow || rightInfo.hasWindow,
            notInAggColumns = leftInfo.notInAggColumns ++ rightInfo.notInAggColumns
        )

    def createTupleExprInfo(using q: Quotes)(
        term: q.reflect.Term,
        exprs: List[q.reflect.Term]
    ): ExprInfo =
        val infoList = exprs.map(createExprInfo)
        ExprInfo(
            expr = term.asExpr,
            isUngrouped = infoList.map(_.isUngrouped).fold(false)(_ || _),
            isColumn = false,
            isValue = false,
            isAgg = false,
            hasAgg = infoList.map(_.hasAgg).fold(false)(_ || _),
            hasWindow = infoList.map(_.hasWindow).fold(false)(_ || _),
            notInAggColumns = infoList.flatMap(_.notInAggColumns)
        )

    def createCaseExprInfo(using q: Quotes)(
        term: q.reflect.Term
    ): ExprInfo =
        import q.reflect.*

        def matchRecursive(term: Term): List[ExprInfo] =
            term match
                case Apply(Apply(Apply(TypeApply(Select(t, "else"), _), elseTerm :: Nil), _), _) =>
                    matchRecursive(t) :+ createExprInfo(elseTerm)
                case Apply(Apply(Apply(TypeApply(Select(t, "then"), _), thenTerm :: Nil), _), _) =>
                    matchRecursive(t) :+ createExprInfo(thenTerm)
                case Apply(Apply(Apply(TypeApply(Select(t, "else if"), _), ifTerm :: Nil), _), _) =>
                    matchRecursive(t) :+ createExprInfo(ifTerm)
                case Apply(Apply(TypeApply(Select(t, "then"), _), thenTerm :: Nil), _) =>
                   matchRecursive(t) :+ createExprInfo(thenTerm)
                case Apply(Apply(Apply(TypeApply(Ident("if"), _), ifTerm :: Nil), _), _) =>
                    createExprInfo(ifTerm) :: Nil

        val infoList = matchRecursive(term)
        ExprInfo(
            expr = term.asExpr,
            isUngrouped = infoList.map(_.isUngrouped).fold(false)(_ || _),
            isColumn = false,
            isValue = false,
            isAgg = false,
            hasAgg = infoList.map(_.hasAgg).fold(false)(_ || _),
            hasWindow = infoList.map(_.hasWindow).fold(false)(_ || _),
            notInAggColumns = infoList.flatMap(_.notInAggColumns)
        )

    def createFuncExprInfo(using q: Quotes)(
        term: q.reflect.Term
    ): ExprInfo =
        import q.reflect.*

        val params = 
            term.symbol.paramSymss
                .flatMap(a => a)
                .filter(a => !a.isTypeParam && !a.flags.is(Flags.Given))
        val args = term match
            case Apply(Apply(Apply(func, args), _), _) =>
                args
            case Apply(Apply(func, args), _) =>
                args
            case Apply(func, args) =>
                args
        val argsMap =
            params.map(_.name).zip(
                args.map:
                    case NamedArg(_, arg) => arg
                    case arg => arg
            ).toMap
        var isUngrouped = false
        var hasAgg = false
        var hasWindow = false
        var notInAggColumns = List[String]()
        if argsMap.contains("orderBy") || argsMap.contains("sortBy") then
            report.error("ORDER BY specified, but this expression is not an aggregate function.", term.asExpr)
        if argsMap.contains("withinGroup") then
            report.error("WITHIN GROUP specified, but this expression is not an aggregate function.", term.asExpr)
        if argsMap.contains("filter") then
            report.error("FILTER specified, but this expression is not an aggregate function.", term.asExpr)
        val argInfoList = argsMap
            .filterKeys(k => !List("orderBy", "sortBy", "withinGroup", "filter").contains(k))
            .values
            .toList
            .map(createExprInfo)
        for o <- argInfoList do
            isUngrouped = isUngrouped || o.isUngrouped
            hasAgg = hasAgg || o.hasAgg
            hasWindow = hasWindow || o.hasWindow
            notInAggColumns = notInAggColumns ++ o.notInAggColumns
        ExprInfo(
            expr = term.asExpr,
            isUngrouped = isUngrouped,
            isColumn = false,
            isValue = false,
            isAgg = false,
            hasAgg = hasAgg,
            hasWindow = hasWindow,
            notInAggColumns = notInAggColumns
        )
    
    def createWindowExprInfo(using q: Quotes)(
        term: q.reflect.Term
    ): ExprInfo =
        import q.reflect.*

        val params = 
            term.symbol.paramSymss
                .flatMap(a => a)
                .filter(a => !a.isTypeParam && !a.flags.is(Flags.Given))
        val args = term match
            case Apply(Apply(Apply(func, args), _), _) =>
                args
            case Apply(Apply(func, args), _) =>
                args
            case Apply(func, args) =>
                args
        val argsMap =
            params.map(_.name).zip(
                args.map:
                    case NamedArg(_, arg) => arg
                    case arg => arg
            ).toMap
        var isUngrouped = false
        var hasAgg = false
        var hasWindow = false
        var notInAggColumns = List[String]()
        if argsMap.contains("orderBy") || argsMap.contains("sortBy") then
            report.error("ORDER BY specified, but this expression is not an aggregate function.", term.asExpr)
        if argsMap.contains("withinGroup") then
            report.error("WITHIN GROUP specified, but this expression is not an aggregate function.", term.asExpr)
        if argsMap.contains("filter") then
            report.error("FILTER specified, but this expression is not an aggregate function.", term.asExpr)
        val argInfoList = argsMap
            .filterKeys(k => !List("orderBy", "sortBy", "withinGroup", "filter").contains(k))
            .values
            .toList
            .map(createExprInfo)
        for o <- argInfoList do
            isUngrouped = isUngrouped || o.isUngrouped
            hasAgg = hasAgg || o.hasAgg
            hasWindow = hasWindow || o.hasWindow
            notInAggColumns = notInAggColumns ++ o.notInAggColumns
        if hasWindow then
            report.error("Window function calls cannot be nested.", term.asExpr)
        ExprInfo(
            expr = term.asExpr,
            isUngrouped = isUngrouped,
            isColumn = false,
            isValue = false,
            isAgg = false,
            hasAgg = hasAgg,
            hasWindow = true,
            notInAggColumns = notInAggColumns
        )

    def createAggExprInfo(using q: Quotes)(
        term: q.reflect.Term
    ): ExprInfo =
        import q.reflect.*

        val params = 
            term.symbol.paramSymss
                .flatMap(a => a)
                .filter(a => !a.isTypeParam && !a.flags.is(Flags.Given))
        val args = term match
            case Apply(Apply(Apply(func, args), _), _) =>
                args
            case Apply(Apply(func, args), _) =>
                args
            case Apply(func, args) =>
                args
        val argsMap =
            params.map(_.name).zip(
                args.map:
                    case NamedArg(_, arg) => arg
                    case arg => arg
            ).toMap
        var hasAgg = false
        var hasWindow = false
        val orderByInfo = argsMap
            .get("orderBy")
            .orElse(argsMap.get("sortBy"))
            .map(createSortInfo)
        for o <- orderByInfo do
            hasAgg = hasAgg || o.hasAgg
            hasWindow = hasWindow || o.hasWindow
        val withinGroupInfo = argsMap
            .get("withinGroup")
            .map(createSortInfo)
        for w <- withinGroupInfo do
            hasAgg = hasAgg || w.hasAgg
            hasWindow = hasWindow || w.hasWindow
        val filterInfo = argsMap
            .get("filter")
            .map(createExprInfo)
        for f <- filterInfo do
            hasAgg = hasAgg || f.hasAgg
            hasWindow = hasWindow || f.hasWindow
        val argInfoList = argsMap
            .filterKeys(k => !List("orderBy", "sortBy", "withinGroup", "filter").contains(k))
            .values
            .toList
            .map(createExprInfo)
        for o <- argInfoList do
            hasAgg = hasAgg || o.hasAgg
            hasWindow = hasWindow || o.hasWindow
        if hasAgg then
            report.error("Aggregate function calls cannot be nested.", term.asExpr)
        if hasWindow then
            report.error("Aggregate function calls cannot contain window function calls.", term.asExpr)
        ExprInfo(
            expr = term.asExpr,
            isUngrouped = false,
            isColumn = false,
            isValue = false,
            isAgg = true,
            hasAgg = true,
            hasWindow = false,
            notInAggColumns = Nil
        )

    def createOverExprInfo(using q: Quotes)(
        term: q.reflect.Term,
        window: q.reflect.Term,
        over: Option[q.reflect.Term]
    ): ExprInfo =
        import q.reflect.*

        def createOverInfo(over: q.reflect.Term): List[ExprInfo] =
            over match
                case Apply(Apply(TypeApply(Ident("partitionBy"), _), partition :: Nil), _) =>
                    createExprInfo(partition) :: Nil
                case Apply(Apply(TypeApply(Ident("sortBy" | "orderBy"), _), order :: Nil), _) =>
                    createSortInfo(order) :: Nil
                case Apply(Apply(TypeApply(Select(o, "sortBy" | "orderBy"), _), order :: Nil), _) =>
                    createOverInfo(o) :+ createSortInfo(order)
                case Apply(Apply(Select(o, "rowsBetween" | "rangeBetween" | "groupsBetween"), _), _) =>
                    createOverInfo(o)
                case Apply(Apply(Select(o, "rows" | "range" | "groups"), _), _) =>
                    createOverInfo(o)

        val isAgg = window.symbol.annotations.exists:
            case Apply(Select(New(TypeIdent("aggFunction")), _), _) => true
            case _ => false
        val isWindow = window.symbol.annotations.exists:
            case Apply(Select(New(TypeIdent("windowFunction")), _), _) => true
            case _ => false
        if !isAgg && !isWindow then
            report.error("OVER specified, but this expression is not a window function nor an aggregate function.", window.asExpr)
        val windowInfo = 
            if isAgg then createAggExprInfo(window)
            else createWindowExprInfo(window)
        val overInfoList = over match
            case None => Nil
            case Some(overTerm) => createOverInfo(overTerm)
        val infoList = windowInfo :: overInfoList
        ExprInfo(
            expr = term.asExpr,
            isUngrouped = infoList.map(_.isUngrouped).fold(false)(_ || _),
            isColumn = false,
            isValue = false,
            isAgg = false,
            hasAgg = infoList.map(_.hasAgg).fold(false)(_ || _),
            hasWindow = true,
            notInAggColumns = infoList.flatMap(_.notInAggColumns)
        )

    def createGroupingExprInfo(using q: Quotes)(
        term: q.reflect.Term,
        expr: q.reflect.Term
    ): ExprInfo =
        import q.reflect.*

        val info = createExprInfo(expr)
        if info.hasAgg then
            report.error("Aggregate function calls cannot be nested.", term.asExpr)
        if info.hasWindow then
            report.error("Aggregate function calls cannot contain window function calls.", term.asExpr)
        if info.isUngrouped || info.isValue || info.isColumn then
            report.error("Arguments to GROUPING must be grouping expressions of the associated query level.", term.asExpr)
        ExprInfo(
            expr = term.asExpr,
            isUngrouped = false,
            isColumn = false,
            isValue = false,
            isAgg = false,
            hasAgg = true,
            hasWindow = false,
            notInAggColumns = Nil
        )

    def createJsonObjectExprInfo(using q: Quotes)(
        term: q.reflect.Term,
        expr: q.reflect.Term
    ): ExprInfo =
        import q.reflect.*

        val infoList = expr match
            case Typed(Repeated(pair, _), _) =>
                pair.map:
                    case p@Apply(Apply(TypeApply(Apply(TypeApply(Ident("value"), _), x :: Nil), _), y :: Nil), _) =>
                        createBinaryExprInfo(p, x, y)
        ExprInfo(
            expr = term.asExpr,
            isUngrouped = infoList.map(_.isUngrouped).fold(false)(_ || _),
            isColumn = false,
            isValue = false,
            isAgg = false,
            hasAgg = infoList.map(_.hasAgg).fold(false)(_ || _),
            hasWindow = infoList.map(_.hasWindow).fold(false)(_ || _),
            notInAggColumns = infoList.flatMap(_.notInAggColumns)
        )

    def createSortInfo(using q: Quotes)(
        term: q.reflect.Term
    ): ExprInfo =
        import q.reflect.*

        term match
            case Apply(Apply(Apply(TypeApply(Ident("asc"), _), expr :: Nil), _), _) => 
                createExprInfo(expr)
            case Apply(Apply(Apply(TypeApply(Ident("ascNullsFirst"), _), expr :: Nil), _), _) => 
                createExprInfo(expr)
            case Apply(Apply(Apply(TypeApply(Ident("ascNullsLast"), _), expr :: Nil), _), _) => 
                createExprInfo(expr)
            case Apply(Apply(Apply(TypeApply(Ident("desc"), _), expr :: Nil), _), _) => 
                createExprInfo(expr)
            case Apply(Apply(Apply(TypeApply(Ident("descNullsFirst"), _), expr :: Nil), _), _) => 
                createExprInfo(expr)
            case Apply(Apply(Apply(TypeApply(Ident("descNullsLast"), _), expr :: Nil), _), _) => 
                createExprInfo(expr)
            case _ =>
                createExprInfo(term)