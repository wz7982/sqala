package sqala.macros

import sqala.common.AsSqlExpr
import sqala.static.statement.query.Query

import scala.quoted.*
import scala.language.experimental.namedTuples

private[sqala] object AnalysisExprMacro:
    case class ExprInfo(
        hasAgg: Boolean,
        isAgg: Boolean,
        hasWindow: Boolean,
        isValue: Boolean,
        isGroup: Boolean,
        ungroupedPaths: List[List[String]]
    )

    def binaryOperators: List[String] =
        List(
            "==", "!=", ">", ">=", "<", "<=", "&&", "||",
            "+", "-", "*", "/", "%", "->", "->>",
            "like", "contains", "startsWith", "endsWith"
        )

    def unaryOperators: List[String] =
        List("unary_+", "unary_-", "unary_!", "prior")

    def validateDiv(using q: Quotes)(term: q.reflect.Term): Unit =
        import q.reflect.*

        val isZero = term match
            case Literal(IntConstant(0)) => true
            case Literal(LongConstant(0L)) => true
            case Literal(FloatConstant(0F)) => true
            case Literal(DoubleConstant(0D)) => true
            case _ => false
        if isZero then report.warning("Division by zero.", term.pos)

    def treeInfoMacro(using q: Quotes)(
        term: q.reflect.Term,
        currentArgs: List[String],
        groups: List[q.reflect.Term],
        inConnectBy: Boolean
    ): ExprInfo =
        import q.reflect.*

        val isGroupExpr = groups.map(_.show).contains(term.show)

        val exprInfo = term match
            // TODO 多级引用q.t.c
            case TypeApply(
                Select(
                    Inlined(
                        Some(
                            Apply(
                                Select(Ident(objectName), "selectDynamic"), 
                                Literal(StringConstant(valName)) :: Nil
                            )
                        ), 
                        _, 
                        _
                    ), 
                    "$asInstanceOf$"
                )
                ,
                _
            ) =>
                ExprInfo(
                    hasAgg = false,
                    isAgg = false,
                    hasWindow = false,
                    isValue = false,
                    isGroup = false,
                    ungroupedPaths = (objectName :: valName :: Nil) :: Nil
                )
            case Apply(Apply(TypeApply(Select(left, op), _), right :: Nil), _)
                if binaryOperators.contains(op)
            =>
                binaryInfoMacro(left, op, right, currentArgs, groups, inConnectBy)
            case Apply(Apply(Select(left, op), right :: Nil), _)
                if binaryOperators.contains(op)
            =>
                binaryInfoMacro(left, op, right, currentArgs, groups, inConnectBy)
            case Apply(Apply(TypeApply(Apply(Apply(TypeApply(Ident(op), _), left :: Nil), _), _), right :: Nil), _)
                if binaryOperators.contains(op)
            =>
                binaryInfoMacro(left, op, right, currentArgs, groups, inConnectBy)
            case Apply(Select(expr, op), _) if op.startsWith("unary_") =>
                treeInfoMacro(expr, currentArgs, groups, inConnectBy)
            case Apply(TypeApply(Ident("prior"), _), expr :: Nil) =>
                if !inConnectBy then
                    report.warning("Prior can only be used in CONNECT BY.", term.pos)
                treeInfoMacro(expr, currentArgs, groups, inConnectBy)
            case Apply(Apply(TypeApply(Ident("asExpr"), _), t :: Nil), _) =>
                treeInfoMacro(t, currentArgs, groups, inConnectBy)
            case Apply(TypeApply(Select(_, "asExpr"), _), t :: Nil) =>
                treeInfoMacro(t, currentArgs, groups, inConnectBy)
            case _ if
                term.symbol.annotations.exists:
                    case Apply(Select(New(TypeIdent("sqlAgg")), _), _) => true
                    case _ => false
            =>
                val funcArgsRef = term match
                    case Apply(Apply(_, funcArgs), _) => funcArgs
                    case Apply(_, funcArgs) => funcArgs
                    case _ => Nil
                val funcArgs = funcArgsRef.flatMap:
                    case Typed(Repeated(a, _), _) => a
                    case a => a :: Nil
                val argsInfo =
                    funcArgs.map(a => treeInfoMacro(a, currentArgs, groups, inConnectBy))
                for a <- argsInfo do
                    if a.hasAgg then
                        report.warning("Aggregate function calls cannot be nested.", term.pos)
                    if a.hasWindow then
                        report.warning("Aggregate function calls cannot contain window function calls.", term.pos)
                ExprInfo(
                    hasAgg = true,
                    isAgg = true,
                    hasWindow = false,
                    isValue = false,
                    isGroup = false,
                    ungroupedPaths = Nil
                )
            case _ if
                term.symbol.annotations.exists:
                    case Apply(Select(New(TypeIdent("sqlFunction")), _), _) => true
                    case _ => false
            =>
                val funcArgsRef = term match
                    case Apply(Apply(_, funcArgs), _) => funcArgs
                    case Apply(_, funcArgs) => funcArgs
                    case _ => Nil
                val funcArgs = funcArgsRef.flatMap:
                    case Typed(Repeated(a, _), _) => a
                    case a => a :: Nil
                val argsInfo =
                    funcArgs.map(a => treeInfoMacro(a, currentArgs, groups, inConnectBy))
                ExprInfo(
                    hasAgg = argsInfo.map(_.hasAgg).fold(false)(_ || _),
                    isAgg = false,
                    hasWindow = argsInfo.map(_.hasWindow).fold(false)(_ || _),
                    isValue = false,
                    isGroup = false,
                    ungroupedPaths = argsInfo.flatMap(_.ungroupedPaths)
                )
            case _ if
                term.symbol.annotations.exists:
                    case Apply(Select(New(TypeIdent("sqlWindow")), _), _) => true
                    case _ => false
            =>
                report.warning(
                    "Window function requires an OVER clause.", 
                    term.pos
                )
                ExprInfo(
                    hasAgg = false,
                    isAgg = false,
                    hasWindow = false,
                    isValue = false,
                    isGroup = false,
                    ungroupedPaths = Nil
                )
            case Apply(Select(f, "over"), o :: Nil) =>
                val isAggOrWindow = f.symbol.annotations.exists:
                    case Apply(Select(New(TypeIdent("sqlWindow" | "sqlAgg")), _), _) => true
                    case _ => false
                if !isAggOrWindow then
                    report.warning(
                        "OVER specified, but this expression is not a window function nor an aggregate function.",
                        term.pos
                    )

                val funcArgsRef = f match
                    case Apply(Apply(_, funcArgs), _) => funcArgs
                    case Apply(_, funcArgs) => funcArgs
                    case _ => Nil
                val funcArgs = funcArgsRef.flatMap:
                    case Typed(Repeated(a, _), _) => a
                    case a => a :: Nil
                val funcInfo =
                    funcArgs.map(a => treeInfoMacro(a, currentArgs, groups, inConnectBy))

                def windowParamsInfo(paramTerm: Term): List[ExprInfo] =
                    paramTerm match
                        case Typed(Repeated(params, _), _) =>
                            params.map(t => treeInfoMacro(t, currentArgs, groups, inConnectBy))
                        case _ => Nil

                def removeFrame(window: Term): Term =
                    window match
                        case Apply(Select(t, "rowsBetween" | "rangeBetween" | "groupsBetween"), _) =>
                            t
                        case _ => window

                val over = removeFrame(o)

                val windowInfo = over match
                    case Apply(Ident("partitionBy"), partition :: Nil) =>
                        windowParamsInfo(partition)
                    case Apply(Ident("sortBy" | "orderBy"), order :: Nil) =>
                        windowParamsInfo(order)
                    case Apply(
                        Select(Apply(Ident("partitionBy"), partition :: Nil), "sortBy" | "orderBy"),
                        order :: Nil
                    ) =>
                        windowParamsInfo(partition) ++ windowParamsInfo(order)
                    case _ => Nil

                val info = funcInfo ++ windowInfo

                for i <- info do
                    if i.hasAgg then
                        report.warning("Window function calls cannot contain aggregate function calls.", term.pos)
                    if i.hasWindow then
                        report.warning("Window function calls cannot be nested.", term.pos)
                    if i.ungroupedPaths.nonEmpty then
                        val c = i.ungroupedPaths.head.mkString(".")
                        report.warning(
                            s"Column \"$c\" must appear in the GROUP BY clause or be used in an aggregate function.",
                            term.pos
                        )

                ExprInfo(
                    hasAgg = false,
                    isAgg = false,
                    hasWindow = true,
                    isValue = false,
                    isGroup = false,
                    ungroupedPaths = info.flatMap(_.ungroupedPaths)
                )
            // TODO 其他表达式 还要多分析一个within 还有子查询、元组、grouping、in等，还有any all的几种情况
            case Select(expr, order)
                if List(
                    "asc", 
                    "desc", 
                    "ascNullsFirst", 
                    "ascNullsLast", 
                    "descNullsFirst", 
                    "descNullsLast"
                ).contains(order)
            =>
                treeInfoMacro(expr, currentArgs, groups, inConnectBy)
            case _ =>
                term.tpe.widen.asType match
                    case '[Query[_]] =>
                        val queryInfo = 
                            AnalysisClauseMacro.analysisQueryMacro(term, groups)
                        if !queryInfo.isOneRow then
                            report.warning("Subquery must be return one row.", term.pos)
                        ExprInfo(
                            hasAgg = false,
                            isAgg = false,
                            hasWindow = false,
                            isValue = false,
                            isGroup = false,
                            ungroupedPaths = Nil
                        )
                    case '[t] =>
                        if Expr.summon[AsSqlExpr[t]].isDefined then
                            ExprInfo(
                                hasAgg = false,
                                isAgg = false,
                                hasWindow = false,
                                isValue = true,
                                isGroup = false,
                                ungroupedPaths = Nil
                            )
                        else
                            report.error(s"$term")
                            ExprInfo(
                                hasAgg = false,
                                isAgg = false,
                                hasWindow = false,
                                isValue = false,
                                isGroup = false,
                                ungroupedPaths = Nil
                            )
        
        exprInfo.copy(
            ungroupedPaths = if isGroupExpr then Nil else exprInfo.ungroupedPaths,
            isGroup = isGroupExpr
        )

    def binaryInfoMacro(using q: Quotes)(
        left: q.reflect.Term,
        op: String,
        right: q.reflect.Term,
        currentArgs: List[String],
        groups: List[q.reflect.Term],
        inConnectBy: Boolean
    ): ExprInfo =
        if op == "/" || op == "%" then
            validateDiv(right)
        val leftInfo = treeInfoMacro(left, currentArgs, groups, inConnectBy)
        val rightInfo = treeInfoMacro(right, currentArgs, groups, inConnectBy)
        ExprInfo(
            hasAgg = leftInfo.hasAgg || rightInfo.hasAgg,
            isAgg = false,
            hasWindow = leftInfo.hasWindow || rightInfo.hasWindow,
            isValue = false,
            isGroup = false,
            ungroupedPaths = leftInfo.ungroupedPaths ++ rightInfo.ungroupedPaths
        )

    def fetchOrderExprMacro(using q: Quotes)(
        term: q.reflect.Term
    ): q.reflect.Term =
        import q.reflect.*
        
        term match
            case Select(expr, order)
                if List(
                    "asc", 
                    "desc", 
                    "ascNullsFirst", 
                    "ascNullsLast", 
                    "descNullsFirst", 
                    "descNullsLast"
                ).contains(order)
            =>
                expr
            case _ => term