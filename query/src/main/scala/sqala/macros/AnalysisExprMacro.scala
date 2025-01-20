package sqala.macros

import sqala.common.AsSqlExpr
import sqala.static.statement.query.Query

import scala.quoted.*

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
        groups: List[String],
        inConnectBy: Boolean
    ): ExprInfo =
        import q.reflect.*

        val isGroupExpr = groups.contains(term.show)

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
            // TODO asExpr 尤其是子查询
            case _ if
                term.symbol.annotations.find:
                    case Apply(Select(New(TypeIdent("sqlAgg")), _), _) => true
                    case _ => false
                .isDefined
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
                term.symbol.annotations.find:
                    case Apply(Select(New(TypeIdent("sqlFunction")), _), _) => true
                    case _ => false
                .isDefined
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
            // TODO 窗口等其他表达式
            case _ =>
                term.tpe.widen.asType match
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
        groups: List[String],
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