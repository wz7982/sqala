package sqala.macros

import sqala.common.AsSqlExpr
import sqala.static.dsl.Table
import sqala.static.statement.query.*

import scala.quoted.*
import scala.language.experimental.namedTuples


private[sqala] object AnalysisExprMacro:
    case class ExprInfo(
        hasAgg: Boolean,
        isAgg: Boolean,
        hasWindow: Boolean,
        isValue: Boolean,
        isGroup: Boolean,
        ungroupedPaths: List[List[String]],
        notInAggPaths: List[List[String]]
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

    def analysisTreeInfo(using q: Quotes)(
        term: q.reflect.Term,
        currentArgs: List[String],
        groups: List[q.reflect.Term],
        inConnectBy: Boolean
    ): ExprInfo =
        import q.reflect.*

        val info = treeInfo(term, currentArgs, groups, inConnectBy)
        val ungrouped =
            info.ungroupedPaths
        if ungrouped.nonEmpty then
            val c = ungrouped.head.mkString(".")
            report.warning(
                s"Column \"$c\" must appear in the GROUP BY clause or be used in an aggregate function.",
                term.pos
            )
        info

    def treeInfo(using q: Quotes)(
        term: q.reflect.Term,
        currentArgs: List[String],
        groups: List[q.reflect.Term],
        inConnectBy: Boolean
    ): ExprInfo =
        import q.reflect.*

        val isGroupExpr = groups.map(_.show).contains(term.show)

        def analysisField(term: Term): (List[String], Boolean) =
            term match
                case TypeApply(
                    Select(
                        Inlined(
                            Some(
                                Apply(
                                    Select(o, "selectDynamic"), 
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
                    val info = analysisField(o)
                    (info._1 :+ valName, info._2)
                case TypeApply(
                    Select(
                        Block(
                            _, 
                            Inlined(
                                Some(
                                    Apply(
                                        Select(o, "selectDynamic"), 
                                        Literal(StringConstant(valName)) :: Nil
                                    )
                                ),
                                _,
                                _
                            )
                        ), 
                        "$asInstanceOf$"
                    ), 
                    _
                ) =>
                    val info = analysisField(o)
                    (info._1 :+ valName, info._2)
                case o@Ident(objectName) =>
                    o.tpe.asType match
                        case '[Table[_]] | '[SubQuery[_, _]] =>
                            (objectName :: Nil, false)
                        case _ =>
                            (objectName :: Nil, true)

        val exprInfo = term match
            case f@TypeApply(_, _) =>
                val fieldInfo = analysisField(f)
                if fieldInfo._2 then
                    ExprInfo(
                        hasAgg = false,
                        isAgg = false,
                        hasWindow = false,
                        isValue = false,
                        isGroup = false,
                        ungroupedPaths = fieldInfo._1 :: Nil,
                        notInAggPaths = Nil
                    )
                else
                    ExprInfo(
                        hasAgg = false,
                        isAgg = false,
                        hasWindow = false,
                        isValue = false,
                        isGroup = false,
                        ungroupedPaths = Nil,
                        notInAggPaths = fieldInfo._1 :: Nil
                    )
            case Apply(Apply(TypeApply(Select(left, op), _), right :: Nil), _)
                if binaryOperators.contains(op)
            =>
                binaryInfo(left, op, right, currentArgs, groups, inConnectBy)
            case Apply(Apply(Select(left, op), right :: Nil), _)
                if binaryOperators.contains(op)
            =>
                binaryInfo(left, op, right, currentArgs, groups, inConnectBy)
            case Apply(Apply(TypeApply(Apply(Apply(TypeApply(Ident(op), _), left :: Nil), _), _), right :: Nil), _)
                if binaryOperators.contains(op)
            =>
                binaryInfo(left, op, right, currentArgs, groups, inConnectBy)
            case Apply(Select(expr, op), _) if op.startsWith("unary_") =>
                treeInfo(expr, currentArgs, groups, inConnectBy)
            case Apply(TypeApply(Ident("prior"), _), expr :: Nil) =>
                if !inConnectBy then
                    report.warning("Prior can only be used in CONNECT BY.", term.pos)
                treeInfo(expr, currentArgs, groups, inConnectBy)
            case Apply(Apply(TypeApply(Ident("asExpr"), _), t :: Nil), _) =>
                treeInfo(t, currentArgs, groups, inConnectBy)
            case Apply(TypeApply(Select(_, "asExpr"), _), t :: Nil) =>
                treeInfo(t, currentArgs, groups, inConnectBy)
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
                    funcArgs.map(a => treeInfo(a, currentArgs, groups, inConnectBy))
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
                    ungroupedPaths = Nil,
                    notInAggPaths = Nil
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
                    funcArgs.map(a => treeInfo(a, currentArgs, groups, inConnectBy))
                ExprInfo(
                    hasAgg = argsInfo.map(_.hasAgg).fold(false)(_ || _),
                    isAgg = false,
                    hasWindow = argsInfo.map(_.hasWindow).fold(false)(_ || _),
                    isValue = false,
                    isGroup = false,
                    ungroupedPaths = argsInfo.flatMap(_.ungroupedPaths),
                    notInAggPaths = argsInfo.flatMap(_.notInAggPaths)
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
                    ungroupedPaths = Nil,
                    notInAggPaths = Nil
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
                    funcArgs.map(a => treeInfo(a, currentArgs, groups, inConnectBy))

                def windowParamsInfo(paramTerm: Term): List[ExprInfo] =
                    paramTerm match
                        case Typed(Repeated(params, _), _) =>
                            params.map(t => treeInfo(t, currentArgs, groups, inConnectBy))
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
                    ungroupedPaths = info.flatMap(_.ungroupedPaths),
                    notInAggPaths = info.flatMap(_.notInAggPaths)
                )
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
                treeInfo(expr, currentArgs, groups, inConnectBy)
            case Apply(TypeApply(Select(Ident(n), "apply"), _), terms) 
                if n.startsWith("Tuple")
            =>
                val exprInfoList = terms.map(t => treeInfo(t, currentArgs, groups, inConnectBy))
                ExprInfo(
                    hasAgg = exprInfoList.map(_.hasAgg).fold(false)(_ || _),
                    isAgg = false,
                    hasWindow = exprInfoList.map(_.hasWindow).fold(false)(_ || _),
                    isValue = false,
                    isGroup = false,
                    ungroupedPaths = exprInfoList.flatMap(_.ungroupedPaths),
                    notInAggPaths = exprInfoList.flatMap(_.notInAggPaths)
                )
            case Apply(Apply(TypeApply(Ident("extract"), _), Apply(_, expr :: Nil) :: Nil), _) =>
                treeInfo(expr, currentArgs, groups, inConnectBy)
            case Apply(TypeApply(Ident("extract"), _), Apply(_, expr :: Nil) :: Nil) =>
                treeInfo(expr, currentArgs, groups, inConnectBy)
            case Apply(Ident("grouping"), Typed(Repeated(terms, _), _) :: Nil) =>
                val exprInfoList = terms.map(t => treeInfo(t, currentArgs, groups, inConnectBy))
                val ungrouped = exprInfoList.flatMap(_.ungroupedPaths)
                if ungrouped.nonEmpty || groups.isEmpty then
                    report.warning(
                        "Arguments to GROUPING must be grouping expressions of the associated query level.",
                        term.pos
                    )
                ExprInfo(
                    hasAgg = true,
                    isAgg = true,
                    hasWindow = false,
                    isValue = false,
                    isGroup = false,
                    ungroupedPaths = Nil,
                    notInAggPaths = Nil
                )
            case Apply(Apply(TypeApply(Select(expr, "between"), _), between), _) =>
                val exprInfoList = (expr :: between).map(t => treeInfo(t, currentArgs, groups, inConnectBy))
                ExprInfo(
                    hasAgg = exprInfoList.map(_.hasAgg).fold(false)(_ || _),
                    isAgg = false,
                    hasWindow = exprInfoList.map(_.hasWindow).fold(false)(_ || _),
                    isValue = false,
                    isGroup = false,
                    ungroupedPaths = exprInfoList.flatMap(_.ungroupedPaths),
                    notInAggPaths = exprInfoList.flatMap(_.notInAggPaths)
                )
            case Apply(Apply(TypeApply(Select(expr, "in"), _), in), _) =>
                val exprInfoList = (expr :: in).map(t => treeInfo(t, currentArgs, groups, inConnectBy))
                ExprInfo(
                    hasAgg = exprInfoList.map(_.hasAgg).fold(false)(_ || _),
                    isAgg = false,
                    hasWindow = exprInfoList.map(_.hasWindow).fold(false)(_ || _),
                    isValue = false,
                    isGroup = false,
                    ungroupedPaths = exprInfoList.flatMap(_.ungroupedPaths),
                    notInAggPaths = exprInfoList.flatMap(_.notInAggPaths)
                )
            case Apply(Ident("if"), ifTerm :: Nil) =>
                treeInfo(ifTerm, currentArgs, groups, inConnectBy)
            case Apply(Apply(TypeApply(Select(ifTerm, "then"), _), thenTerm :: Nil), _) =>
                val ifInfo = treeInfo(ifTerm, currentArgs, groups, inConnectBy)
                val thenInfo = treeInfo(thenTerm, currentArgs, groups, inConnectBy)
                val exprInfoList = List(ifInfo, thenInfo)
                ExprInfo(
                    hasAgg = exprInfoList.map(_.hasAgg).fold(false)(_ || _),
                    isAgg = false,
                    hasWindow = exprInfoList.map(_.hasWindow).fold(false)(_ || _),
                    isValue = false,
                    isGroup = false,
                    ungroupedPaths = exprInfoList.flatMap(_.ungroupedPaths),
                    notInAggPaths = exprInfoList.flatMap(_.notInAggPaths)
                )
            case Apply(Apply(TypeApply(Select(ifTerm, "else"), _), elseTerm :: Nil), _) =>
                val ifInfo = treeInfo(ifTerm, currentArgs, groups, inConnectBy)
                val elseInfo = treeInfo(elseTerm, currentArgs, groups, inConnectBy)
                val exprInfoList = List(ifInfo, elseInfo)
                ExprInfo(
                    hasAgg = exprInfoList.map(_.hasAgg).fold(false)(_ || _),
                    isAgg = false,
                    hasWindow = exprInfoList.map(_.hasWindow).fold(false)(_ || _),
                    isValue = false,
                    isGroup = false,
                    ungroupedPaths = exprInfoList.flatMap(_.ungroupedPaths),
                    notInAggPaths = exprInfoList.flatMap(_.notInAggPaths)
                )
            case Apply(Select(leftTerm, "else if"), rightTerm :: Nil) =>
                val leftInfo = treeInfo(leftTerm, currentArgs, groups, inConnectBy)
                val rightInfo = treeInfo(rightTerm, currentArgs, groups, inConnectBy)
                val exprInfoList = List(leftInfo, rightInfo)
                ExprInfo(
                    hasAgg = exprInfoList.map(_.hasAgg).fold(false)(_ || _),
                    isAgg = false,
                    hasWindow = exprInfoList.map(_.hasWindow).fold(false)(_ || _),
                    isValue = false,
                    isGroup = false,
                    ungroupedPaths = exprInfoList.flatMap(_.ungroupedPaths),
                    notInAggPaths = exprInfoList.flatMap(_.notInAggPaths)
                )
            case Apply(TypeApply(Ident("exists"), _) , query :: Nil) =>
                AnalysisClauseMacro.analysisQuery(term, groups)
                ExprInfo(
                    hasAgg = false,
                    isAgg = false,
                    hasWindow = false,
                    isValue = false,
                    isGroup = false,
                    ungroupedPaths = Nil,
                    notInAggPaths = Nil
                )
            case Apply(Apply(TypeApply(Ident("any" | "all"), _) , term :: Nil), _) =>
                term.tpe.asType match
                    case '[Query[_]] => AnalysisClauseMacro.analysisQuery(term, groups)
                    case _ =>
                ExprInfo(
                    hasAgg = false,
                    isAgg = false,
                    hasWindow = false,
                    isValue = false,
                    isGroup = false,
                    ungroupedPaths = Nil,
                    notInAggPaths = Nil
                )
            case Apply(TypeApply(Ident("any" | "all"), _) , expr :: Nil) =>
                treeInfo(expr, currentArgs, groups, inConnectBy)
            case _ =>
                term.tpe.widen.asType match
                    case '[Query[_]] =>
                        val queryInfo = 
                            AnalysisClauseMacro.analysisQuery(term, groups)
                        if !queryInfo.isOneRow then
                            report.warning("Subquery must be return one row.", term.pos)
                        ExprInfo(
                            hasAgg = false,
                            isAgg = false,
                            hasWindow = false,
                            isValue = false,
                            isGroup = false,
                            ungroupedPaths = Nil,
                            notInAggPaths = Nil
                        )
                    case '[Table[_]] | '[SubQuery[_, _]] =>
                        term match
                            case Ident(n) =>
                                ExprInfo(
                                    hasAgg = false,
                                    isAgg = false,
                                    hasWindow = false,
                                    isValue = false,
                                    isGroup = false,
                                    ungroupedPaths = Nil,
                                    notInAggPaths = (n :: "*" :: Nil) :: Nil
                                )
                    case '[t] =>
                        if Expr.summon[AsSqlExpr[t]].isDefined then
                            ExprInfo(
                                hasAgg = false,
                                isAgg = false,
                                hasWindow = false,
                                isValue = true,
                                isGroup = false,
                                ungroupedPaths = Nil,
                                notInAggPaths = Nil
                            )
                        else
                            ExprInfo(
                                hasAgg = false,
                                isAgg = false,
                                hasWindow = false,
                                isValue = false,
                                isGroup = false,
                                ungroupedPaths = Nil,
                                notInAggPaths = Nil
                            )
        
        exprInfo.copy(
            ungroupedPaths = if isGroupExpr then Nil else exprInfo.ungroupedPaths,
            isGroup = isGroupExpr
        )

    def binaryInfo(using q: Quotes)(
        left: q.reflect.Term,
        op: String,
        right: q.reflect.Term,
        currentArgs: List[String],
        groups: List[q.reflect.Term],
        inConnectBy: Boolean
    ): ExprInfo =
        if op == "/" || op == "%" then
            validateDiv(right)
        val leftInfo = treeInfo(left, currentArgs, groups, inConnectBy)
        val rightInfo = treeInfo(right, currentArgs, groups, inConnectBy)
        ExprInfo(
            hasAgg = leftInfo.hasAgg || rightInfo.hasAgg,
            isAgg = false,
            hasWindow = leftInfo.hasWindow || rightInfo.hasWindow,
            isValue = false,
            isGroup = false,
            ungroupedPaths = leftInfo.ungroupedPaths ++ rightInfo.ungroupedPaths,
            notInAggPaths = leftInfo.notInAggPaths ++ rightInfo.notInAggPaths
        )

    def fetchOrderExpr(using q: Quotes)(
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