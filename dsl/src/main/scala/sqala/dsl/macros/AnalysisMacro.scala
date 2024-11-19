package sqala.dsl.macros

import sqala.ast.statement.SqlQuery
import sqala.dsl.{AsSqlExpr, Table}
import sqala.dsl.statement.query.*

import scala.NamedTuple.NamedTuple
import scala.quoted.*

object AnalysisMacro:
    inline def analysisFilter[T](inline f: T): Unit =
        ${ analysisFilterMacro[T]('f) }

    inline def analysisHaving[T](inline f: T): Unit =
        ${ analysisHavingMacro[T]('f) }

    transparent inline def analysisSelect[F, N <: Tuple, V <: Tuple](
        inline f: F,
        mappedItems: NamedTuple[N, V],
        ast: SqlQuery.Select,
        qc: QueryContext
    ): ProjectionQuery[N, V, ?] =
        ${ analysisSelectMacro[F, N, V]('f, 'mappedItems, 'ast, 'qc) }

    inline def analysisGroupedSelect[T](inline f: T): Unit =
        ${ analysisGroupedSelectMacro[T]('f) }

    inline def analysisGroup[T](inline f: T): Unit =
        ${ analysisGroupMacro[T]('f) }

    inline def analysisOrder[T](inline f: T): Unit =
        ${ analysisOrderMacro[T]('f) }

    inline def analysisGroupedOrder[T](inline f: T): Unit =
        ${ analysisGroupedOrderMacro[T]('f) }

    inline def analysisDistinctOrder[T](inline f: T): Unit =
        ${ analysisDistinctOrderMacro[T]('f) }

    private def analysisFilterMacro[T: Type](f: Expr[T])(using q: Quotes): Expr[Unit] =
        import q.reflect.*

        val (args, body) = unwrapFuncMacro(f)

        val treeInfo = treeInfoMacro(args, body)

        if treeInfo.hasAgg then
            report.error("Aggregate functions are not allowed in WHERE/ON.", treeInfo.expr)

        if treeInfo.hasWindow then
            report.error("Window functions are not allowed in WHERE/ON.", treeInfo.expr)

        '{}

    private def analysisHavingMacro[T: Type](f: Expr[T])(using q: Quotes): Expr[Unit] =
        import q.reflect.*

        val (args, body) = unwrapFuncMacro(f)

        val treeInfo = treeInfoMacro(args, body)

        if treeInfo.hasWindow then
            report.error("Window functions are not allowed in HAVING.", treeInfo.expr)

        val ungrouped = treeInfo.ungroupedRef

        if ungrouped.nonEmpty then
            val c = ungrouped.head
                report.error(
                    s"Column \"${c._1}.${c._2}\" must appear in the GROUP BY clause or be used in an aggregate function.",
                    treeInfo.expr
                )

        '{}

    private def analysisSelectMacro[F, N <: Tuple : Type, V <: Tuple : Type](
        f: Expr[F],
        mappedItems: Expr[NamedTuple[N, V]],
        ast: Expr[SqlQuery.Select],
        qc: Expr[QueryContext]
    )(using q: Quotes): Expr[ProjectionQuery[N, V, ?]] =
        import q.reflect.*

        val (args, body) = unwrapFuncMacro(f)

        val terms = body.tpe.asType match
            case '[type t <: Tuple; t] =>
                body match
                    case Apply(_, applyTerms) => applyTerms
            case '[type t <: scala.NamedTuple.AnyNamedTuple; t] =>
                body match
                    case Apply(_, Apply(_, applyTerms) :: Nil) =>
                        applyTerms
                    case _ => Nil
            case _ => Nil
        val info = terms.map(t => treeInfoMacro(args, t))
        val hasAgg = info.map(_.hasAgg).fold(false)(_ || _)
            if hasAgg then
                for i <- info if i.nonAggRef.nonEmpty do
                    val c = i.nonAggRef.head
                    report.error(
                        s"Column \"${c._1}.${c._2}\" must appear in the GROUP BY clause or be used in an aggregate function.",
                        i.expr
                    )
                
        val isAgg = info.map(_.hasAgg).forall(i => i)
        if isAgg then
            '{
                new ProjectionQuery[N, V, OneRow]($mappedItems, $ast)(using $qc)
            }
        else 
            '{
                new ProjectionQuery[N, V, ManyRows]($mappedItems, $ast)(using $qc)
            }

    private def analysisGroupedSelectMacro[T: Type](f: Expr[T])(using q: Quotes): Expr[Unit] =
        import q.reflect.*

        val (args, body) = unwrapFuncMacro(f)

        val terms = body.tpe.asType match
            case '[type t <: Tuple; t] =>
                body match
                    case Inlined(Some(Apply(_, Apply(_, applyTerms) :: Nil)), _, _) =>
                        applyTerms
                    case Apply(_, applyTerms) => applyTerms
                    case _ => Nil
            case _ => Nil
        val info = terms.map(t => treeInfoMacro(args, t))
        for i <- info do
            if i.nonAggRef.nonEmpty && i.ungroupedRef.nonEmpty && i.nonAggRef.exists(c => i.ungroupedRef.contains(c)) then
                val c = i.ungroupedRef.head
                    report.error(
                        s"Column \"${c._1}.${c._2}\" must appear in the GROUP BY clause or be used in an aggregate function.",
                        i.expr
                    )

        '{}

    private def analysisGroupMacro[T: Type](f: Expr[T])(using q: Quotes): Expr[Unit] =
        import q.reflect.*

        val (args, body) = unwrapFuncMacro(f)

        body.tpe.asType match
            case '[type t <: Tuple; t] =>
                val terms = body match
                    case Inlined(Some(Apply(_, Apply(_, applyTerms) :: Nil)), _, _) =>
                        applyTerms
                    case _ => Nil
                val info = terms.map(t => treeInfoMacro(args, t))
                for i <- info do
                    if i.hasAgg then
                        report.error("Aggregate function are not allowed in GROUP BY.", i.expr)
                    if i.hasWindow then
                        report.error("Window function are not allowed in GROUP BY.", i.expr)
                    if i.isConst then
                        report.error("Constant are not allowed in GROUP BY.", i.expr)
            case _ => Nil

        '{}

    private def analysisOrderMacro[T: Type](f: Expr[T])(using q: Quotes): Expr[Unit] =
        import q.reflect.*

        val (args, body) = unwrapFuncMacro(f)

        val orderExpr =
            body match
                case Apply(TypeApply(Select(Ident("Expr"), _), _), e :: Nil) =>
                    Some(treeInfoMacro(args, e))
                case _ => None

        for e <- orderExpr do
            if e.hasAgg then
                report.error("Aggregate function are not allowed in ungrouped ORDER BY.", e.expr)

            if e.hasWindow then
                report.error("Window function are not allowed in ungrouped ORDER BY.", e.expr)

            if e.isConst then
                report.error("Constant are not allowed in ungrouped ORDER BY.", e.expr)
        
        '{}

    private def analysisGroupedOrderMacro[T: Type](f: Expr[T])(using q: Quotes): Expr[Unit] =
        import q.reflect.*

        val (args, body) = unwrapFuncMacro(f)

        val orderExpr =
            body match
                case Apply(TypeApply(Select(Ident("Expr"), _), _), e :: Nil) =>
                    Some(treeInfoMacro(args, e))
                case _ => None

        for e <- orderExpr do
            if e.isConst then
                report.error("Constant are not allowed in ungrouped ORDER BY.", e.expr)
        
        '{}

    private def analysisDistinctOrderMacro[T: Type](f: Expr[T])(using q: Quotes): Expr[Unit] =
        import q.reflect.*

        val (args, body) = unwrapFuncMacro(f)

        val orderTerm =
            body match
                case Apply(TypeApply(Select(Ident("Expr"), _), _), e :: Nil) =>
                    Some(e)
                case _ => None

        for t <- orderTerm do
            val validated = t match
                case TypeApply(
                    Select(
                        Apply(
                            Select(_, "selectDynamic"),
                            Literal(StringConstant(_)) :: Nil
                        ),
                        "$asInstanceOf$"
                    ),
                    _
                ) => true
                case _ => false

            if !validated then
                report.error("For SELECT DISTINCT, ORDER BY expressions must appear in select list.", t.asExpr)
        
        '{}

    case class ExprInfo(
        expr: Expr[Any],
        hasAgg: Boolean, 
        isAgg: Boolean,
        isGroup: Boolean,
        hasWindow: Boolean, 
        isConst: Boolean, 
        columnRef: List[(String, String)], 
        aggRef: List[(String, String)],
        nonAggRef: List[(String, String)],
        ungroupedRef: List[(String, String)]
    )

    private def treeInfoMacro[T](using q: Quotes)(
        args: List[(String, q.reflect.TypeRepr)],
        term: q.reflect.Term
    ): ExprInfo =
        import q.reflect.*

        def operators: List[String] =
            List(
                "==", "===", "!=", "<>", ">", ">=", "<", "<=", 
                "&&", "||", "+", "-", "*", "/", "%", "->", "->>",
                "like", "notLike", "startsWith", "endsWith", "contains"
            )

        term match
            case TypeApply(
                Select(
                    Apply(
                        Select(ident@Ident(objectName), "selectDynamic"),
                        Literal(StringConstant(valName)) :: Nil
                    ),
                    "$asInstanceOf$"
                ),
                _
            ) =>
                ident.tpe.asType match
                    case '[Table[_]] =>
                        ExprInfo(
                            term.asExpr, 
                            false, 
                            false, 
                            false,
                            false, 
                            false, 
                            (objectName, valName) :: Nil, 
                            Nil, 
                            (objectName, valName) :: Nil, 
                            Nil
                        )
                    case '[UngroupedTable[_]] =>
                        if !args.map(_._1).contains(objectName) then
                            report.error:
                                s"Subquery uses ungrouped column \"$objectName.$valName\" from outer query."
                        ExprInfo(
                            term.asExpr, 
                            false, 
                            false, 
                            false,
                            false, 
                            false, 
                            (objectName, valName) :: Nil, 
                            Nil,
                            (objectName, valName) :: Nil,
                            (objectName, valName) :: Nil
                        )
                    case '[Sort[_, _]] =>
                        ExprInfo(
                            term.asExpr, 
                            false, 
                            false, 
                            false,
                            false, 
                            false, 
                            (objectName, valName) :: Nil, 
                            Nil, 
                            (objectName, valName) :: Nil, 
                            Nil
                        )
                    case '[Group[_, _]] =>
                        ExprInfo(
                            term.asExpr, 
                            false, 
                            false, 
                            true,
                            false, 
                            false, 
                            (objectName, valName) :: Nil, 
                            Nil, 
                            (objectName, valName) :: Nil, 
                            Nil
                        )
                    case _ =>
                        ExprInfo(term.asExpr, false, false, false, false, false, Nil, Nil, Nil, Nil)
            case TypeApply(
                Select(
                    Inlined(
                        Some(
                            Apply(
                                Select(ident@Ident(objectName), "selectDynamic"),
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
                ident.tpe.asType match
                    case '[SubQuery[_, _]] =>
                        ExprInfo(
                            term.asExpr, 
                            false, 
                            false, 
                            false,
                            false, 
                            false, 
                            (objectName, valName) :: Nil, 
                            Nil,
                            (objectName, valName) :: Nil,
                            Nil
                        )
                    case '[UngroupedSubQuery[_, _]] =>
                        if !args.map(_._1).contains(objectName) then
                            report.error:
                                s"Subquery uses ungrouped column \"$objectName.$valName\" from outer query."
                        ExprInfo(
                            term.asExpr, 
                            false, 
                            false, 
                            false,
                            false, 
                            false, 
                            (objectName, valName) :: Nil, 
                            Nil,
                            (objectName, valName) :: Nil,
                            (objectName, valName) :: Nil
                        )
                    case _ =>
                        ExprInfo(term.asExpr, false, false, false, false, false, Nil, Nil, Nil, Nil)
            case Apply(Apply(TypeApply(Select(left, op), _), right :: Nil), _)
                if operators.contains(op)
            =>
                right.tpe.asType match
                    case '[Query[t, ManyRows]] =>
                        report.error("Subquery must return only one row.", right.asExpr)
                    case _ =>
                val leftInfo = treeInfoMacro(args, left)
                val rightInfo = treeInfoMacro(args, right)
                ExprInfo(
                    term.asExpr, 
                    leftInfo.hasAgg || rightInfo.hasAgg,
                    false,
                    false,
                    leftInfo.hasWindow || rightInfo.hasWindow,
                    false,
                    leftInfo.columnRef ++ rightInfo.columnRef,
                    leftInfo.aggRef ++ rightInfo.aggRef,
                    leftInfo.nonAggRef ++ rightInfo.nonAggRef,
                    leftInfo.ungroupedRef ++ rightInfo.ungroupedRef
                )
            case Apply(Apply(Select(left, op), right :: Nil), _)
                if operators.contains(op)
            =>
                right.tpe.asType match
                    case '[Query[t, ManyRows]] =>
                        report.error("Subquery must return only one row.", right.asExpr)
                    case _ =>
                val leftInfo = treeInfoMacro(args, left)
                val rightInfo = treeInfoMacro(args, right)
                ExprInfo(
                    term.asExpr, 
                    leftInfo.hasAgg || rightInfo.hasAgg,
                    false,
                    false,
                    leftInfo.hasWindow || rightInfo.hasWindow,
                    false,
                    leftInfo.columnRef ++ rightInfo.columnRef,
                    leftInfo.aggRef ++ rightInfo.aggRef,
                    leftInfo.nonAggRef ++ rightInfo.nonAggRef,
                    leftInfo.ungroupedRef ++ rightInfo.ungroupedRef
                )
            case Apply(Apply(TypeApply(Apply(Apply(TypeApply(Ident(op), _), left :: Nil), _), _), right :: Nil), _) 
                if operators.contains(op)
            =>
                right.tpe.asType match
                    case '[Query[t, ManyRows]] =>
                        report.error("Subquery must return only one row.", right.asExpr)
                    case _ =>
                val leftInfo = treeInfoMacro(args, left)
                val rightInfo = treeInfoMacro(args, right)
                ExprInfo(
                    term.asExpr, 
                    leftInfo.hasAgg || rightInfo.hasAgg,
                    false,
                    false,
                    leftInfo.hasWindow || rightInfo.hasWindow,
                    false,
                    leftInfo.columnRef ++ rightInfo.columnRef,
                    leftInfo.aggRef ++ rightInfo.aggRef,
                    leftInfo.nonAggRef ++ rightInfo.nonAggRef,
                    leftInfo.ungroupedRef ++ rightInfo.ungroupedRef
                )
            case Apply(Apply(TypeApply(Ident("asExpr"), _), _), _) =>
                ExprInfo(term.asExpr, false, false, false, false, true, Nil, Nil, Nil, Nil)
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
                    funcArgs.map(a => treeInfoMacro(args, a))
                for a <- argsInfo do
                    if a.hasAgg then
                        report.error("Aggregate function calls cannot be nested.", a.expr)
                    if a.hasWindow then
                        report.error("Aggregate function calls cannot contain window function calls.", a.expr)
                val columnsRef = 
                    argsInfo.flatMap(_.columnRef)
                if !columnsRef.map(_._1).forall(args.map(_._1).contains) then
                    report.error("Outer query columns are not allowed in aggregate functions.", term.asExpr)
                ExprInfo(term.asExpr, true, true, false, false, false, columnsRef, columnsRef, Nil, argsInfo.flatMap(_.ungroupedRef))
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
                    funcArgs.map(a => treeInfoMacro(args, a))
                ExprInfo(
                    term.asExpr, 
                    argsInfo.map(_.hasAgg).fold(false)(_ || _),
                    false,
                    false,
                    argsInfo.map(_.hasWindow).fold(false)(_ || _), 
                    false, 
                    argsInfo.flatMap(_.columnRef), 
                    argsInfo.flatMap(_.aggRef),
                    argsInfo.flatMap(_.nonAggRef),
                    argsInfo.flatMap(_.ungroupedRef)
                )
            case _ if
                term.symbol.annotations.find:
                    case Apply(Select(New(TypeIdent("sqlWindow")), _), _) => true
                    case _ => false
                .isDefined
            =>
                val funcArgs = term match
                    case Apply(Apply(_, funcArgs), _) => funcArgs
                    case Apply(_, funcArgs) => funcArgs
                    case _ => Nil
                val argsInfo = 
                    funcArgs.map(a => treeInfoMacro(args, a))
                ExprInfo(
                    term.asExpr, 
                    false,
                    false,
                    false,
                    false, 
                    false, 
                    argsInfo.flatMap(_.columnRef), 
                    Nil,
                    argsInfo.flatMap(_.nonAggRef),
                    argsInfo.flatMap(_.ungroupedRef)
                )
            case Apply(Apply(TypeApply(Select(Ident("Expr" | "WindowFunc"), "over"), _), windowFunc :: Nil), windowArg :: Nil) =>
                val funcInfo = treeInfoMacro(args, windowFunc)
                val isWindow = windowFunc.symbol.annotations.find:
                    case Apply(Select(New(TypeIdent("sqlWindow")), _), _) => true
                    case _ => false
                .isDefined
                if !funcInfo.isAgg && !isWindow then
                    report.error(
                        "OVER specified, but expression is not a window function nor an aggregate function.", 
                        funcInfo.expr
                    )

                def windowParamsInfo(paramTerm: Term): List[ExprInfo] =
                    paramTerm match
                        case Typed(Repeated(params, _), _) =>
                            params.map(t => treeInfoMacro(args, t))
                        case _ => Nil

                def removeFrame(window: Term): (term: Term, groupsBetween: Boolean) =
                    window match
                        case Apply(Select(t, "rowsBetween" | "rangeBetween"), _) =>
                            t -> false
                        case Apply(Select(t, "groupsBetween"), _) =>
                            t -> true
                        case _ => window -> false

                val window = removeFrame(windowArg)

                val windowInfo: List[ExprInfo] = window.term match
                    case Apply(Ident("partitionBy"), partition :: Nil) =>
                        if window.groupsBetween then
                            report.error("GROUPS mode requires an ORDER BY clause.", window.term.asExpr)
                        windowParamsInfo(partition)
                    case Apply(Ident("orderBy"), order :: Nil) =>
                        windowParamsInfo(order)
                    case Apply(
                        Select(Apply(Ident("partitionBy"), partition :: Nil), "orderBy"), 
                        order :: Nil
                    ) =>
                        windowParamsInfo(partition) ++ windowParamsInfo(order)
                    case _ => Nil
                
                for i <- windowInfo do
                    if i.hasAgg then
                        report.error("Window function calls cannot contain aggregate function calls.", i.expr)
                    if i.hasWindow then
                        report.error("Window function calls cannot be nested.", i.expr)
                    if i.ungroupedRef.nonEmpty then
                        val c = i.ungroupedRef.head
                        report.error(
                            s"Column \"${c._1}.${c._2}\" must appear in the GROUP BY clause or be used in an aggregate function.",
                            i.expr
                        )
                ExprInfo(
                    term.asExpr, 
                    false, 
                    false, 
                    false,
                    true, 
                    false,
                    funcInfo.columnRef ++ windowInfo.flatMap(_.columnRef), 
                    Nil,
                    funcInfo.columnRef ++ windowInfo.flatMap(_.nonAggRef), 
                    funcInfo.ungroupedRef ++ windowInfo.flatMap(_.ungroupedRef), 
                )
            case Apply(TypeApply(Apply(TypeApply(Ident("as"), _), expr :: Nil), _), _) =>
                treeInfoMacro(args, expr)
            case Apply(Apply(TypeApply(Ident("extract"), _), Apply(_, expr :: Nil) :: Nil), _) =>
                treeInfoMacro(args, expr)
            case Apply(Select(expr, op), _) if op.startsWith("unary_") =>
                treeInfoMacro(args, expr)
            case Apply(Apply(TypeApply(Select(expr, "in" | "notIn"), _), _), _) =>
                treeInfoMacro(args, expr)
            case Apply(TypeApply(Select(Ident("Expr"), order), _), expr :: Nil)
                if List("asc", "desc", "ascNullsFirst", "ascNullsLast", "descNullsFirst", "descNullsLast").contains(order)
            =>
                treeInfoMacro(args, expr)
            case Apply(Ident("if"), ifTerm :: Nil) =>
                treeInfoMacro(args, ifTerm)
            case Apply(Apply(TypeApply(Select(ifTerm, "then"), _), thenTerm :: Nil), _) =>
                val ifInfo = treeInfoMacro(args, ifTerm)
                val thenInfo = treeInfoMacro(args, thenTerm)
                ExprInfo(
                    term.asExpr,
                    ifInfo.hasAgg || thenInfo.hasAgg,
                    false,
                    false,
                    ifInfo.hasWindow || thenInfo.hasWindow,
                    false,
                    ifInfo.columnRef ++ thenInfo.columnRef,
                    ifInfo.aggRef ++ thenInfo.aggRef,
                    ifInfo.nonAggRef ++ thenInfo.nonAggRef,
                    ifInfo.ungroupedRef ++ thenInfo.ungroupedRef,
                )
            case Apply(Apply(TypeApply(Select(ifTerm, "else"), _), elseTerm :: Nil), _) =>
                val ifInfo = treeInfoMacro(args, ifTerm)
                val elseInfo = treeInfoMacro(args, elseTerm)
                ExprInfo(
                    term.asExpr,
                    ifInfo.hasAgg || elseInfo.hasAgg,
                    false,
                    false,
                    ifInfo.hasWindow || elseInfo.hasWindow,
                    false,
                    ifInfo.columnRef ++ elseInfo.columnRef,
                    ifInfo.aggRef ++ elseInfo.aggRef,
                    ifInfo.nonAggRef ++ elseInfo.nonAggRef,
                    ifInfo.ungroupedRef ++ elseInfo.ungroupedRef,
                )
            case Apply(Select(leftTerm, "else if"), rightTerm :: Nil) =>
                val leftInfo = treeInfoMacro(args, leftTerm)
                val rightInfo = treeInfoMacro(args, rightTerm)
                ExprInfo(
                    term.asExpr,
                    leftInfo.hasAgg || rightInfo.hasAgg,
                    false,
                    false,
                    leftInfo.hasWindow || rightInfo.hasWindow,
                    false,
                    leftInfo.columnRef ++ rightInfo.columnRef,
                    leftInfo.aggRef ++ rightInfo.aggRef,
                    leftInfo.nonAggRef ++ rightInfo.nonAggRef,
                    leftInfo.ungroupedRef ++ rightInfo.ungroupedRef,
                )
            case _ 
                if term.tpe.asType match
                    case '[Tuple] => true
                    case _ => false
            =>
                term match
                    case Apply(_, terms) =>
                        val exprInfo = terms.map(t => treeInfoMacro(args, t))
                        ExprInfo(
                            term.asExpr,
                            exprInfo.map(_.hasAgg).fold(false)(_ || _),
                            false,
                            false,
                            exprInfo.map(_.hasWindow).fold(false)(_ || _),
                            false,
                            exprInfo.flatMap(_.columnRef),
                            exprInfo.flatMap(_.aggRef),
                            exprInfo.flatMap(_.nonAggRef),
                            exprInfo.flatMap(_.ungroupedRef)
                        )
                    case _ =>
                        ExprInfo(
                            term.asExpr,
                            false,
                            false,
                            false,
                            false,
                            false,
                            Nil,
                            Nil,
                            Nil,
                            Nil
                        )
            case Apply(Apply(TypeApply(Select(expr, "between" | "notBetween"), _), between), _) =>
                val exprInfo = treeInfoMacro(args, expr)
                val betweenInfo = between.map(b => treeInfoMacro(args, b))
                val info = exprInfo :: betweenInfo
                ExprInfo(
                    term.asExpr,
                    info.map(_.hasAgg).fold(false)(_ || _),
                    false,
                    false,
                    info.map(_.hasWindow).fold(false)(_ || _), 
                    false, 
                    info.flatMap(_.columnRef), 
                    info.flatMap(_.aggRef),
                    info.flatMap(_.nonAggRef),
                    info.flatMap(_.ungroupedRef)
                )
            case Apply(Ident("grouping"), Typed(Repeated(items, _), _) :: Nil) =>
                val info = items.map(i => treeInfoMacro(args, i))
                for i <- info do
                    if !i.isGroup then
                        report.error(
                            "Arguments to GROUPING must be grouping expressions of the associated query level.",
                            i.expr
                        )
                ExprInfo(
                    term.asExpr,
                    true,
                    false,
                    false,
                    false, 
                    false, 
                    info.flatMap(_.columnRef), 
                    info.flatMap(_.columnRef),
                    Nil,
                    Nil
                )
            case _ =>
                term.tpe.widen.asType match
                    case '[t] =>
                        if Expr.summon[AsSqlExpr[t]].isDefined then
                            ExprInfo(term.asExpr, false, false, false, false, true, Nil, Nil, Nil, Nil)
                        else
                            ExprInfo(term.asExpr, false, false, false, false, false, Nil, Nil, Nil, Nil)

    private def unwrapFuncMacro[T](
        value: Expr[T]
    )(using q: Quotes): (args: List[(String, q.reflect.TypeRepr)], body: q.reflect.Term) =
        import q.reflect.*

        def unwrapInlined(term: Term): Term =
            term match
                case Inlined(_, _, inlinedTerm) =>
                    unwrapInlined(inlinedTerm)
                case _ => term

        val term = value.asTerm

        val func = unwrapInlined(term) match
            case Block(_, Inlined(_, _, Block(statement :: Nil, _))) =>
                statement
            case Block(_, blockTerm) =>
                unwrapInlined(blockTerm) match
                    case Block(statement :: Nil, _) =>
                        statement
            case _ => report.errorAndAbort("Unsupported usage.")

        val args = func match
            case DefDef(_, params :: Nil, _, _) =>
                params.params.asInstanceOf[List[ValDef]].map:
                    case ValDef(argName, argType, argTerm) =>
                        argName -> argType.tpe
            case _ => report.errorAndAbort("Unsupported usage.")

        val body = func match
            case DefDef(_, _, _, Some(funBody)) =>
                funBody
            case _ => report.errorAndAbort("Unsupported usage.")

        (args, body)