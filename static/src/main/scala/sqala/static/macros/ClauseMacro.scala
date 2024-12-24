package sqala.static.macros

import sqala.ast.expr.SqlExpr
import sqala.ast.order.*
import sqala.ast.statement.*
import sqala.ast.table.*
import sqala.static.common.*
import sqala.static.statement.query.*

import scala.quoted.*

object ClauseMacro:
    inline def fetchArgNames[T](inline f: T): List[String] =
        ${ fetchArgNamesMacro[T]('f) }

    inline def fetchFilter[T](
        inline f: T,
        inline inGroupBy: Boolean,
        inline inConnectBy: Boolean,
        tableNames: List[String],
        queryContext: QueryContext
    ): SqlExpr =
        ${ fetchFilterMacro[T]('f, 'inGroupBy, 'inConnectBy, 'tableNames, 'queryContext) }

    inline def fetchSortBy[T](
        inline f: T,
        inline inDistinctOn: Boolean,
        tableNames: List[String],
        queryContext: QueryContext
    ): List[SqlOrderBy] =
        ${ fetchSortByMacro[T]('f, 'inDistinctOn, 'tableNames, 'queryContext) }

    transparent inline def fetchMap[F, T](
        inline f: F,
        tableNames: List[String],
        ast: SqlQuery.Select,
        queryContext: QueryContext
    ): Query[T, ?] =
        ${ fetchMapMacro[F, T]('f, 'tableNames, 'ast, 'queryContext) }

    inline def fetchGroupBy[T](
        inline f: T,
        tableNames: List[String],
        queryContext: QueryContext
    ): List[SqlExpr] =
        ${ fetchGroupByMacro[T]('f, 'tableNames, 'queryContext) }

    inline def fetchGroupedMap[T](
        inline f: T,
        inline inDistinctOn: Boolean,
        tableNames: List[String],
        queryContext: QueryContext
    ): List[SqlSelectItem] =
        ${ fetchGroupedMapMacro[T]('f, 'inDistinctOn, 'tableNames, 'queryContext) }

    inline def fetchFunctionTable[T](
        inline f: T,
        queryContext: QueryContext
    ): SqlTable =
        ${ fetchFunctionTableMacro[T]('f, 'queryContext) }

    inline def fetchPivot[T](
        inline f: T,
        tableNames: List[String],
        queryContext: QueryContext
    ): List[SqlExpr.Func] =
        ${ fetchPivotMacro[T]('f, 'tableNames, 'queryContext) }

    inline def fetchPivotFor[T](
        inline f: T,
        tableNames: List[String],
        queryContext: QueryContext
    ): List[(SqlExpr, List[SqlExpr])] =
        ${ fetchPivotForMacro[T]('f, 'tableNames, 'queryContext) }

    inline def fetchSet[T](
        inline f: T,
        tableNames: List[String],
        queryContext: QueryContext
    ): (SqlExpr, SqlExpr) =
        ${ fetchSetMacro[T]('f, 'tableNames, 'queryContext) }

    inline def fetchInsert[T](
        inline f: T,
        tableNames: List[String]
    ): List[SqlExpr] =
        ${ fetchInsertMacro[T]('f, 'tableNames) }

    inline def bindGeneratedPrimaryKey[A](id: Long, entity: A): A =
        ${ bindGeneratedPrimaryKeyMacro[A]('id, 'entity) }

    def fetchArgNamesMacro[T](f: Expr[T])(using q: Quotes): Expr[List[String]] =
        val (args, _) = unwrapFuncMacro(f)

        Expr(args)

    def fetchFilterMacro[T](
        f: Expr[T],
        inGroupBy: Expr[Boolean],
        inConnectBy: Expr[Boolean],
        tableNames: Expr[List[String]],
        queryContext: Expr[QueryContext]
    )(using q: Quotes): Expr[SqlExpr] =
        import q.reflect.*

        val (args, body) = unwrapFuncMacro(f)

        val inGroupValue = inGroupBy.value.get

        val (expr, info) = ExprMacro.treeInfoMacro(args, tableNames, body, queryContext, inConnectBy.value.get, false, false)

        val ungrouped = info.ungroupedRef

        if ungrouped.nonEmpty then
            val c = ungrouped.head
                report.error(
                    s"Column \"${c._1}.${c._2}\" must appear in the GROUP BY clause or be used in an aggregate function.",
                    body.asExpr
                )

        if inGroupValue then
            if info.hasWindow then
                report.error("Window functions are not allowed in HAVING.", body.asExpr)
        else
            if info.hasAgg then
                report.error("Aggregate functions are not allowed in WHERE/ON.", body.asExpr)

            if info.hasWindow then
                report.error("Window functions are not allowed in WHERE/ON.", body.asExpr)

        expr

    def fetchSortByMacro[T](
        f: Expr[T],
        inDistinctOn: Expr[Boolean],
        tableNames: Expr[List[String]],
        queryContext: Expr[QueryContext]
    )(using q: Quotes): Expr[List[SqlOrderBy]] =
        import q.reflect.*

        val (args, body) = unwrapFuncMacro(f)

        val sort = body match
            case Apply(TypeApply(Select(Ident(t), "apply"), _), terms)
                if t.startsWith("Tuple")
            =>
                terms.map(t => ExprMacro.sortInfoMacro(args, tableNames, t, queryContext, false, false, inDistinctOn.value.get))
            case _ =>
                ExprMacro.sortInfoMacro(args, tableNames, body, queryContext, false, false, inDistinctOn.value.get) :: Nil

        val info = sort.map(_._2)

        val ungrouped = info.flatMap(_.ungroupedRef)

        if ungrouped.nonEmpty then
            val c = ungrouped.head
                report.error(
                    s"Column \"${c._1}.${c._2}\" must appear in the GROUP BY clause or be used in an aggregate function.",
                    body.asExpr
                )

        for e <- info do
            if e.isConst then
                report.error("Value expression are not allowed in ORDER BY.", body.asExpr)

        Expr.ofList(sort.map(_._1))

    def fetchGroupByMacro[T](
        f: Expr[T],
        tableNames: Expr[List[String]],
        queryContext: Expr[QueryContext]
    )(using q: Quotes): Expr[List[SqlExpr]] =
        import q.reflect.*

        val (args, body) = unwrapFuncMacro(f)

        val group = body match
            case Inlined(Some(Apply(_, Apply(TypeApply(Select(Ident(t), "apply"), _), terms) :: Nil)), _, _)
                if t.startsWith("Tuple")
            =>
                terms.map(t => ExprMacro.treeInfoMacro(args, tableNames, t, queryContext, false, false, false))
            case Apply(TypeApply(Select(Ident(t), "apply"), _), terms) =>
                terms.map(t => ExprMacro.treeInfoMacro(args, tableNames, t, queryContext, false, false, false))
            case _ =>
                report.errorAndAbort(
                    s"\"${body.show}\" cannot be converted to SQL expression.",
                    body.asExpr
                )

        Expr.ofList(group.map(_._1))

    def fetchMapMacro[F, T: Type](
        f: Expr[F],
        tableNames: Expr[List[String]],
        ast: Expr[SqlQuery.Select],
        queryContext: Expr[QueryContext]
    )(using q: Quotes): Expr[Query[T, ?]] =
        import q.reflect.*

        val (args, body) = unwrapFuncMacro(f)

        val (items, info) = body match
            case Inlined(Some(Apply(n, Apply(TypeApply(Select(Ident(t), "apply"), _), terms) :: Nil)), _, _)
                if t.startsWith("Tuple")
            =>
                val names = n match
                    case TypeApply(Apply(TypeApply(_, Applied(_, ns) :: Nil), _), _) =>
                        ns.map:
                            case Singleton(Literal(StringConstant(s))) => Expr(s)
                val items = terms
                    .map(t => ExprMacro.treeInfoMacro(args, tableNames, t, queryContext, false, false, false))
                val exprs = items.map(_._1).zip(names).map: (i, n) =>
                    '{ SqlSelectItem.Item($i, Some($n)) }
                exprs -> items.map(_._2)
            case Apply(n, Apply(TypeApply(Select(Ident(t), "apply"), _), terms) :: Nil)
                if t.startsWith("Tuple")
            =>
                val names = n match
                    case TypeApply(Apply(TypeApply(_, Applied(_, ns) :: Nil), _), _) =>
                        ns.map:
                            case Singleton(Literal(StringConstant(s))) => Expr(s)
                val items = terms
                    .map(t => ExprMacro.treeInfoMacro(args, tableNames, t, queryContext, false, false, false))
                val exprs = items.map(_._1).zip(names).map: (i, n) =>
                    '{ SqlSelectItem.Item($i, Some($n)) }
                exprs -> items.map(_._2)
            case Apply(TypeApply(Select(Ident(t), "apply"), _), terms)
                if t.startsWith("Tuple")
            =>
                val items = terms
                    .map(t => ExprMacro.treeInfoMacro(args, tableNames, t, queryContext, false, false, false))
                val exprs = items.map(_._1).map: i =>
                    '{ SqlSelectItem.Item($i, None) }
                exprs -> items.map(_._2)
            case _ =>
                val (expr, info) = ExprMacro.treeInfoMacro(args, tableNames, body, queryContext, false, false, false)
                ('{ SqlSelectItem.Item($expr, None) } :: Nil) -> (info :: Nil)

        val itemsExpr = Expr.ofList(items)

        val hasAgg = info.map(_.hasAgg).fold(false)(_ || _)
        if hasAgg then
            for i <- info if i.nonAggRef.nonEmpty do
                val c = i.nonAggRef.head
                report.error(
                    s"Column \"${c._1}.${c._2}\" must appear in the GROUP BY clause or be used in an aggregate function.",
                    body.asExpr
                )

        if hasAgg then
            '{
                ProjectionQuery[T, OneRow]($ast.copy(select = $itemsExpr))(using $queryContext)
            }
        else
            '{
                ProjectionQuery[T, ManyRows]($ast.copy(select = $itemsExpr))(using $queryContext)
            }

    def fetchGroupedMapMacro[T](
        f: Expr[T],
        inDistinctOn: Expr[Boolean],
        tableNames: Expr[List[String]],
        queryContext: Expr[QueryContext]
    )(using q: Quotes): Expr[List[SqlSelectItem]] =
        import q.reflect.*

        val (args, body) = unwrapFuncMacro(f)

        val (items, info) = body match
            case Inlined(Some(Apply(n, Apply(TypeApply(Select(Ident(t), "apply"), _), terms) :: Nil)), _, _)
                if t.startsWith("Tuple")
            =>
                val names = n match
                    case TypeApply(Apply(TypeApply(_, Applied(_, ns) :: Nil), _), _) =>
                        ns.map:
                            case Singleton(Literal(StringConstant(s))) => Expr(s)
                val items = terms
                    .map(t => ExprMacro.treeInfoMacro(args, tableNames, t, queryContext, false, false, inDistinctOn.value.get))
                val exprs = items.map(_._1).zip(names).map: (i, n) =>
                    '{ SqlSelectItem.Item($i, Some($n)) }
                exprs -> items.map(_._2)
            case Apply(n, Apply(TypeApply(Select(Ident(t), "apply"), _), terms) :: Nil) =>
                val names = n match
                    case TypeApply(Apply(TypeApply(_, Applied(_, ns) :: Nil), _), _) =>
                        ns.map:
                            case Singleton(Literal(StringConstant(s))) => Expr(s)
                val items = terms
                    .map(t => ExprMacro.treeInfoMacro(args, tableNames, t, queryContext, false, false, inDistinctOn.value.get))
                val exprs = items.map(_._1).zip(names).map: (i, n) =>
                    '{ SqlSelectItem.Item($i, Some($n)) }
                exprs -> items.map(_._2)
            case Apply(TypeApply(Select(Ident(t), "apply"), _), terms)
                if t.startsWith("Tuple")
            =>
                val items = terms
                    .map(t => ExprMacro.treeInfoMacro(args, tableNames, t, queryContext, false, false, inDistinctOn.value.get))
                val exprs = items.map(_._1).map: i =>
                    '{ SqlSelectItem.Item($i, None) }
                exprs -> items.map(_._2)
            case _ =>
                val (expr, info) = ExprMacro.treeInfoMacro(args, tableNames, body, queryContext, false, false, inDistinctOn.value.get)
                ('{ SqlSelectItem.Item($expr, None) } :: Nil) -> (info :: Nil)

        for i <- info do
            if i.nonAggRef.nonEmpty && i.ungroupedRef.nonEmpty && i.nonAggRef.exists(c => i.ungroupedRef.contains(c)) then
                val c = i.ungroupedRef.head
                    report.error(
                        s"Column \"${c._1}.${c._2}\" must appear in the GROUP BY clause or be used in an aggregate function.",
                        body.asExpr
                    )

        Expr.ofList(items)

    def fetchFunctionTableMacro[T: Type](
        f: Expr[T],
        queryContext: Expr[QueryContext]
    )(using q: Quotes): Expr[SqlTable] =
        import q.reflect.*

        def unwrapInlined(term: Term): Term =
            term match
                case Inlined(_, _, inlinedTerm) =>
                    unwrapInlined(inlinedTerm)
                case _ => term

        val term = unwrapInlined(f.asTerm)

        term match
            case _ if
                term.symbol.annotations.find:
                    case Apply(Select(New(TypeIdent("sqlFunctionTable")), _), _) => true
                    case _ => false
                .isDefined
            =>
                val functionName = TableMacro.tableNameMacro[T]
                val metaData = TableMacro.tableMetaDataMacro[T]
                val (functionExpr, _) = ExprMacro.createFunction(Nil, '{ Nil }, functionName.value.get, term, false, queryContext, false, false, false)
                '{
                    val function = $functionExpr.asInstanceOf[SqlExpr.Func]
                    val columns = $metaData.columnNames
                    SqlTable.FuncTable($functionName, function.args, Some(SqlTableAlias($functionName, columns)))
                }
            case _ =>
                report.errorAndAbort(
                    s"The function table must have the @sqlFunctionTable annotation.",
                    f
                )

    def fetchPivotMacro[T](
        f: Expr[T],
        tableNames: Expr[List[String]],
        queryContext: Expr[QueryContext]
    )(using q: Quotes): Expr[List[SqlExpr.Func]] =
        import q.reflect.*

        val (args, body) = unwrapFuncMacro(f)

        body match
            case Inlined(Some(Apply(n, Apply(TypeApply(Select(Ident(t), "apply"), _), terms) :: Nil)), _, _)
                if t.startsWith("Tuple")
            =>
                val functions = terms.map: t =>
                    val (expr, info) = ExprMacro.treeInfoMacro(args, tableNames, t, queryContext, false, false, false)
                    if !info.isAgg then
                        report.error("The expression in PVIOT must be an aggregate function.", t.asExpr)
                    '{ $expr.asInstanceOf[SqlExpr.Func] }

                Expr.ofList(functions)
            case _ =>
                report.errorAndAbort(s"\"${body.show}\" cannot be converted to PIVOT expression.", body.asExpr)

    def fetchPivotForMacro[T](
        f: Expr[T],
        tableNames: Expr[List[String]],
        queryContext: Expr[QueryContext]
    )(using q: Quotes): Expr[List[(SqlExpr, List[SqlExpr])]] =
        import q.reflect.*

        val (args, body) = unwrapFuncMacro(f)

        def fetchOne(term: Term): (Expr[SqlExpr], List[Expr[SqlExpr]]) =
            term match
                case Apply(Apply(TypeApply(Apply(TypeApply(Ident("within"), _), expr :: Nil), _), terms :: Nil), _) =>
                    terms match
                        case Inlined(Some(Apply(n, Apply(TypeApply(Select(Ident(t), "apply"), _), terms) :: Nil)), _, _) =>
                            val (forExpr, forInfo) = ExprMacro.treeInfoMacro(args, tableNames, expr, queryContext, false, false, false)

                            val in = terms
                                .map(t => ExprMacro.treeInfoMacro(args, tableNames, t, queryContext, false, false, false))

                            val info = forInfo :: in.map(_._2)

                            for i <- info do
                                if i.hasAgg then
                                    report.error("Aggregate function calls cannot be nested.", term.asExpr)
                                if i.hasWindow then
                                    report.error("Aggregate function calls cannot contain window function calls.", term.asExpr)

                            val forType = expr.tpe.widen

                            for t <- terms do
                                if !(t.tpe.widen =:= forType) then
                                    report.error("Expressions must have the same type.", t.asExpr)

                            forExpr -> in.map(_._1)
                        case _ =>
                            report.errorAndAbort(s"\"${body.show}\" cannot be converted to PIVOT expression.", body.asExpr)
                case _ =>
                    report.errorAndAbort(s"\"${body.show}\" cannot be converted to PIVOT expression.", body.asExpr)

        val forList = body match
            case Apply(TypeApply(Select(Ident(t), "apply"), _), terms)
                if t.startsWith("Tuple")
            =>
                terms.map(t => fetchOne(t))
            case _ =>
                fetchOne(body) :: Nil

        val forExpr = Expr.ofList(forList.map(_._1))
        val inExpr = Expr.ofList(forList.map(_._2).map(Expr.ofList))

        '{
            $forExpr.zip($inExpr)
        }

    def fetchSetMacro[T](
        f: Expr[T],
        tableNames: Expr[List[String]],
        queryContext: Expr[QueryContext]
    )(using q: Quotes): Expr[(SqlExpr, SqlExpr)] =
        import q.reflect.*

        val (args, body) = unwrapFuncMacro(f)

        body match
            case Apply(
                Apply(
                    TypeApply(
                        Apply(
                            TypeApply(Ident(":="), _),
                            TypeApply(
                                Select(
                                    Apply(
                                        Select(ident@Ident(objectName), "selectDynamic"),
                                        Literal(StringConstant(valName)) :: Nil
                                    ),
                                    "$asInstanceOf$"
                                ),
                                _
                            )
                            :: Nil
                        ),
                        _
                    ),
                    value :: Nil
                ),
                _
            ) =>
                val columnExpr = ident.tpe.asType match
                    case '[Table[t]] =>
                        val metaDataExpr = TableMacro.tableMetaDataMacro[t]
                        val valueNameExpr = Expr(valName)
                        '{
                            val columnName =
                                $metaDataExpr.fieldNames
                                    .zip($metaDataExpr.columnNames)
                                    .find(_._1 == $valueNameExpr)
                                    .map(_._2)
                                    .get
                            SqlExpr.Column(None, columnName)
                        }
                val (valueExpr, _) =
                    ExprMacro.treeInfoMacro(args, tableNames, value, queryContext, false, false, false)
                Expr.ofTuple(columnExpr, valueExpr)
            case _ =>
                report.errorAndAbort(
                    s"\"${body.show}\" cannot be converted to SET clause.",
                    body.asExpr
                )

    def fetchInsertMacro[T](
        f: Expr[T],
        tableNames: Expr[List[String]]
    )(using q: Quotes): Expr[List[SqlExpr]] =
        import q.reflect.*

        val (args, body) = unwrapFuncMacro(f)

        def column(term: q.reflect.Term): Expr[SqlExpr] =
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
                        case '[Table[t]] =>
                            val metaDataExpr = TableMacro.tableMetaDataMacro[t]
                            val valueNameExpr = Expr(valName)
                            '{
                                val columnName =
                                    $metaDataExpr.fieldNames
                                        .zip($metaDataExpr.columnNames)
                                        .find(_._1 == $valueNameExpr)
                                        .map(_._2)
                                        .get
                                SqlExpr.Column(None, columnName)
                            }
                case _ =>
                    report.errorAndAbort(
                        s"\"${body.show}\" cannot be converted to INTO clause.",
                        body.asExpr
                    )

        body match
            case Apply(TypeApply(Select(Ident(t), "apply"), _), terms)
                if t.startsWith("Tuple")
            =>
                Expr.ofList(terms.map(t => column(t)))
            case t =>
                Expr.ofList(column(t) :: Nil)

    def bindGeneratedPrimaryKeyMacro[A: Type](
        id: Expr[Long],
        entity: Expr[A]
    )(using q: Quotes): Expr[A] =
        import q.reflect.*

        val tpr = TypeRepr.of[A]
        val fields = tpr.typeSymbol.declaredFields
        val ctor = tpr.typeSymbol.primaryConstructor

        val terms = fields.map: f =>
            val autoInc = f.annotations.find:
                case Apply(Select(New(TypeIdent("autoInc")), _), _) => true
                case _ => false
            if autoInc.isDefined then
                f.typeRef.asType match
                    case '[Long] =>
                        id.asTerm
                    case '[Int] =>
                        '{ $id.toInt }.asTerm
            else
                Select.unique(entity.asTerm, f.name)

        New(Inferred(tpr)).select(ctor).appliedToArgs(terms).asExprOf[A]

    private def unwrapFuncMacro[T](
        value: Expr[T]
    )(using q: Quotes): (args: List[String], body: q.reflect.Term) =
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
            case Block(blockTerm :: Nil, _) =>
                blockTerm
            case _ =>
                report.errorAndAbort(
                    s"\"${term.show}\" cannot be converted to SQL expression.",
                    term.asExpr
                )

        val args = func match
            case DefDef(_, params :: Nil, _, _) =>
                params.params.asInstanceOf[List[ValDef]].map:
                    case v@ValDef(argName, _, _) =>
                        if argName.startsWith("_") || argName.endsWith("_") then
                            report.error("Parameter names cannot start or end with \"_\".", v.pos)
                        argName
            case _ =>
                report.errorAndAbort(
                    s"\"${term.show}\" cannot be converted to SQL expression.",
                    term.asExpr
                )

        val body = func match
            case DefDef(_, _, _, Some(funBody)) =>
                funBody
            case _ =>
                report.errorAndAbort(
                    s"\"${term.show}\" cannot be converted to SQL expression.",
                    term.asExpr
                )

        (args, body)