package sqala.static.macros

import sqala.ast.expr.SqlExpr
import sqala.ast.order.SqlOrderBy
import sqala.ast.statement.*
import sqala.static.common.*
import sqala.static.statement.query.*

import scala.quoted.*

object ClauseMacro:
    inline def fetchArgNames[T](inline f: T): List[String] =
        ${ fetchArgNamesMacro[T]('f) }

    inline def fetchFilter[T](
        inline f: T, 
        inline inGroup: Boolean,
        tableNames: List[String], 
        queryContext: QueryContext
    ): SqlExpr =
        ${ fetchFilterMacro[T]('f, 'inGroup, 'tableNames, 'queryContext) }

    inline def fetchSortBy[T](
        inline f: T, 
        tableNames: List[String], 
        queryContext: QueryContext
    ): List[SqlOrderBy] =
        ${ fetchSortByMacro[T]('f, 'tableNames, 'queryContext) }

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
        tableNames: List[String], 
        queryContext: QueryContext
    ): List[SqlSelectItem] =
        ${ fetchGroupedMapMacro[T]('f, 'tableNames, 'queryContext) }

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

    def fetchArgNamesMacro[T](f: Expr[T])(using q: Quotes): Expr[List[String]] =
        val (args, _) = unwrapFuncMacro(f)

        Expr(args)

    def fetchFilterMacro[T](
        f: Expr[T], 
        inGroup: Expr[Boolean],
        tableNames: Expr[List[String]],
        queryContext: Expr[QueryContext]
    )(using q: Quotes): Expr[SqlExpr] =
        import q.reflect.*

        val (args, body) = unwrapFuncMacro(f)

        val inGroupValue = inGroup.value.get

        val (expr, info) = ExprMacro.treeInfoMacro(args, tableNames, body, queryContext)

        if inGroupValue then
            if info.hasWindow then
                report.error("Window functions are not allowed in HAVING.", body.asExpr)

            val ungrouped = info.ungroupedRef

            if ungrouped.nonEmpty then
                val c = ungrouped.head
                    report.error(
                        s"Column \"${c._1}.${c._2}\" must appear in the GROUP BY clause or be used in an aggregate function.",
                        body.asExpr
                    )
        else
            if info.hasAgg then
                report.error("Aggregate functions are not allowed in WHERE/ON.", body.asExpr)

            if info.hasWindow then
                report.error("Window functions are not allowed in WHERE/ON.", body.asExpr)

        expr

    def fetchSortByMacro[T](
        f: Expr[T], 
        tableNames: Expr[List[String]],
        queryContext: Expr[QueryContext]
    )(using q: Quotes): Expr[List[SqlOrderBy]] =
        import q.reflect.*
        
        val (args, body) = unwrapFuncMacro(f)

        val sort = body match
            case Apply(TypeApply(Select(Ident(t), "apply"), _), terms) 
                if t.startsWith("Tuple")
            => 
                terms.map(t => ExprMacro.sortInfoMacro(args, tableNames, t, queryContext))
            case _ =>
                ExprMacro.sortInfoMacro(args, tableNames, body, queryContext) :: Nil

        val info = sort.map(_._2)

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
                terms.map(t => ExprMacro.treeInfoMacro(args, tableNames, t, queryContext))
            case Apply(TypeApply(Select(Ident(t), "apply"), _), terms) =>
                terms.map(t => ExprMacro.treeInfoMacro(args, tableNames, t, queryContext))
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
                    .map(t => ExprMacro.treeInfoMacro(args, tableNames, t, queryContext))
                val exprs = items.map(_._1).zip(names).map: (i, n) =>
                    '{ SqlSelectItem.Item($i, Some($n)) }
                exprs -> items.map(_._2)
            case Apply(TypeApply(Select(Ident(t), "apply"), _), terms) 
                if t.startsWith("Tuple")
            => 
                val items = terms
                    .map(t => ExprMacro.treeInfoMacro(args, tableNames, t, queryContext))
                val exprs = items.map(_._1).map: i =>
                    '{ SqlSelectItem.Item($i, None) }
                exprs -> items.map(_._2)
            case _ =>
                val (expr, info) = ExprMacro.treeInfoMacro(args, tableNames, body, queryContext)
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
                Query[T, OneRow]($ast.copy(select = $itemsExpr))
            }
        else 
            '{
                Query[T, ManyRows]($ast.copy(select = $itemsExpr))
            }

    def fetchGroupedMapMacro[T](
        f: Expr[T], 
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
                    .map(t => ExprMacro.treeInfoMacro(args, tableNames, t, queryContext))
                val exprs = items.map(_._1).zip(names).map: (i, n) =>
                    '{ SqlSelectItem.Item($i, Some($n)) }
                exprs -> items.map(_._2)
            case Apply(TypeApply(Select(Ident(t), "apply"), _), terms) 
                if t.startsWith("Tuple")
            => 
                val items = terms
                    .map(t => ExprMacro.treeInfoMacro(args, tableNames, t, queryContext))
                val exprs = items.map(_._1).map: i =>
                    '{ SqlSelectItem.Item($i, None) }
                exprs -> items.map(_._2)
            case _ =>
                val (expr, info) = ExprMacro.treeInfoMacro(args, tableNames, body, queryContext)
                ('{ SqlSelectItem.Item($expr, None) } :: Nil) -> (info :: Nil)

        for i <- info do
            if i.nonAggRef.nonEmpty && i.ungroupedRef.nonEmpty && i.nonAggRef.exists(c => i.ungroupedRef.contains(c)) then
                val c = i.ungroupedRef.head
                    report.error(
                        s"Column \"${c._1}.${c._2}\" must appear in the GROUP BY clause or be used in an aggregate function.",
                        body.asExpr
                    )

        Expr.ofList(items)

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
                    ExprMacro.treeInfoMacro(args, tableNames, value, queryContext)
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
                    case ValDef(argName, _, _) =>
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