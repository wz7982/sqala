package sqala.static.macros

import sqala.ast.expr.SqlExpr
import sqala.ast.order.SqlOrderBy
import sqala.ast.statement.SqlSelectItem
import sqala.static.common.QueryContext

import scala.quoted.*

object ClauseMacro:
    inline def fetchArgNames[T](inline f: T): List[String] =
        ${ fetchArgNamesMacro[T]('f) }

    inline def fetchExpr[T](inline f: T, tableNames: List[String], queryContext: QueryContext): SqlExpr =
        ${ fetchExprMacro[T]('f, 'tableNames, 'queryContext) }

    inline def fetchSortBy[T](inline f: T, tableNames: List[String], queryContext: QueryContext): List[SqlOrderBy] =
        ${ fetchSortByMacro[T]('f, 'tableNames, 'queryContext) }

    inline def fetchGroupBy[T](inline f: T, tableNames: List[String], queryContext: QueryContext): List[SqlExpr] =
        ${ fetchGroupByMacro[T]('f, 'tableNames, 'queryContext) }

    inline def fetchMap[T](
        inline f: T, 
        tableNames: List[String], 
        queryContext: QueryContext
    ): List[SqlSelectItem] =
        ${ fetchMapMacro[T]('f, 'tableNames, 'queryContext) }

    def fetchArgNamesMacro[T](f: Expr[T])(using q: Quotes): Expr[List[String]] =
        val (args, _) = unwrapFuncMacro(f)

        Expr(args)

    def fetchExprMacro[T](
        f: Expr[T], 
        tableNames: Expr[List[String]],
        queryContext: Expr[QueryContext]
    )(using q: Quotes): Expr[SqlExpr] =
        val (args, body) = unwrapFuncMacro(f)

        ExprMacro.treeInfoMacro(args, tableNames, body, queryContext)

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

        Expr.ofList(sort)

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
            case _ =>
                report.errorAndAbort(
                    s"\"${body.show}\" cannot be converted to SQL expression.", 
                    body.asExpr
                )

        Expr.ofList(group)

    def fetchMapMacro[T](
        f: Expr[T], 
        tableNames: Expr[List[String]],
        queryContext: Expr[QueryContext]
    )(using q: Quotes): Expr[List[SqlSelectItem]] =
        import q.reflect.*
        
        val (args, body) = unwrapFuncMacro(f)

        val items = body match
            case Inlined(Some(Apply(n, Apply(TypeApply(Select(Ident(t), "apply"), _), terms) :: Nil)), _, _) 
                if t.startsWith("Tuple")
            =>
                val names = n match
                    case TypeApply(Apply(TypeApply(_, Applied(_, ns) :: Nil), _), _) =>
                        ns.map:
                            case Singleton(Literal(StringConstant(s))) => Expr(s)
                val items = terms
                    .map(t => ExprMacro.treeInfoMacro(args, tableNames, t, queryContext))
                items.zip(names).map: (i, n) =>
                    '{ SqlSelectItem.Item($i, Some($n)) }
            case Apply(TypeApply(Select(Ident(t), "apply"), _), terms) 
                if t.startsWith("Tuple")
            => 
                val items = terms
                    .map(t => ExprMacro.treeInfoMacro(args, tableNames, t, queryContext))
                items.map: i =>
                    '{ SqlSelectItem.Item($i, None) }
            case _ =>
                val i = ExprMacro.treeInfoMacro(args, tableNames, body, queryContext)
                '{ SqlSelectItem.Item($i, None) } :: Nil

        Expr.ofList(items)

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