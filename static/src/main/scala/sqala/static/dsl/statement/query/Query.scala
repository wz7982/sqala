package sqala.static.dsl.statement.query

import sqala.ast.expr.{SqlBinaryOperator, SqlExpr}
import sqala.ast.group.{SqlGroup, SqlGroupingItem}
import sqala.ast.limit.{SqlFetch, SqlFetchMode, SqlFetchUnit, SqlLimit}
import sqala.ast.quantifier.SqlQuantifier
import sqala.ast.statement.*
import sqala.ast.table.{SqlJoinCondition, SqlJoinType, SqlTable, SqlTableAlias}
import sqala.metadata.{Dialect, SqlBoolean}
import sqala.static.dsl.*
import sqala.static.dsl.table.{Table, TransformTableKind}
import sqala.util.queryToString
import sqala.util.NonEmptyList

import scala.compiletime.ops.int.-
import sqala.ir.IrQuery
import sqala.ir.ToAst
import sqala.ir.IrLimit
import sqala.ir.IrFetch
import sqala.ir.IrLock
import sqala.ir.IrLockWaitMode
import sqala.ir.IrGroup
import sqala.ir.IrGroupingItem
import sqala.ir.IrExpr

/**
 * A typed SQL query, the base type for all query operations in the DSL.
 */
class Query[T, OKS <: Tuple, L <: Int, S <: QuerySize](
    private[sqala] val params: T,
    private[sqala] val tree: IrQuery
)(using
    private[sqala] val qc: QueryContext[L]
):
    /**
     * Generates the SQL string for the given dialect.
     *
     * `standardEscapeStrings` controls string literal escaping: `true` treats
     * backslashes literally (standard behavior, e.g. PostgreSQL, Oracle);
     * `false` uses backslashes as escape characters (e.g. MySQL).
     *
     * {{{
     * val sql = q.sql(PostgresqlDialect, true)
     * }}}
     */
    def sql(dialect: Dialect, standardEscapeStrings: Boolean = true): String =
        queryToString(ToAst().toAst(tree), dialect, standardEscapeStrings)

    /**
     * Combines two queries with `UNION`, removing duplicate rows.
     * Both queries must have compatible row types, enforced at compile time.
     *
     * {{{
     * q1.union(q2)
     * }}}
     */
    def union[R, ROKS <: Tuple, RS <: QuerySize](unionQuery: QueryContext[L - 1] ?=> Query[R, ROKS, L, RS])(using
        u: Union[T, R, L],
        c: CombineKindTuple[OKS, ROKS]
    ): UnionQuery[u.R, c.R, L] =
        given QueryContext[L - 1] = QueryContext()
        UnionQuery(
            u.unionQueryItems(params, 1),
            IrQuery.Set(
                tree,
                SqlSetOperator.Union(None),
                unionQuery.tree,
                Nil,
                None
            )
        )

    /**
     * Combines two queries with `UNION ALL`, preserving duplicate rows.
     * Both queries must have compatible row types, enforced at compile time.
     *
     * {{{
     * q1.unionAll(q2)
     * }}}
     */
    def unionAll[R, ROKS <: Tuple, RS <: QuerySize](unionQuery: QueryContext[L - 1] ?=> Query[R, ROKS, L, RS])(using
        u: Union[T, R, L],
        c: CombineKindTuple[OKS, ROKS]
    ): UnionQuery[u.R, c.R, L] =
        given QueryContext[L - 1] = QueryContext()
        UnionQuery(
            u.unionQueryItems(params, 1),
            IrQuery.Set(
                tree,
                SqlSetOperator.Union(Some(SqlQuantifier.All)),
                unionQuery.tree,
                Nil,
                None
            )
        )

    /**
     * Computes the set difference with `EXCEPT`, removing duplicates.
     * Both queries must have compatible row types, enforced at compile time.
     *
     * {{{
     * q1.except(q2)
     * }}}
     */
    def except[R, ROKS <: Tuple, RS <: QuerySize](unionQuery: QueryContext[L - 1] ?=> Query[R, ROKS, L, RS])(using
        u: Union[T, R, L],
        c: CombineKindTuple[OKS, ROKS]
    ): UnionQuery[u.R, c.R, L] =
        given QueryContext[L - 1] = QueryContext()
        UnionQuery(
            u.unionQueryItems(params, 1),
            IrQuery.Set(
                tree,
                SqlSetOperator.Except(None),
                unionQuery.tree,
                Nil,
                None
            )
        )

    /**
     * Computes the set difference with `EXCEPT ALL`, preserving duplicates.
     * Both queries must have compatible row types, enforced at compile time.
     *
     * {{{
     * q1.exceptAll(q2)
     * }}}
     */
    def exceptAll[R, ROKS <: Tuple, RS <: QuerySize](unionQuery: QueryContext[L - 1] ?=> Query[R, ROKS, L, RS])(using
        u: Union[T, R, L],
        c: CombineKindTuple[OKS, ROKS]
    ): UnionQuery[u.R, c.R, L] =
        given QueryContext[L - 1] = QueryContext()
        UnionQuery(
            u.unionQueryItems(params, 1),
            IrQuery.Set(
                tree,
                SqlSetOperator.Except(Some(SqlQuantifier.All)),
                unionQuery.tree,
                Nil,
                None
            )
        )

    /**
     * Computes the set intersection with `INTERSECT`, removing duplicates.
     * Both queries must have compatible row types, enforced at compile time.
     *
     * {{{
     * q1.intersect(q2)
     * }}}
     */
    def intersect[R, ROKS <: Tuple, RS <: QuerySize](unionQuery: QueryContext[L - 1] ?=> Query[R, ROKS, L, RS])(using
        u: Union[T, R, L],
        c: CombineKindTuple[OKS, ROKS]
    ): UnionQuery[u.R, c.R, L] =
        given QueryContext[L - 1] = QueryContext()
        UnionQuery(
            u.unionQueryItems(params, 1),
            IrQuery.Set(
                tree,
                SqlSetOperator.Intersect(None),
                unionQuery.tree,
                Nil,
                None
            )
        )

    /**
     * Computes the set intersection with `INTERSECT ALL`, preserving duplicates.
     * Both queries must have compatible row types, enforced at compile time.
     *
     * {{{
     * q1.intersectAll(q2)
     * }}}
     */
    def intersectAll[R, ROKS <: Tuple, RS <: QuerySize](unionQuery: QueryContext[L - 1] ?=> Query[R, ROKS, L, RS])(using
        u: Union[T, R, L],
        c: CombineKindTuple[OKS, ROKS]
    ): UnionQuery[u.R, c.R, L] =
        given QueryContext[L - 1] = QueryContext()
        UnionQuery(
            u.unionQueryItems(params, 1),
            IrQuery.Set(
                tree,
                SqlSetOperator.Intersect(Some(SqlQuantifier.All)),
                unionQuery.tree,
                Nil,
                None
            )
        )

     /**
     * Wraps the query as a `COUNT(*)` subquery, removing any existing
     * `LIMIT` and `ORDER BY` to produce a valid scalar subquery.
     */
    // private[sqala] def size: Query[Expr[Long, Column[L]], EmptyTuple, L, OneRow] =
    //     val countExpr = count()
    //     val expr = Expr[Long, Column[L]](countExpr.asIrExpr)
    //     tree match
    //         case s@SqlQuery.Select(p, _, _, _, None, _, _, _, _, _, _)
    //             if p != Some(SqlQuantifier.Distinct)
    //         =>
    //             Query(
    //                 expr,
    //                 s.copy(
    //                     select = SqlSelectItem.Expr(expr.asIrExpr, None) :: Nil,
    //                     limit = None,
    //                     orderBy = Nil
    //                 )
    //             )
    //         case _ =>
    //             def removeLimitAndOrderBy(tree: SqlQuery): SqlQuery =
    //                 tree match
    //                     case s: SqlQuery.Select =>
    //                         s.copy(limit = None, orderBy = Nil)
    //                     case s: SqlQuery.Set =>
    //                         s.copy(limit = None, orderBy = Nil)
    //                     case v: SqlQuery.Values =>
    //                         v.copy(limit = None, orderBy = Nil)
    //                     case c: SqlQuery.With =>
    //                         c.copy(query = removeLimitAndOrderBy(c.query))

    //             val outerQuery: SqlQuery.Select = SqlQuery.Select(
    //                 None,
    //                 SqlSelectItem.Expr(expr.asIrExpr, None) :: Nil,
    //                 SqlTable.Subquery(
    //                     false,
    //                     removeLimitAndOrderBy(tree),
    //                     Some(SqlTableAlias("t", Nil)),
    //                     None
    //                 ) :: Nil,
    //                 None,
    //                 None,
    //                 None,
    //                 Nil,
    //                 None,
    //                 Nil,
    //                 None,
    //                 None
    //             )
    //             Query(expr, outerQuery)

    /**
     * Wraps the query as an `EXISTS` subquery, producing a boolean
     * expression.
     */
    // private[sqala] def exists: Query[Expr[Boolean, Column[L]], EmptyTuple, L, OneRow] =
    //     val expr = Expr[Boolean, Column[L]](SqlExpr.ExistsPredicate(tree))
    //     val outerQuery: SqlQuery.Select = SqlQuery.Select(
    //         None,
    //         SqlSelectItem.Expr(expr.asIrExpr, None) :: Nil,
    //         Nil,
    //         None,
    //         None,
    //         None,
    //         Nil,
    //         None,
    //         Nil,
    //         None,
    //         None
    //     )
    //     Query(expr, outerQuery)