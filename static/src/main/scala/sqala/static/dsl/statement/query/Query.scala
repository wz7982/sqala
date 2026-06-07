package sqala.static.dsl.statement.query

import sqala.ast.expr.{SqlBinaryOperator, SqlExpr, SqlSubqueryQuantifier}
import sqala.ast.group.{SqlGroupBy, SqlGroupingItem}
import sqala.ast.limit.{SqlFetch, SqlFetchMode, SqlFetchUnit, SqlLimit}
import sqala.ast.quantifier.SqlQuantifier
import sqala.ast.statement.*
import sqala.ast.table.{SqlJoinCondition, SqlJoinType, SqlTable, SqlTableAlias}
import sqala.metadata.{Dialect, SqlBoolean}
import sqala.static.dsl.*
import sqala.static.dsl.table.{Table, TransformTableKind}
import sqala.util.queryToString

import scala.compiletime.ops.int.-

/**
 * A typed SQL query, the base type for all query operations in the DSL.
 */
sealed class Query[T, OKS <: Tuple, L <: Int, S <: QuerySize](
    private[sqala] val params: T,
    private[sqala] val tree: SqlQuery
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
        queryToString(tree, dialect, standardEscapeStrings)

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
        given QueryContext[L - 1] = qc.asInstanceOf[QueryContext[L - 1]]
        UnionQuery(
            u.unionQueryItems(params, 1),
            SqlQuery.Set(
                tree,
                SqlSetOperator.Union(None),
                unionQuery.tree,
                Nil,
                None,
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
        given QueryContext[L - 1] = qc.asInstanceOf[QueryContext[L - 1]]
        UnionQuery(
            u.unionQueryItems(params, 1),
            SqlQuery.Set(
                tree,
                SqlSetOperator.Union(Some(SqlQuantifier.All)),
                unionQuery.tree,
                Nil,
                None,
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
        given QueryContext[L - 1] = qc.asInstanceOf[QueryContext[L - 1]]
        UnionQuery(
            u.unionQueryItems(params, 1),
            SqlQuery.Set(
                tree,
                SqlSetOperator.Except(None),
                unionQuery.tree,
                Nil,
                None,
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
        given QueryContext[L - 1] = qc.asInstanceOf[QueryContext[L - 1]]
        UnionQuery(
            u.unionQueryItems(params, 1),
            SqlQuery.Set(
                tree,
                SqlSetOperator.Except(Some(SqlQuantifier.All)),
                unionQuery.tree,
                Nil,
                None,
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
        given QueryContext[L - 1] = qc.asInstanceOf[QueryContext[L - 1]]
        UnionQuery(
            u.unionQueryItems(params, 1),
            SqlQuery.Set(
                tree,
                SqlSetOperator.Intersect(None),
                unionQuery.tree,
                Nil,
                None,
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
        given QueryContext[L - 1] = qc.asInstanceOf[QueryContext[L - 1]]
        UnionQuery(
            u.unionQueryItems(params, 1),
            SqlQuery.Set(
                tree,
                SqlSetOperator.Intersect(Some(SqlQuantifier.All)),
                unionQuery.tree,
                Nil,
                None,
                None
            )
        )

    /**
     * Skips the first `n` rows of the result. Maps to `OFFSET`.
     *
     * {{{
     * from(User).drop(10).take(5)
     * }}}
     */
    def drop(n: Int): Query[T, OKS, L, S] =
        val sqlExpr = n.asExpr.asSqlExpr
        val limit = tree match
            case s: SqlQuery.Select => s.limit
            case s: SqlQuery.Set => s.limit
            case SqlQuery.Cte(_, _, s: SqlQuery.Select, _) => s.limit
            case SqlQuery.Cte(_, _, s: SqlQuery.Set, _) => s.limit
            case _ => None
        val sqlLimit = limit
            .map(l => SqlLimit(Some(sqlExpr), l.fetch))
            .orElse(Some(SqlLimit(Some(sqlExpr), None)))
        val newTree = tree match
            case s: SqlQuery.Select => s.copy(limit = sqlLimit)
            case s: SqlQuery.Set => s.copy(limit = sqlLimit)
            case SqlQuery.Cte(w, r, s: SqlQuery.Select, l) =>
                SqlQuery.Cte(w, r, s.copy(limit = sqlLimit), l)
            case SqlQuery.Cte(w, r, s: SqlQuery.Set, l) =>
                SqlQuery.Cte(w, r, s.copy(limit = sqlLimit), l)
            case _ => tree
        Query(params, newTree)

    /**
     * Alias of `drop`, provided for users familiar with `OFFSET`.
     *
     * {{{
     * from(User).offset(10).limit(5)
     * }}}
     */
    def offset(n: Int): Query[T, OKS, L, S] =
        drop(n)

    /**
     * Internal implementation for `take` and `takeWithTies`.
     * Handles merging an existing `OFFSET` with the new `LIMIT`/`FETCH`
     * clause, and propagates the new fetch specification through
     * `Select`, `Set`, and `Cte` query types.
     */
    private[sqala] def takeExpr[TS <: QuerySize](n: SqlExpr, unit: SqlFetchUnit, mode: SqlFetchMode): Query[T, OKS, L, TS] =
        val limit = tree match
            case s: SqlQuery.Select => s.limit
            case s: SqlQuery.Set => s.limit
            case SqlQuery.Cte(_, _, s: SqlQuery.Select, _) => s.limit
            case SqlQuery.Cte(_, _, s: SqlQuery.Set, _) => s.limit
            case _ => None
        val sqlLimit = limit
            .map(l => SqlLimit(l.offset, Some(SqlFetch(n, unit, mode))))
            .orElse(Some(SqlLimit(None, Some(SqlFetch(n, unit, mode)))))
        val newTree = tree match
            case s: SqlQuery.Select => s.copy(limit = sqlLimit)
            case s: SqlQuery.Set => s.copy(limit = sqlLimit)
            case SqlQuery.Cte(w, r, s: SqlQuery.Select, l) =>
                SqlQuery.Cte(w, r, s.copy(limit = sqlLimit), l)
            case SqlQuery.Cte(w, r, s: SqlQuery.Set, l) =>
                SqlQuery.Cte(w, r, s.copy(limit = sqlLimit), l)
            case _ => tree
        Query(params, newTree)

    /**
     * Limits the result to at most `n` rows. Maps to `LIMIT` or `FETCH NEXT`.
     *
     * {{{
     * from(User).drop(10).take(5)
     * }}}
     */
    def take(n: Int)(using
        s: TakeSize[n.type]
    ): Query[T, OKS, L, s.R] =
        takeExpr[s.R](n.asExpr.asSqlExpr, SqlFetchUnit.RowCount, SqlFetchMode.Only)

    /**
     * Alias of `take`, provided for users familiar with `LIMIT`.
     *
     * {{{
     * from(User).offset(10).limit(5)
     * }}}
     */
    def limit(n: Int)(using
        s: TakeSize[n.type]
    ): Query[T, OKS, L, s.R] =
        take(n)

    /**
     * Locks selected rows for update. Maps to `FOR UPDATE`.
     *
     * {{{
     * from(User).forUpdate
     * }}}
     */
    def forUpdate: Query[T, OKS, L, S] =
        val newTree = tree match
            case s: SqlQuery.Select => s.copy(lock = Some(SqlLock.Update(None)))
            case s: SqlQuery.Set => s.copy(lock = Some(SqlLock.Update(None)))
            case c: SqlQuery.Cte => c.copy(lock = Some(SqlLock.Update(None)))
            case v: SqlQuery.Values => v.copy(lock = Some(SqlLock.Update(None)))
        Query(params, newTree)

    /**
     * Locks selected rows for update, failing immediately if rows are
     * already locked. Maps to `FOR UPDATE NOWAIT`.
     *
     * {{{
     * from(User).forUpdateNoWait
     * }}}
     */
    def forUpdateNoWait: Query[T, OKS, L, S] =
        val newTree = tree match
            case s: SqlQuery.Select => s.copy(lock = Some(SqlLock.Update(Some(SqlLockWaitMode.NoWait))))
            case s: SqlQuery.Set => s.copy(lock = Some(SqlLock.Update(Some(SqlLockWaitMode.NoWait))))
            case c: SqlQuery.Cte => c.copy(lock = Some(SqlLock.Update(Some(SqlLockWaitMode.NoWait))))
            case v: SqlQuery.Values => v.copy(lock = Some(SqlLock.Update(Some(SqlLockWaitMode.NoWait))))
        Query(params, newTree)

    /**
     * Locks selected rows for update, skipping already locked rows.
     * Maps to `FOR UPDATE SKIP LOCKED`.
     *
     * {{{
     * from(User).forUpdateSkipLocked
     * }}}
     */
    def forUpdateSkipLocked: Query[T, OKS, L, S] =
        val newTree = tree match
            case s: SqlQuery.Select => s.copy(lock = Some(SqlLock.Update(Some(SqlLockWaitMode.SkipLocked))))
            case s: SqlQuery.Set => s.copy(lock = Some(SqlLock.Update(Some(SqlLockWaitMode.SkipLocked))))
            case c: SqlQuery.Cte => c.copy(lock = Some(SqlLock.Update(Some(SqlLockWaitMode.SkipLocked))))
            case v: SqlQuery.Values => v.copy(lock = Some(SqlLock.Update(Some(SqlLockWaitMode.SkipLocked))))
        Query(params, newTree)

    /**
     * Locks selected rows in share mode, allowing other transactions to
     * read but not update. Maps to `FOR SHARE`.
     *
     * {{{
     * from(User).forShare
     * }}}
     */
    def forShare: Query[T, OKS, L, S] =
        val newTree = tree match
            case s: SqlQuery.Select => s.copy(lock = Some(SqlLock.Share(None)))
            case s: SqlQuery.Set => s.copy(lock = Some(SqlLock.Share(None)))
            case c: SqlQuery.Cte => c.copy(lock = Some(SqlLock.Share(None)))
            case v: SqlQuery.Values => v.copy(lock = Some(SqlLock.Share(None)))
        Query(params, newTree)

    /**
     * Locks selected rows in share mode, failing immediately if rows
     * are already locked. Maps to `FOR SHARE NOWAIT`.
     *
     * {{{
     * from(User).forShareNoWait
     * }}}
     */
    def forShareNoWait: Query[T, OKS, L, S] =
        val newTree = tree match
            case s: SqlQuery.Select => s.copy(lock = Some(SqlLock.Share(Some(SqlLockWaitMode.NoWait))))
            case s: SqlQuery.Set => s.copy(lock = Some(SqlLock.Share(Some(SqlLockWaitMode.NoWait))))
            case c: SqlQuery.Cte => c.copy(lock = Some(SqlLock.Share(Some(SqlLockWaitMode.NoWait))))
            case v: SqlQuery.Values => v.copy(lock = Some(SqlLock.Share(Some(SqlLockWaitMode.NoWait))))
        Query(params, newTree)

    /**
     * Locks selected rows in share mode, skipping already locked rows.
     * Maps to `FOR SHARE SKIP LOCKED`.
     *
     * {{{
     * from(User).forShareSkipLocked
     * }}}
     */
    def forShareSkipLocked: Query[T, OKS, L, S] =
        val newTree = tree match
            case s: SqlQuery.Select => s.copy(lock = Some(SqlLock.Share(Some(SqlLockWaitMode.SkipLocked))))
            case s: SqlQuery.Set => s.copy(lock = Some(SqlLock.Share(Some(SqlLockWaitMode.SkipLocked))))
            case c: SqlQuery.Cte => c.copy(lock = Some(SqlLock.Share(Some(SqlLockWaitMode.SkipLocked))))
            case v: SqlQuery.Values => v.copy(lock = Some(SqlLock.Share(Some(SqlLockWaitMode.SkipLocked))))
        Query(params, newTree)

     /**
     * Wraps the query as a `COUNT(*)` subquery, removing any existing
     * `LIMIT` and `ORDER BY` to produce a valid scalar subquery.
     */
    private[sqala] def size: Query[Expr[Long, Column[L]], EmptyTuple, L, OneRow] =
        val countExpr = count()
        val expr = Expr[Long, Column[L]](countExpr.asSqlExpr)
        tree match
            case s@SqlQuery.Select(p, _, _, _, None, _, _, _, _)
                if p != Some(SqlQuantifier.Distinct)
            =>
                Query(
                    expr,
                    s.copy(
                        select = SqlSelectItem.Expr(expr.asSqlExpr, None) :: Nil,
                        limit = None,
                        orderBy = Nil
                    )
                )
            case _ =>
                def removeLimitAndOrderBy(tree: SqlQuery): SqlQuery =
                    tree match
                        case s: SqlQuery.Select => s.copy(limit = None, orderBy = Nil)
                        case s: SqlQuery.Set => s.copy(limit = None)
                        case c: SqlQuery.Cte => c.copy(query = removeLimitAndOrderBy(c.query))
                        case _ => tree
                val outerQuery: SqlQuery.Select = SqlQuery.Select(
                    None,
                    SqlSelectItem.Expr(expr.asSqlExpr, None) :: Nil,
                    SqlTable.Subquery(
                        false,
                        removeLimitAndOrderBy(tree),
                        Some(SqlTableAlias("t", Nil)),
                        None
                    ) :: Nil,
                    None,
                    None,
                    None,
                    Nil,
                    None,
                    None
                )
                Query(expr, outerQuery)

    /**
     * Wraps the query as an `EXISTS` subquery, producing a boolean
     * expression.
     */
    private[sqala] def exists: Query[Expr[Boolean, Column[L]], EmptyTuple, L, OneRow] =
        val expr = Expr[Boolean, Column[L]](SqlExpr.Subquery(Some(SqlSubqueryQuantifier.Exists), tree))
        val outerQuery: SqlQuery.Select = SqlQuery.Select(
            None,
            SqlSelectItem.Expr(expr.asSqlExpr, None) :: Nil,
            Nil,
            None,
            None,
            None,
            Nil,
            None,
            None
        )
        Query(expr, outerQuery)

/**
 * A query with an `ORDER BY` clause, enabling `takeWithTies`/`limitWithTies`
 * in addition to all `Query` operations.
 */
sealed class SortedQuery[T, OKS <: Tuple, L <: Int, S <: QuerySize](
    override private[sqala] val params: T,
    override private[sqala] val tree: SqlQuery
)(using
    override private[sqala] val qc: QueryContext[L]
) extends Query[T, OKS, L, S](params, tree):
    override def drop(n: Int): SortedQuery[T, OKS, L, S] =
        val sqlExpr = n.asExpr.asSqlExpr
        val limit = tree match
            case s: SqlQuery.Select => s.limit
            case s: SqlQuery.Set => s.limit
            case SqlQuery.Cte(_, _, s: SqlQuery.Select, _) => s.limit
            case SqlQuery.Cte(_, _, s: SqlQuery.Set, _) => s.limit
            case _ => None
        val sqlLimit = limit
            .map(l => SqlLimit(Some(sqlExpr), l.fetch))
            .orElse(Some(SqlLimit(Some(sqlExpr), None)))
        val newTree = tree match
            case s: SqlQuery.Select => s.copy(limit = sqlLimit)
            case s: SqlQuery.Set => s.copy(limit = sqlLimit)
            case SqlQuery.Cte(w, r, s: SqlQuery.Select, l) =>
                SqlQuery.Cte(w, r, s.copy(limit = sqlLimit), l)
            case SqlQuery.Cte(w, r, s: SqlQuery.Set, l) =>
                SqlQuery.Cte(w, r, s.copy(limit = sqlLimit), l)
            case _ => tree
        SortedQuery(params, newTree)

    override def offset(n: Int): SortedQuery[T, OKS, L, S] =
        drop(n)

    override private[sqala] def takeExpr[TS <: QuerySize](n: SqlExpr, unit: SqlFetchUnit, mode: SqlFetchMode): SortedQuery[T, OKS, L, TS] =
        val limit = tree match
            case s: SqlQuery.Select => s.limit
            case s: SqlQuery.Set => s.limit
            case SqlQuery.Cte(_, _, s: SqlQuery.Select, _) => s.limit
            case SqlQuery.Cte(_, _, s: SqlQuery.Set, _) => s.limit
            case _ => None
        val sqlLimit = limit
            .map(l => SqlLimit(l.offset, Some(SqlFetch(n, unit, mode))))
            .orElse(Some(SqlLimit(None, Some(SqlFetch(n, unit, mode)))))
        val newTree = tree match
            case s: SqlQuery.Select => s.copy(limit = sqlLimit)
            case s: SqlQuery.Set => s.copy(limit = sqlLimit)
            case SqlQuery.Cte(w, r, s: SqlQuery.Select, l) =>
                SqlQuery.Cte(w, r, s.copy(limit = sqlLimit), l)
            case SqlQuery.Cte(w, r, s: SqlQuery.Set, l) =>
                SqlQuery.Cte(w, r, s.copy(limit = sqlLimit), l)
            case _ => tree
        SortedQuery(params, newTree)

    /**
     * Limits the result to at most `n` rows, including rows tied with
     * the last row's sort key. Maps to `FETCH NEXT n ROWS WITH TIES`.
     * Only available on sorted queries.
     *
     * {{{
     * from(User).sortBy(u => u.name).takeWithTies(5)
     * }}}
     */
    def takeWithTies(n: Int)(using
        s: TakeSize[n.type]
    ): SortedQuery[T, OKS, L, s.R] =
        takeExpr[s.R](n.asExpr.asSqlExpr, SqlFetchUnit.RowCount, SqlFetchMode.WithTies)

    /**
     * Alias of `takeWithTies`, provided for users familiar with `LIMIT`.
     *
     * {{{
     * from(User).sortBy(u => u.name).limitWithTies(5)
     * }}}
     */
    def limitWithTies(n: Int)(using
        s: TakeSize[n.type]
    ): SortedQuery[T, OKS, L, s.R] =
        takeWithTies(n)

/**
 * A `SELECT` query. Created by `from`.
 */
final case class SelectQuery[T, OKS <: Tuple, L <: Int](
    override private[sqala] val params: T,
    override private[sqala] val tree: SqlQuery.Select
)(using
    override private[sqala] val qc: QueryContext[L]
) extends Query[T, OKS, L, ManyRows](params, tree):
    /**
     * Adds a `WHERE` clause to the query. The condition must be a valid
     * filter expression — aggregate functions and window functions are
     * rejected at compile time.
     *
     * {{{
     * from(User).filter(u => u.id > 1)
     * }}}
     */
    def filter[F](f: QueryContext[L] ?=> T => F)(using
        a: AsExpr[F, L],
        b: SqlBoolean[a.R],
        kt: KindToTuple[a.K],
        i: CanInFilter[kt.R],
        e: ExcludeCurrentLevelColumn[kt.R, L],
        c: CombineKindTuple[OKS, e.R]
    ): SelectQuery[T, c.R, L] =
        val cond = a.asExpr(f(params))
        SelectQuery(params, tree.addWhere(cond.asSqlExpr))

    /**
     * Alias of `filter`, provided for users familiar with `WHERE`.
     *
     * {{{
     * from(User).where(u => u.id > 1)
     * }}}
     */
    def where[F](f: QueryContext[L] ?=> T => F)(using
        a: AsExpr[F, L],
        b: SqlBoolean[a.R],
        kt: KindToTuple[a.K],
        i: CanInFilter[kt.R],
        e: ExcludeCurrentLevelColumn[kt.R, L],
        c: CombineKindTuple[OKS, e.R]
    ): SelectQuery[T, c.R, L] =
        filter(f)

    /**
     * Conditionally adds a `WHERE` clause. The condition is only applied
     * when `test` evaluates to `true`. Useful for building dynamic queries
     * without manually concatenating `WHERE TRUE`.
     *
     * {{{
     * val name: Option[String] = None
     * from(User).filterIf(name.isDefined)(u => u.name == name)
     * }}}
     */
    def filterIf[F](test: => Boolean)(f: QueryContext[L] ?=> T => F)(using
        a: AsExpr[F, L],
        b: SqlBoolean[a.R],
        kt: KindToTuple[a.K],
        i: CanInFilter[kt.R],
        e: ExcludeCurrentLevelColumn[kt.R, L],
        c: CombineKindTuple[OKS, e.R]
    ): SelectQuery[T, c.R, L] =
        if test then filter(f) else this.asInstanceOf[SelectQuery[T, c.R, L]]

    /**
     * Alias of `filterIf`, provided for users familiar with `WHERE`.
     *
     * {{{
     * from(User).whereIf(name.isDefined)(u => u.name == name)
     * }}}
     */
    def whereIf[F](test: => Boolean)(f: QueryContext[L] ?=> T => F)(using
        a: AsExpr[F, L],
        b: SqlBoolean[a.R],
        kt: KindToTuple[a.K],
        i: CanInFilter[kt.R],
        e: ExcludeCurrentLevelColumn[kt.R, L],
        c: CombineKindTuple[OKS, e.R]
    ): SelectQuery[T, c.R, L] =
        filterIf(test)(f)

    /**
     * Enables `for` comprehension syntax. Equivalent to `filter`.
     * Used automatically by Scala's desugaring of `for` with `if`.
     *
     * {{{
     * for u <- from(User) if u.id > 1 yield u.name
     * }}}
     */
    def withFilter[F](f: QueryContext[L] ?=> T => F)(using
        a: AsExpr[F, L],
        b: SqlBoolean[a.R],
        kt: KindToTuple[a.K],
        i: CanInFilter[kt.R],
        e: ExcludeCurrentLevelColumn[kt.R, L],
        c: CombineKindTuple[OKS, e.R]
    ): SelectQuery[T, c.R, L] =
        filter(f)

    /**
     * Adds an `ORDER BY` clause. Sort expressions default to `ASC` unless
     * `.desc` or other ordering is specified. Multiple calls accumulate.
     *
     * {{{
     * from(User).sortBy(u => (u.name, u.id.desc))
     * }}}
     */
    def sortBy[S](f: QueryContext[L] ?=> T => S)(using
        a: AsSort[S, ManyRows, L],
        e: ExcludeCurrentLevelColumn[a.KS, L],
        c: CombineKindTuple[OKS, e.R]
    ): SortedSelectQuery[T, c.R, L] =
        val sort = a.asSorts(f(params))
        SortedSelectQuery(
            params,
            tree.copy(orderBy = tree.orderBy ++ sort.map(_.asSqlOrderingItem))
        )

    /**
     * Alias of `sortBy`, provided for users familiar with `ORDER BY`.
     *
     * {{{
     * from(User).orderBy(u => (u.name, u.id.desc))
     * }}}
     */
    def orderBy[S](f: QueryContext[L] ?=> T => S)(using
        a: AsSort[S, ManyRows, L],
        e: ExcludeCurrentLevelColumn[a.KS, L],
        c: CombineKindTuple[OKS, e.R]
    ): SortedSelectQuery[T, c.R, L] =
        sortBy(f)

    /**
     * Specifies the projection list, maps to `SELECT`. Accepts
     * expressions, tuples, and named tuples. The return type is
     * automatically derived, including nullability from outer joins.
     *
     * {{{
     * // Single expression
     * from(User).map(u => u.name)
     *
     * // Tuple
     * from(User).map(u => (u.id, u.name))
     *
     * // Named tuple
     * from(User).map(u => (name = u.name, count = count()))
     * }}}
     */
    def map[M](f: QueryContext[L] ?=> T => M)(using
        a: AsMap[M, L],
        i: CanInMap[a.KS],
        e: ExcludeCurrentLevelColumn[a.KS, L],
        c: CombineKindTuple[OKS, e.R]
    ): MappedSelectQuery[a.R, T, c.R, L, i.R] =
        val mapped = f(params)
        val sqlSelect = a.asSelectItems(mapped, 1)
        MappedSelectQuery(
            a.transform(mapped),
            params,
            tree.copy(select = sqlSelect)
        )

    /**
     * Alias of `map`, provided for users familiar with `SELECT`.
     *
     * {{{
     * from(User).select(u => (name = u.name))
     * }}}
     */
    def select[M](f: QueryContext[L] ?=> T => M)(using
        a: AsMap[M, L],
        i: CanInMap[a.KS],
        e: ExcludeCurrentLevelColumn[a.KS, L],
        c: CombineKindTuple[OKS, e.R]
    ): MappedSelectQuery[a.R, T, c.R, L, i.R] =
        map(f)

    /**
     * Adds `DISTINCT` to the projection. Maps to `SELECT DISTINCT`.
     * After `mapDistinct`, the `sortBy` lambda parameter type represents
     * the projected fields rather than the underlying table.
     *
     * {{{
     * from(User).mapDistinct(u => u.name)
     * }}}
     */
    def mapDistinct[M](f: QueryContext[L] ?=> T => M)(using
        a: AsMap[M, L],
        i: CanInMap[a.KS],
        e: ExcludeCurrentLevelColumn[a.KS, L],
        c: CombineKindTuple[OKS, e.R],
        td: ToDistinct[M, L]
    ): MappedDistinctSelectQuery[a.R, td.R, c.R, L, i.R] =
        val mapped = f(params)
        val sqlSelect = a.asSelectItems(mapped, 1)
        MappedDistinctSelectQuery(
            a.transform(mapped),
            td.toDistinct(mapped),
            tree.copy(quantifier = Some(SqlQuantifier.Distinct), select = sqlSelect)
        )

    /**
     * Alias of `mapDistinct`, provided for users familiar with `SELECT`.
     *
     * {{{
     * from(User).selectDistinct(u => u.name)
     * }}}
     */
    def selectDistinct[M](f: QueryContext[L] ?=> T => M)(using
        a: AsMap[M, L],
        i: CanInMap[a.KS],
        e: ExcludeCurrentLevelColumn[a.KS, L],
        c: CombineKindTuple[OKS, e.R],
        td: ToDistinct[M, L]
    ): MappedDistinctSelectQuery[a.R, td.R, c.R, L, i.R] =
        mapDistinct(f)

    /**
     * Adds a `GROUP BY` clause. All non-grouped columns are marked as
     * `UngroupedColumn` and rejected from subsequent `filter` or `map`
     * unless wrapped in aggregate functions.
     *
     * Supports grouping by expressions, tuples, and named tuples.
     *
     * {{{
     * // Single expression
     * from(Post).groupBy(p => p.channelId).map((g, p) => (g, count()))
     *
     * // Tuple
     * from(Post).groupBy(p => (p.channelId, p.title)).map((g, p) => (g._1, g._2, count()))
     *
     * // Named tuple
     * from(Post).groupBy(p => (cid = p.channelId, title = p.title))
     *     .map((g, p) => (g.cid, g.title, count()))
     * }}}
     */
    def groupBy[G](f: QueryContext[L] ?=> T => G)(using
        a: AsGroup[G, L],
        i: CanInGroup[a.KS],
        e: ExcludeCurrentLevelColumn[a.KS, L],
        c: CombineKindTuple[OKS, e.R],
        tu: TransformTableKind[T, UngroupedColumn],
        tt: ToTuple[tu.R]
    ): Grouping[a.R *: tt.R, c.R, L] =
        val group = f(params)
        val groupExprs = a.asExprs(group)
        Grouping(
            a.asGroup(group) *: tt.toTuple(tu.transform(params)),
            tree.copy(
                groupBy = Some(
                    SqlGroupBy(None, groupExprs.map(g => SqlGroupingItem.Expr(g.asSqlExpr)))
                )
            )
        )

    /**
     * Adds a `GROUP BY CUBE` clause, generating all possible grouping
     * combinations (2^n sets). All non-grouped columns and the grouping
     * expressions themselves are wrapped with `Option` since `CUBE`
     * introduces nulls for absent dimension values.
     *
     * {{{
     * from(Person).groupByCube(p => (age = p.age, nation = p.nation))
     *     .map((g, p) => (g.age, g.nation, count()))
     * }}}
     */
    def groupByCube[G](f: QueryContext[L] ?=> T => G)(using
        a: AsGroup[G, L],
        i: CanInGroup[a.KS],
        e: ExcludeCurrentLevelColumn[a.KS, L],
        c: CombineKindTuple[OKS, e.R],
        to: ToOption[a.R],
        tot: ToOption[T],
        tu: TransformTableKind[tot.R, UngroupedColumn],
        tt: ToTuple[tu.R]
    ): Grouping[to.R *: tt.R, c.R, L] =
        val group = f(params)
        val groupExprs = a.asExprs(group)
        Grouping(
            to.toOption(a.asGroup(group)) *: tt.toTuple(tu.transform(tot.toOption(params))),
            tree.copy(
                groupBy = Some(
                    SqlGroupBy(None, SqlGroupingItem.Cube(groupExprs.map(_.asSqlExpr)) :: Nil)
                )
            )
        )

    /**
     * Adds a `GROUP BY ROLLUP` clause, generating hierarchical groupings
     * (n + 1 sets). Like `CUBE`, wraps non-grouped columns and grouping
     * expressions with `Option`.
     *
     * {{{
     * from(Person).groupByRollup(p => (age = p.age, nation = p.nation))
     *     .map((g, p) => (g.age, g.nation, count()))
     * }}}
     */
    def groupByRollup[G](f: QueryContext[L] ?=> T => G)(using
        a: AsGroup[G, L],
        i: CanInGroup[a.KS],
        e: ExcludeCurrentLevelColumn[a.KS, L],
        c: CombineKindTuple[OKS, e.R],
        to: ToOption[a.R],
        tot: ToOption[T],
        tu: TransformTableKind[tot.R, UngroupedColumn],
        tt: ToTuple[tu.R]
    ): Grouping[to.R *: tt.R, c.R, L] =
        val group = f(params)
        val groupExprs = a.asExprs(group)
        Grouping(
            to.toOption(a.asGroup(group)) *: tt.toTuple(tu.transform(tot.toOption(params))),
            tree.copy(
                groupBy = Some(
                    SqlGroupBy(None, SqlGroupingItem.Rollup(groupExprs.map(_.asSqlExpr)) :: Nil)
                )
            )
        )

    /**
     * Adds a `GROUP BY GROUPING SETS` clause, allowing arbitrary grouping
     * combinations. The grouping function specifies the base grouping
     * expression, and the second function builds a nested tuple of grouping
     * sets. Pass `()` for the empty grouping set.
     *
     * {{{
     * from(Entity).groupBySets(e => (a = e.a, b = e.b))(
     *     g => ((), g.a, (g.a, g.b))
     * ).map((g, e) => (g.a, g.b, count()))
     * }}}
     */
    def groupBySets[G, S](f: QueryContext[L] ?=> T => G)(using
        a: AsGroup[G, L],
        i: CanInGroup[a.KS],
        e: ExcludeCurrentLevelColumn[a.KS, L],
        c: CombineKindTuple[OKS, e.R],
        to: ToOption[a.R],
        tot: ToOption[T],
        tu: TransformTableKind[tot.R, UngroupedColumn],
        tt: ToTuple[tu.R]
    )(g: a.R => S)(using
        s: AsGroupingSets[S]
    ): Grouping[to.R *: tt.R, c.R, L] =
        val group = a.asGroup(f(params))
        val groupExprs = s.asSqlExprs(g(group))
        Grouping(
            to.toOption(group) *: tt.toTuple(tu.transform(tot.toOption(params))),
            tree.copy(
                groupBy = Some(
                    SqlGroupBy(None, SqlGroupingItem.GroupingSets(groupExprs) :: Nil)
                )
            )
        )

object SelectQuery:
    /**
     * Creates a hierarchical recursive query using `WITH RECURSIVE`
     * under the hood. The function receives the current and prior
     * table references for the recursive join condition.
     *
     * {{{
     * from(Comment).filter(c => c.postId == postId)
     *     .connectBy((c, prior) => c.parentId == prior.id)
     *     .startWith(c => c.parentId.isNull)
     *     .map(c => (id = c.id, content = c.content, level = level()))
     * }}}
     */
    extension [T, OKS <: Tuple, L <: Int](query: SelectQuery[Table[T, Column, L], OKS, L])
        def connectBy[F](f: QueryContext[L] ?=> (Table[T, Column, L], Table[T, Column, L]) => F)(using
            a: AsExpr[F, L],
            b: SqlBoolean[a.R],
            kt: KindToTuple[a.K],
            i: CanInFilter[kt.R],
            e: ExcludeCurrentLevelColumn[kt.R, L],
            c: CombineKindTuple[OKS, e.R]
        ): ConnectBy[T, c.R, L] =
            given QueryContext[L] = query.qc
            val cond = a.asExpr(
                f(query.params, query.params.copy(__aliasName__ = Some(tableCte)))
            ).asSqlExpr
            val joinTree = query.tree
                .copy(
                    select = query.tree.select :+ SqlSelectItem.Expr(
                        SqlExpr.Binary(
                            SqlExpr.Column(Some(tableCte), columnPseudoLevel),
                            SqlBinaryOperator.Plus,
                            SqlExpr.NumberLiteral(1)
                        ),
                        Some(columnPseudoLevel)
                    ),
                    from = SqlTable.Join(
                        query.tree.from.head,
                        SqlJoinType.Inner,
                        SqlTable.Ident(tableCte, None, None, None, None),
                        Some(SqlJoinCondition.On(cond))
                    ) :: Nil
                )
            val startTree = query.tree
                .copy(
                    select = query.tree.select :+ SqlSelectItem.Expr(
                        SqlExpr.NumberLiteral(1),
                        Some(columnPseudoLevel)
                    )
                )
            ConnectBy(query.params, joinTree, startTree)

/**
 * A sorted `SELECT` query.
 */
final case class SortedSelectQuery[T, OKS <: Tuple, L <: Int](
    override private[sqala] val params: T,
    override private[sqala] val tree: SqlQuery.Select
)(using
    override private[sqala] val qc: QueryContext[L]
) extends SortedQuery[T, OKS, L, ManyRows](params, tree):
    /**
     * Adds an `ORDER BY` clause. Sort expressions default to `ASC` unless
     * `.desc` or other ordering is specified. Multiple calls accumulate.
     *
     * {{{
     * from(User).sortBy(u => u.name).sortBy(u => u.id.desc)
     * }}}
     */
    def sortBy[S](f: QueryContext[L] ?=> T => S)(using
        a: AsSort[S, ManyRows, L],
        e: ExcludeCurrentLevelColumn[a.KS, L],
        c: CombineKindTuple[OKS, e.R]
    ): SortedSelectQuery[T, c.R, L] =
        val sort = a.asSorts(f(params))
        SortedSelectQuery(
            params,
            tree.copy(orderBy = tree.orderBy ++ sort.map(_.asSqlOrderingItem))
        )

    /**
     * Alias of `sortBy`, provided for users familiar with `ORDER BY`.
     *
     * {{{
     * from(User).orderBy(u => u.name).orderBy(u => u.id.desc)
     * }}}
     */
    def orderBy[S](f: QueryContext[L] ?=> T => S)(using
        a: AsSort[S, ManyRows, L],
        e: ExcludeCurrentLevelColumn[a.KS, L],
        c: CombineKindTuple[OKS, e.R]
    ): SortedSelectQuery[T, c.R, L] =
        sortBy(f)

/**
 * A `SELECT` query after `map` projection.
 */
final case class MappedSelectQuery[M, T, OKS <: Tuple, L <: Int, S <: QuerySize](
    override private[sqala] val params: M,
    private[sqala] val tables: T,
    override private[sqala] val tree: SqlQuery.Select
)(using
    override private[sqala] val qc: QueryContext[L]
) extends Query[M, OKS, L, S](params, tree):
    /**
     * Adds an `ORDER BY` clause. Sort expressions default to `ASC` unless
     * `.desc` or other ordering is specified. Multiple calls accumulate.
     *
     * {{{
     * from(User).map(u => u.name).sortBy(u => u.id.desc)
     * }}}
     */
    def sortBy[SS](f: QueryContext[L] ?=> T => SS)(using
        a: AsSort[SS, S, L],
        e: ExcludeCurrentLevelColumn[a.KS, L],
        c: CombineKindTuple[OKS, e.R]
    ): MappedSortedSelectQuery[M, T, c.R, L, S] =
        val sort = a.asSorts(f(tables))
        MappedSortedSelectQuery(
            params,
            tables,
            tree.copy(orderBy = tree.orderBy ++ sort.map(_.asSqlOrderingItem))
        )

    /**
     * Alias of `sortBy`, provided for users familiar with `ORDER BY`.
     *
     * {{{
     * from(User).map(u => u.name).orderBy(u => u.id.desc)
     * }}}
     */
    def orderBy[SS](f: QueryContext[L] ?=> T => SS)(using
        a: AsSort[SS, S, L],
        e: ExcludeCurrentLevelColumn[a.KS, L],
        c: CombineKindTuple[OKS, e.R]
    ): MappedSortedSelectQuery[M, T, c.R, L, S] =
        sortBy(f)

/**
 * A `SELECT` query after `map` and `sortBy`.
 */
final case class MappedSortedSelectQuery[M, T, OKS <: Tuple, L <: Int, S <: QuerySize](
    override private[sqala] val params: M,
    private[sqala] val tables: T,
    override private[sqala] val tree: SqlQuery.Select
)(using
    override private[sqala] val qc: QueryContext[L]
) extends SortedQuery[M, OKS, L, S](params, tree):
    /**
     * Adds an `ORDER BY` clause. Sort expressions default to `ASC` unless
     * `.desc` or other ordering is specified. Multiple calls accumulate.
     *
     * {{{
     * from(User).map(u => u.name).sortBy(u => u.id).sortBy(u => u.id.desc)
     * }}}
     */
    def sortBy[SS](f: QueryContext[L] ?=> T => SS)(using
        a: AsSort[SS, S, L],
        e: ExcludeCurrentLevelColumn[a.KS, L],
        c: CombineKindTuple[OKS, e.R]
    ): MappedSortedSelectQuery[M, T, c.R, L, S] =
        val sort = a.asSorts(f(tables))
        MappedSortedSelectQuery(
            params,
            tables,
            tree.copy(orderBy = tree.orderBy ++ sort.map(_.asSqlOrderingItem))
        )

    /**
     * Alias of `sortBy`, provided for users familiar with `ORDER BY`.
     *
     * {{{
     * from(User).map(u => u.name).orderBy(u => u.id).orderBy(u => u.id.desc)
     * }}}
     */
    def orderBy[SS](f: QueryContext[L] ?=> T => SS)(using
        a: AsSort[SS, S, L],
        e: ExcludeCurrentLevelColumn[a.KS, L],
        c: CombineKindTuple[OKS, e.R]
    ): MappedSortedSelectQuery[M, T, c.R, L, S] =
        sortBy(f)

/**
 * A `SELECT DISTINCT` query after `mapDistinct` projection.
 */
final case class MappedDistinctSelectQuery[M, D, OKS <: Tuple, L <: Int, S <: QuerySize](
    override private[sqala] val params: M,
    private[sqala] val distinctExprs: D,
    override private[sqala] val tree: SqlQuery.Select
)(using
    override private[sqala] val qc: QueryContext[L]
) extends Query[M, OKS, L, S](params, tree):
    /**
     * Sorts by distinct-projected fields. The lambda parameter type represents the projected type,
     * ensuring only columns in the `DISTINCT` list appear in `ORDER BY`.
     *
     * {{{
     * from(User).mapDistinct(u => (name = u.name)).sortBy(u => u.name.desc)
     * }}}
     */
    def sortBy[SS](f: D => SS)(using
        a: AsDistinctSort[SS, L]
    ): MappedSortedDistinctSelectQuery[M, D, OKS, L, S] =
        val sort = a.asSorts(f(distinctExprs))
        MappedSortedDistinctSelectQuery(
            params,
            distinctExprs,
            tree.copy(orderBy = tree.orderBy ++ sort.map(_.asSqlOrderBy))
        )

    /**
     * Alias of `sortBy`, provided for users familiar with `ORDER BY`.
     *
     * {{{
     * from(User).mapDistinct(u => (name = u.name)).orderBy(u => u.name.desc)
     * }}}
     */
    def orderBy[SS](f: D => SS)(using
        a: AsDistinctSort[SS, L]
    ): MappedSortedDistinctSelectQuery[M, D, OKS, L, S] =
        sortBy(f)

/**
 * A sorted `SELECT DISTINCT` query after `mapDistinct` and `sortBy`.
 */
final case class MappedSortedDistinctSelectQuery[M, D, OKS <: Tuple, L <: Int, S <: QuerySize](
    override private[sqala] val params: M,
    private[sqala] val distinctExprs: D,
    override private[sqala] val tree: SqlQuery.Select
)(using
    override private[sqala] val qc: QueryContext[L]
) extends SortedQuery[M, OKS, L, S](params, tree):
    /**
     * Sorts by distinct-projected fields. The lambda parameter type represents the projected type,
     * ensuring only columns in the `DISTINCT` list appear in `ORDER BY`.
     *
     * {{{
     * from(User).mapDistinct(u => (id = u.id, name = u.name)).sortBy(u => u.id).sortBy(u => u.name.desc)
     * }}}
     */
    def sortBy[SS](f: D => SS)(using
        a: AsDistinctSort[SS, L]
    ): MappedSortedDistinctSelectQuery[M, D, OKS, L, S] =
        val sort = a.asSorts(f(distinctExprs))
        MappedSortedDistinctSelectQuery(
            params,
            distinctExprs,
            tree.copy(orderBy = tree.orderBy ++ sort.map(_.asSqlOrderBy))
        )

    /**
     * Alias of `sortBy`, provided for users familiar with `ORDER BY`.
     *
     * {{{
     * from(User).mapDistinct(u => (id = u.id, name = u.name)).orderBy(u => u.id).orderBy(u => u.name.desc)
     * }}}
     */
    def orderBy[SS](f: D => SS)(using
        a: AsDistinctSort[SS, L]
    ): MappedSortedDistinctSelectQuery[M, D, OKS, L, S] =
        sortBy(f)

/**
 * A `SELECT` query after `groupBy`.
 */
final case class Grouping[T, OKS <: Tuple, L <: Int](
    private[sqala] val params: T,
    private[sqala] val tree: SqlQuery.Select
)(using
    private[sqala] val qc: QueryContext[L]
):
    given GroupingContext = GroupingContext()

    /**
     * Adds a `HAVING` clause, filtering groups after aggregation.
     * Only aggregate functions and grouped column references are
     * allowed; window functions and ungrouped column references are
     * rejected at compile time.
     *
     * {{{
     * from(Post).groupBy(p => p.channelId)
     *     .having((g, p) => count() > 1)
     *     .map((g, p) => (g, count()))
     * }}}
     */
    def having[F](f: QueryContext[L] ?=> GroupingContext ?=> T => F)(using
        a: AsExpr[F, L],
        b: SqlBoolean[a.R],
        kt: KindToTuple[a.K],
        i: CanInHaving[kt.R],
        e: ExcludeCurrentLevelColumn[kt.R, L],
        c: CombineKindTuple[OKS, e.R]
    ): Grouping[T, c.R, L] =
        val cond = a.asExpr(f(params))
        Grouping(params, tree.addHaving(cond.asSqlExpr))

    /**
     * Specifies the grouped projection list, maps to `SELECT` after
     * `groupBy`. Only grouped expressions and aggregate functions
     * are allowed; raw columns and window functions are rejected at
     * compile time.
     *
     * Supports expressions, tuples, and named tuples.
     *
     * {{{
     * // Single expression
     * from(Post).groupBy(p => p.channelId)
     *     .map((g, p) => count())
     *
     * // Tuple
     * from(Post).groupBy(p => p.channelId)
     *     .map((g, p) => (g, count()))
     *
     * // Named tuple
     * from(Post).groupBy(p => p.channelId)
     *     .map((g, p) => (channelId = g, count = count()))
     * }}}
     */
    def map[M](f: QueryContext[L] ?=> GroupingContext ?=> T => M)(using
        a: AsMap[M, L],
        i: CanInMap[a.KS],
        e: ExcludeCurrentLevelColumn[a.KS, L],
        c: CombineKindTuple[OKS, e.R]
    ): GroupedSelectQuery[a.R, T, c.R, L] =
        val mapped = f(params)
        val sqlSelect = a.asSelectItems(mapped, 1)
        GroupedSelectQuery(
            a.transform(mapped),
            params,
            tree.copy(select = sqlSelect)
        )

    /**
     * Alias of `map`, provided for users familiar with `SELECT`.
     *
     * {{{
     * from(Post).groupBy(p => p.channelId)
     *     .select((g, p) => (g, count()))
     * }}}
     */
    def select[M](f: QueryContext[L] ?=> GroupingContext ?=> T => M)(using
        a: AsMap[M, L],
        i: CanInMap[a.KS],
        e: ExcludeCurrentLevelColumn[a.KS, L],
        c: CombineKindTuple[OKS, e.R]
    ): GroupedSelectQuery[a.R, T, c.R, L] =
        map(f)

/**
 * A grouped `SELECT` query after `map` projection.
 */
final case class GroupedSelectQuery[M, T, OKS <: Tuple, L <: Int](
    override private[sqala] val params: M,
    private[sqala] val tables: T,
    override private[sqala] val tree: SqlQuery.Select
)(using
    override private[sqala] val qc: QueryContext[L]
) extends Query[M, OKS, L, ManyRows](params, tree):
    given GroupingContext = GroupingContext()

    /**
     * Sorts the grouped result. ensuring only grouped expressions
     * and aggregate functions appear in `ORDER BY`.
     *
     * {{{
     * from(Post).groupBy(p => p.channelId)
     *     .map((g, p) => (g, count()))
     *     .sortBy((g, p) => g.desc)
     * }}}
     */
    def sortBy[S](f: QueryContext[L] ?=> GroupingContext ?=> T => S)(using
        a: AsGroupedSort[S, L],
        e: ExcludeCurrentLevelColumn[a.KS, L],
        c: CombineKindTuple[OKS, e.R]
    ): GroupedSortedSelectQuery[M, T, c.R, L] =
        val sort = a.asSorts(f(tables))
        GroupedSortedSelectQuery(
            params,
            tables,
            tree.copy(orderBy = tree.orderBy ++ sort.map(_.asSqlOrderingItem))
        )

    /**
     * Alias of `sortBy`, provided for users familiar with `ORDER BY`.
     *
     * {{{
     * from(Post).groupBy(p => p.channelId)
     *     .map((g, p) => (g, count()))
     *     .orderBy((g, p) => g.desc)
     * }}}
     */
    def orderBy[S](f: QueryContext[L] ?=> GroupingContext ?=> T => S)(using
        a: AsGroupedSort[S, L],
        e: ExcludeCurrentLevelColumn[a.KS, L],
        c: CombineKindTuple[OKS, e.R]
    ): GroupedSortedSelectQuery[M, T, c.R, L] =
        sortBy(f)

/**
 * A grouped and sorted `SELECT` query.
 */
final case class GroupedSortedSelectQuery[M, T, OKS <: Tuple, L <: Int](
    override private[sqala] val params: M,
    private[sqala] val tables: T,
    override private[sqala] val tree: SqlQuery.Select
)(using
    override private[sqala] val qc: QueryContext[L]
) extends SortedQuery[M, OKS, L, ManyRows](params, tree):
    given GroupingContext = GroupingContext()

    /**
     * Sorts the grouped result. ensuring only grouped expressions
     * and aggregate functions appear in `ORDER BY`.
     *
     * {{{
     * from(Post).groupBy(p => p.channelId)
     *     .map((g, p) => (g, count()))
     *     .sortBy((g, p) => g.desc)
     *     .sortBy((g, p) => count().asc)
     * }}}
     */
    def sortBy[S](f: QueryContext[L] ?=> GroupingContext ?=> T => S)(using
        a: AsGroupedSort[S, L],
        e: ExcludeCurrentLevelColumn[a.KS, L],
        c: CombineKindTuple[OKS, e.R]
    ): GroupedSortedSelectQuery[M, T, c.R, L] =
        val sort = a.asSorts(f(tables))
        GroupedSortedSelectQuery(
            params,
            tables,
            tree.copy(orderBy = tree.orderBy ++ sort.map(_.asSqlOrderingItem))
        )

    /**
     * Alias of `sortBy`, provided for users familiar with `ORDER BY`.
     *
     * {{{
     * from(Post).groupBy(p => p.channelId)
     *     .map((g, p) => (g, count()))
     *     .orderBy((g, p) => g.desc)
     *     .orderBy((g, p) => count().asc)
     * }}}
     */
    def orderBy[S](f: QueryContext[L] ?=> GroupingContext ?=> T => S)(using
        a: AsGroupedSort[S, L],
        e: ExcludeCurrentLevelColumn[a.KS, L],
        c: CombineKindTuple[OKS, e.R]
    ): GroupedSortedSelectQuery[M, T, c.R, L] =
        sortBy(f)

/**
 * A query produced by set operations (`union`, `except`, `intersect`).
 */
final case class UnionQuery[T, OKS <: Tuple, L <: Int](
    override private[sqala] val params: T,
    override private[sqala] val tree: SqlQuery.Set
)(using
    override private[sqala] val qc: QueryContext[L]
) extends Query[T, OKS, L, ManyRows](params, tree):
    /**
     * Sorts the union result using column-level sort expressions.
     *
     * {{{
     * q1.unionAll(q2).sortBy(t => t.x.asc)
     * }}}
     */
    def sortBy[S](f: T => S)(using s: AsColumnSort[S, L]): SortedUnionQuery[T, OKS, L] =
        val sort = s.asSorts(f(params))
        SortedUnionQuery(
            params,
            tree.copy(orderBy = tree.orderBy ++ sort.map(_.asSqlOrderingItem))
        )

    /**
     * Alias of `sortBy`, provided for users familiar with `ORDER BY`.
     *
     * {{{
     * q1.unionAll(q2).orderBy(t => t.x.asc)
     * }}}
     */
    def orderBy[S](f: T => S)(using s: AsColumnSort[S, L]): SortedUnionQuery[T, OKS, L] =
        sortBy(f)

/**
 * A sorted union query.
 */
final case class SortedUnionQuery[T, OKS <: Tuple, L <: Int](
    override private[sqala] val params: T,
    override private[sqala] val tree: SqlQuery.Set
)(using
    override private[sqala] val qc: QueryContext[L]
) extends SortedQuery[T, OKS, L, ManyRows](params, tree):
    /**
     * Sorts the union result using column-level sort expressions.
     *
     * {{{
     * q1.unionAll(q2).sortBy(t => t.x.asc).sortBy(t => t.y.desc)
     * }}}
     */
    def sortBy[S](f: T => S)(using s: AsColumnSort[S, L]): SortedUnionQuery[T, OKS, L] =
        val sort = s.asSorts(f(params))
        SortedUnionQuery(
            params,
            tree.copy(orderBy = tree.orderBy ++ sort.map(_.asSqlOrderingItem))
        )

    /**
     * Alias of `sortBy`, provided for users familiar with `ORDER BY`.
     *
     * {{{
     * q1.unionAll(q2).orderBy(t => t.x.asc).orderBy(t => t.y.desc)
     * }}}
     */
    def orderBy[S](f: T => S)(using s: AsColumnSort[S, L]): SortedUnionQuery[T, OKS, L] =
        sortBy(f)

/**
 * A hierarchical recursive query created by `connectBy`. Converts to a
 * standard `WITH RECURSIVE` CTE at SQL generation time.
 */
final case class ConnectBy[T, OKS <: Tuple, L <: Int](
    private[sqala] val table: Table[T, Column, L],
    private[sqala] val connectByTree: SqlQuery.Select,
    private[sqala] val startWithTree: SqlQuery.Select,
    private[sqala] val mapTree: SqlQuery.Select =
        SqlQuery.Select(
            None,
            Nil,
            SqlTable.Ident(tableCte, None, None, None, None) :: Nil,
            None,
            None,
            None,
            Nil,
            None,
            None
        )
)(using private[sqala] val qc: QueryContext[L]):
    given ConnectByContext = ConnectByContext()

    /**
     * Specifies the root-level condition for starting recursion.
     * Maps to the base `WHERE` clause of the CTE.
     *
     * {{{
     * from(Comment).connectBy((c, prior) => c.parentId == prior.id)
     *     .startWith(c => c.parentId.isNull)
     * }}}
     */
    def startWith[F](f: QueryContext[L] ?=> ConnectByContext ?=> Table[T, Column, L] => F)(using
        a: AsExpr[F, L],
        b: SqlBoolean[a.R],
        kt: KindToTuple[a.K],
        i: CanInFilter[kt.R],
        e: ExcludeCurrentLevelColumn[kt.R, L],
        c: CombineKindTuple[OKS, e.R]
    ): ConnectBy[T, c.R, L] =
        val cond = a.asExpr(f(table))
        copy(
            startWithTree = startWithTree.addWhere(cond.asSqlExpr)
        )

    /**
     * Limits the recursion depth. Maps to a filter on the pseudo
     * `LEVEL` column.
     *
     * {{{
     * from(Comment).connectBy(...).startWith(...).maxDepth(5)
     * }}}
     */
    def maxDepth(n: Int): ConnectBy[T, OKS, L] =
        val cond = SqlExpr.Binary(
            SqlExpr.Column(Some(tableCte), columnPseudoLevel),
            SqlBinaryOperator.LessThan,
            SqlExpr.NumberLiteral(n)
        )
        copy(
            connectByTree = connectByTree.addWhere(cond)
        )

    /**
     * Sorts siblings within each level of the recursion.
     *
     * {{{
     * from(Comment).connectBy(...).startWith(...).sortSiblingsBy(c => c.content)
     * }}}
     */
    def sortSiblingsBy[S](f: QueryContext[L] ?=> ConnectByContext ?=> Table[T, Column, L] => S)(using
        a: AsSort[S, ManyRows, L],
        e: ExcludeCurrentLevelColumn[a.KS, L],
        c: CombineKindTuple[OKS, e.R]
    ): ConnectBy[T, c.R, L] =
        val sort = f(table)
        val sqlOrderBy = a.asSorts(sort).map(_.asSqlOrderingItem)
        copy(
            connectByTree = connectByTree.copy(orderBy = connectByTree.orderBy ++ sqlOrderBy)
        )

    /**
     * Alias of `sortSiblingsBy`, provided for users familiar with `ORDER BY`.
     */
    def orderSiblingsBy[S](f: QueryContext[L] ?=> ConnectByContext ?=> Table[T, Column, L] => S)(using
        a: AsSort[S, ManyRows, L],
        e: ExcludeCurrentLevelColumn[a.KS, L],
        c: CombineKindTuple[OKS, e.R]
    ): ConnectBy[T, c.R, L] =
        sortSiblingsBy(f)

    /**
     * Projects the final result from the recursive CTE. Maps to
     * `SELECT` on the CTE table, generating the complete `WITH RECURSIVE`
     * query.
     *
     * {{{
     * from(Comment).connectBy((c, prior) => c.parentId == prior.id)
     *     .startWith(c => c.parentId.isNull)
     *     .map(c => (id = c.id, content = c.content, level = level()))
     * }}}
     */
    def map[M](f: ConnectByContext ?=> Table[T, Column, L] => M)(using
        a: AsMap[M, L],
        i: CanInMap[a.KS],
        e: ExcludeCurrentLevelColumn[a.KS, L],
        c: CombineKindTuple[OKS, e.R]
    ): Query[a.R, c.R, L, ManyRows] =
        val mapped = f(
            Table[T, Column, L](
                Some(tableCte),
                table.__metaData__,
                table.__sqlTable__.copy(
                    alias = table.__sqlTable__.alias.map(_.copy(alias = tableCte))
                )
            )
        )
        val sqlSelect = a.asSelectItems(mapped, 1)
        val metaData = table.__metaData__
        val unionQuery = SqlQuery.Set(
            startWithTree,
            SqlSetOperator.Union(Some(SqlQuantifier.All)),
            connectByTree,
            Nil,
            None,
            None
        )
        val withItem = SqlWithItem(
            tableCte, metaData.columnNames :+ columnPseudoLevel, unionQuery
        )
        val cteTree: SqlQuery.Cte = SqlQuery.Cte(
            true,
            withItem :: Nil,
            mapTree.copy(select = sqlSelect),
            None
        )
        Query(a.transform(mapped), cteTree)

    /**
     * Alias of `map`, provided for users familiar with `SELECT`.
     *
     * {{{
     * from(Comment).connectBy(...).startWith(...).select(c => (id = c.id, level = level()))
     * }}}
     */
    def select[M](f: ConnectByContext ?=> Table[T, Column, L] => M)(using
        a: AsMap[M, L],
        i: CanInMap[a.KS],
        e: ExcludeCurrentLevelColumn[a.KS, L],
        c: CombineKindTuple[OKS, e.R]
    ): Query[a.R, c.R, L, ManyRows] =
        map(f)