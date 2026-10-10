package sqala.static.dsl.statement.query

import sqala.ast.expr.{SqlBinaryOperator, SqlExpr}
import sqala.ast.group.{SqlGroup, SqlGroupingItem}
import sqala.ast.limit.{SqlFetch, SqlFetchMode, SqlFetchUnit, SqlLimit}
import sqala.ast.quantifier.SqlQuantifier
import sqala.ast.statement.*
import sqala.ast.table.{SqlJoinCondition, SqlJoinType, SqlTable, SqlTableAlias}
import sqala.metadata.{Dialect, SqlBoolean}
import sqala.static.dsl.*
import sqala.util.queryToString
import sqala.util.NonEmptyList

import scala.compiletime.ops.int.-
import sqala.ir.IrQuery
import sqala.ir.ToAst

/**
 * A typed SQL query, the base type for all query operations in the DSL.
 */
sealed class Query[T, OKS <: Tuple, L <: Int, S <: QuerySize](
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
        val toAst = ToAst()
        queryToString(toAst.toAst(tree), dialect, standardEscapeStrings)

/**
 * A `SELECT` query.
 */
sealed class SelectQuery[M, T, OKS <: Tuple, L <: Int, S <: QuerySize](
    override private[sqala] val params: M,
    private[sqala] val tables: T,
    override private[sqala] val tree: IrQuery.Select
)(using
    override private[sqala] val qc: QueryContext[L]
) extends Query[M, OKS, L, S](params, tree):
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
        kt: KindToTuple[a.K],
        ci: CanInFilter[a.R, kt.R],
        e: ExcludeCurrentLevelColumn[kt.R, L],
        c: CombineKindTuple[OKS, e.R]
    ): SelectQuery[M, T, c.R, L, S] =
        val cond = a.asExpr(f(tables))
        SelectQuery(params, tables, tree.addWhere(cond.asIrExpr))

    /**
     * Alias of `filter`, provided for users familiar with `WHERE`.
     *
     * {{{
     * from(User).where(u => u.id > 1)
     * }}}
     */
    def where[F](f: QueryContext[L] ?=> T => F)(using
        a: AsExpr[F, L],
        kt: KindToTuple[a.K],
        ci: CanInFilter[a.R, kt.R],
        e: ExcludeCurrentLevelColumn[kt.R, L],
        c: CombineKindTuple[OKS, e.R]
    ): SelectQuery[M, T, c.R, L, S] =
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
        kt: KindToTuple[a.K],
        ci: CanInFilter[a.R, kt.R],
        e: ExcludeCurrentLevelColumn[kt.R, L],
        c: CombineKindTuple[OKS, e.R]
    ): SelectQuery[M, T, c.R, L, S] =
        if test then filter(f) else SelectQuery(params, tables, tree)

    /**
     * Alias of `filterIf`, provided for users familiar with `WHERE`.
     *
     * {{{
     * from(User).whereIf(name.isDefined)(u => u.name == name)
     * }}}
     */
    def whereIf[F](test: => Boolean)(f: QueryContext[L] ?=> T => F)(using
        a: AsExpr[F, L],
        kt: KindToTuple[a.K],
        ci: CanInFilter[a.R, kt.R],
        e: ExcludeCurrentLevelColumn[kt.R, L],
        c: CombineKindTuple[OKS, e.R]
    ): SelectQuery[M, T, c.R, L, S] =
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
        kt: KindToTuple[a.K],
        ci: CanInFilter[a.R, kt.R],
        e: ExcludeCurrentLevelColumn[kt.R, L],
        c: CombineKindTuple[OKS, e.R]
    ): SelectQuery[M, T, c.R, L, S] =
        filter(f)

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
        c: CombineKindTuple[OKS, e.R],
        an: AggNested[a.KS],
        nan: an.R =:= false
    ): SelectQuery[a.R, T, c.R, L, i.R] =
        val mapped = f(tables)
        val sqlSelect = a.asSelectItems(mapped, 1)
        SelectQuery(
            a.transform(mapped),
            tables,
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
        c: CombineKindTuple[OKS, e.R],
        an: AggNested[a.KS],
        nan: an.R =:= false
    ): SelectQuery[a.R, T, c.R, L, i.R] =
        map(f)

    /**
     * Skips the first `n` rows of the result. Maps to `OFFSET`.
     *
     * {{{
     * from(User).drop(10).take(5)
     * }}}
     */
    def drop(n: Int): SelectQuery[M, T, OKS, L, S] =
        this
        // val sqlExpr = n.asExpr.asSqlExpr
        // val limit = tree.limit
        // val sqlLimit = limit
        //     .map(l => SqlLimit(Some(sqlExpr), l.fetch))
        //     .orElse(Some(SqlLimit(Some(sqlExpr), None)))
        // val newTree: SqlQuery.Select = tree.copy(limit = sqlLimit)
        // SelectQuery(params, newTree)

    /**
     * Alias of `drop`, provided for users familiar with `OFFSET`.
     *
     * {{{
     * from(User).offset(10).limit(5)
     * }}}
     */
    def offset(n: Int): SelectQuery[M, T, OKS, L, S] =
        drop(n)

    /**
     * Limits the result to at most `n` rows. Maps to `LIMIT` or `FETCH NEXT`.
     *
     * {{{
     * from(User).drop(10).take(5)
     * }}}
     */
    def take(n: Int)(using
        s: TakeSize[n.type]
    ): SelectQuery[M, T, OKS, L, s.R] =
        SelectQuery(params, tables, tree)
        // val sqlExpr = n.asExpr.asSqlExpr
        // val limit = tree.limit
        // val sqlLimit = limit
        //     .map(l => SqlLimit(l.offset, Some(SqlFetch(sqlExpr, SqlFetchUnit.RowCount, SqlFetchMode.Only))))
        //     .orElse(Some(SqlLimit(None, Some(SqlFetch(sqlExpr, SqlFetchUnit.RowCount, SqlFetchMode.Only)))))
        // val newTree: SqlQuery.Select = tree.copy(limit = sqlLimit)
        // SelectQuery(params, newTree)

    /**
     * Alias of `take`, provided for users familiar with `LIMIT`.
     *
     * {{{
     * from(User).offset(10).limit(5)
     * }}}
     */
    def limit(n: Int)(using
        s: TakeSize[n.type]
    ): SelectQuery[M, T, OKS, L, s.R] =
        take(n)

    // /**
    //  * Locks selected rows for update. Maps to `FOR UPDATE`.
    //  *
    //  * {{{
    //  * from(User).forUpdate
    //  * }}}
    //  */
    // def forUpdate: Query[T, OKS, L, S] =
    //     val newTree: SqlQuery.Select = tree.copy(lock = Some(SqlLock.Update(None)))
    //     Query(params, newTree)

    // /**
    //  * Locks selected rows for update, failing immediately if rows are
    //  * already locked. Maps to `FOR UPDATE NOWAIT`.
    //  *
    //  * {{{
    //  * from(User).forUpdateNoWait
    //  * }}}
    //  */
    // def forUpdateNoWait: Query[T, OKS, L, S] =
    //     val newTree: SqlQuery.Select = tree.copy(lock = Some(SqlLock.Update(Some(SqlLockWaitMode.NoWait))))
    //     Query(params, newTree)

    // /**
    //  * Locks selected rows for update, skipping already locked rows.
    //  * Maps to `FOR UPDATE SKIP LOCKED`.
    //  *
    //  * {{{
    //  * from(User).forUpdateSkipLocked
    //  * }}}
    //  */
    // def forUpdateSkipLocked: Query[T, OKS, L, S] =
    //     val newTree: SqlQuery.Select = tree.copy(lock = Some(SqlLock.Update(Some(SqlLockWaitMode.SkipLocked))))
    //     Query(params, newTree)

    // /**
    //  * Locks selected rows in share mode, allowing other transactions to
    //  * read but not update. Maps to `FOR SHARE`.
    //  *
    //  * {{{
    //  * from(User).forShare
    //  * }}}
    //  */
    // def forShare: Query[T, OKS, L, S] =
    //     val newTree: SqlQuery.Select = tree.copy(lock = Some(SqlLock.Share(None)))
    //     Query(params, newTree)

    // /**
    //  * Locks selected rows in share mode, failing immediately if rows
    //  * are already locked. Maps to `FOR SHARE NOWAIT`.
    //  *
    //  * {{{
    //  * from(User).forShareNoWait
    //  * }}}
    //  */
    // def forShareNoWait: Query[T, OKS, L, S] =
    //     val newTree: SqlQuery.Select = tree.copy(lock = Some(SqlLock.Share(Some(SqlLockWaitMode.NoWait))))
    //     Query(params, newTree)

    // /**
    //  * Locks selected rows in share mode, skipping already locked rows.
    //  * Maps to `FOR SHARE SKIP LOCKED`.
    //  *
    //  * {{{
    //  * from(User).forShareSkipLocked
    //  * }}}
    //  */
    // def forShareSkipLocked: Query[T, OKS, L, S] =
    //     val newTree: SqlQuery.Select = tree.copy(lock = Some(SqlLock.Share(Some(SqlLockWaitMode.SkipLocked))))
    //     Query(params, newTree)