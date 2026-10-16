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

/**
 * A `SELECT` query.
 */
sealed class SelectQuery[T, OKS <: Tuple, L <: Int, S <: QuerySize](
    override private[sqala] val params: T,
    override private[sqala] val tree: IrQuery.Select
)(using
    override private[sqala] val qc: QueryContext[L]
) extends Query[T, OKS, L, S](params, tree):
    /**
     * Skips the first `n` rows of the result. Maps to `OFFSET`.
     *
     * {{{
     * from(User).drop(10).take(5)
     * }}}
     */
    def drop(n: Int): SelectQuery[T, OKS, L, S] =
        val expr = IrExpr.Native(SqlExpr.NumberLiteral(n))
        val limit = tree.limit
            .map(l => IrLimit(Some(expr), l.fetch))
            .orElse(Some(IrLimit(Some(expr), None)))
        val newTree: IrQuery.Select = tree.copy(limit = limit)
        SelectQuery(params, newTree)

    /**
     * Limits the result to at most `n` rows. Maps to `LIMIT` or `FETCH NEXT`.
     *
     * {{{
     * from(User).drop(10).take(5)
     * }}}
     */
    def take(n: Int)(using
        s: TakeSize[n.type]
    ): SelectQuery[T, OKS, L, s.R] =
        val expr = IrExpr.Native(SqlExpr.NumberLiteral(n))
        val limit = tree.limit
            .map(l => IrLimit(l.offset, Some(IrFetch(expr, SqlFetchUnit.RowCount, SqlFetchMode.Only))))
            .orElse(Some(IrLimit(None, Some(IrFetch(expr, SqlFetchUnit.RowCount, SqlFetchMode.Only)))))
        val newTree: IrQuery.Select = tree.copy(limit = limit)
        SelectQuery(params, newTree)

    /**
     * Locks selected rows for update. Maps to `FOR UPDATE`.
     *
     * {{{
     * from(User).forUpdate
     * }}}
     */
    def forUpdate: Query[T, OKS, L, S] =
        val newTree: IrQuery.Select = tree.copy(lock = Some(IrLock.Update(None)))
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
        val newTree: IrQuery.Select = tree.copy(lock = Some(IrLock.Update(Some(IrLockWaitMode.NoWait))))
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
        val newTree: IrQuery.Select = tree.copy(lock = Some(IrLock.Update(Some(IrLockWaitMode.SkipLocked))))
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
        val newTree: IrQuery.Select = tree.copy(lock = Some(IrLock.Share(None)))
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
        val newTree: IrQuery.Select = tree.copy(lock = Some(IrLock.Share(Some(IrLockWaitMode.NoWait))))
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
        val newTree: IrQuery.Select = tree.copy(lock = Some(IrLock.Share(Some(IrLockWaitMode.SkipLocked))))
        Query(params, newTree)

/**
 * A query with an `ORDER BY` clause, enabling `takeWithTies`/`limitWithTies`
 * in addition to all `Query` operations.
 */
sealed class SortedSelectQuery[T, OKS <: Tuple, L <: Int, S <: QuerySize](
    override private[sqala] val params: T,
    override private[sqala] val tree: IrQuery.Select
)(using
    override private[sqala] val qc: QueryContext[L]
) extends SelectQuery[T, OKS, L, S](params, tree):
    override def drop(n: Int): SortedSelectQuery[T, OKS, L, S] =
        val expr = IrExpr.Native(SqlExpr.NumberLiteral(n))
        val limit = tree.limit
            .map(l => IrLimit(Some(expr), l.fetch))
            .orElse(Some(IrLimit(Some(expr), None)))
        val newTree: IrQuery.Select = tree.copy(limit = limit)
        SortedSelectQuery(params, newTree)

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
    ): SelectQuery[T, OKS, L, s.R] =
        val expr = IrExpr.Native(SqlExpr.NumberLiteral(n))
        val limit = tree.limit
            .map(l => IrLimit(l.offset, Some(IrFetch(expr, SqlFetchUnit.RowCount, SqlFetchMode.WithTies))))
            .orElse(Some(IrLimit(None, Some(IrFetch(expr, SqlFetchUnit.RowCount, SqlFetchMode.WithTies)))))
        val newTree: IrQuery.Select = tree.copy(limit = limit)
        SelectQuery(params, newTree)

/**
 * A `SELECT` query. Created by `from`.
 */
final case class TableQuery[T, OKS <: Tuple, L <: Int](
    override private[sqala] val params: T,
    override private[sqala] val tree: IrQuery.Select
)(using
    override private[sqala] val qc: QueryContext[L]
) extends SelectQuery[T, OKS, L, ManyRows](params, tree):
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
        ba: ValidateArgType[SqlBoolean, a.R, "boolean"],
        kt: KindToTuple[a.K],
        ci: CanInFilter[kt.R],
        e: ExcludeCurrentLevelColumn[kt.R, L],
        c: CombineKindTuple[OKS, e.R]
    ): TableQuery[T, c.R, L] =
        val cond = a.asExpr(f(params))
        TableQuery(params, tree.addWhere(cond.asIrExpr))

    /**
     * Conditionally adds a `WHERE` clause. The condition is only applied
     * when `test` evaluates to `true`. Useful for building dynamic queries
     * without manually concatenating `WHERE TRUE`.
     *
     * {{{
     * val name: Option[String] = None
     * from(User).filterIf(name.isDefined)(u => u.name === name)
     * }}}
     */
    def filterIf[F](test: => Boolean)(f: QueryContext[L] ?=> T => F)(using
        a: AsExpr[F, L],
        ba: ValidateArgType[SqlBoolean, a.R, "boolean"],
        kt: KindToTuple[a.K],
        ci: CanInFilter[kt.R],
        e: ExcludeCurrentLevelColumn[kt.R, L],
        c: CombineKindTuple[OKS, e.R]
    ): TableQuery[T, c.R, L] =
        if test then filter(f) else TableQuery(params, tree)

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
        ba: ValidateArgType[SqlBoolean, a.R, "boolean"],
        kt: KindToTuple[a.K],
        ci: CanInFilter[kt.R],
        e: ExcludeCurrentLevelColumn[kt.R, L],
        c: CombineKindTuple[OKS, e.R]
    ): TableQuery[T, c.R, L] =
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
        c: CombineKindTuple[OKS, e.R],
        an: AggNotNested[a.KS]
    ): SortedTableQuery[T, c.R, L] =
        val sort = a.asSorts(f(params))
        SortedTableQuery(
            params,
            tree.copy(orderBy = tree.orderBy ++ sort.map(_.asIrOrderingItem))
        )

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
        an: AggNotNested[a.KS]
    ): MappedQuery[a.R, T, c.R, L, i.R] =
        val mapped = f(params)
        val sqlSelect = a.asSelectItems(mapped, 1)
        MappedQuery(
            a.transform(mapped),
            params,
            tree.copy(select = sqlSelect)
        )

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
        td: ToDistinct[M, L],
        an: AggNotNested[a.KS]
    ): MappedDistinctQuery[a.R, td.R, c.R, L, i.R] =
        val mapped = f(params)
        val sqlSelect = a.asSelectItems(mapped, 1)
        MappedDistinctQuery(
            a.transform(mapped),
            td.toDistinct(mapped),
            tree.copy(quantifier = Some(SqlQuantifier.Distinct), select = sqlSelect)
        )

    /**
     * Adds a `GROUP BY` clause. All non-grouped columns are marked as
     * `UngroupedColumn` and rejected from subsequent `having` or `map`
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
                    IrGroup(None, groupExprs.map(g => IrGroupingItem.Expr(g.asIrExpr)))
                )
            )
        )

    /**
     * Adds a multi-dimensional `GROUP BY` clause. All non-grouped columns are marked as
     * `UngroupedColumn` and rejected from subsequent `having` or `map`
     * unless wrapped in aggregate functions.
     *
     * Supports grouping by expressions, tuples, and named tuples.
     *
     * {{{
     * from(Entity).groupBySets(e => (a = e.a, b = e.b))(
     *     g => groupingSets((), g.a, (g.a, g.b))
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
    )(g: QueryContext[L] ?=> a.R => S)(using
        s: AsMultidimensionalGrouping[S]
    ): Grouping[to.R *: tt.R, c.R, L] =
        val group = a.asGroup(f(params))
        val groupingItems = s.asIrGroupingItems(g(group))
        Grouping(
            to.toOption(group) *: tt.toTuple(tu.transform(tot.toOption(params))),
            tree.copy(
                groupBy = Some(
                    IrGroup(None, groupingItems)
                )
            )
        )

// object TableQuery:
    /**
     * Creates a hierarchical recursive query using `WITH RECURSIVE`
     * under the hood. The function receives the current and prior
     * table references for the recursive join condition.
     *
     * {{{
     * from(Comment).filter(c => c.postId === postId)
     *     .connectBy((c, prior) => c.parentId === prior.id)
     *     .startWith(c => c.parentId.isNull)
     *     .map(c => (id = c.id, content = c.content, level = level()))
     * }}}
     */
    // extension [T, OKS <: Tuple, L <: Int](query: TableQuery[Table[T, Column, L], OKS, L])
    //     def connectBy[F](f: QueryContext[L] ?=> (Table[T, Column, L], Table[T, Column, L]) => F)(using
    //         a: AsExpr[F, L],
    //         b: SqlBoolean[a.R],
    //         kt: KindToTuple[a.K],
    //         i: CanInFilter[kt.R],
    //         e: ExcludeCurrentLevelColumn[kt.R, L],
    //         c: CombineKindTuple[OKS, e.R]
    //     ): ConnectBy[T, c.R, L] =
    //         given QueryContext[L] = query.qc
    //         val cond = a.asExpr(
    //             f(query.params, query.params.copy(__aliasName__ = tableCte))
    //         ).asIrExpr
    //         val joinTree = query.tree
    //             .copy(
    //                 select = query.tree.select :+ SqlSelectItem.Expr(
    //                     SqlExpr.Binary(
    //                         SqlExpr.Column(Some(tableCte), columnPseudoLevel),
    //                         SqlBinaryOperator.Plus,
    //                         SqlExpr.NumberLiteral(1)
    //                     ),
    //                     Some(columnPseudoLevel)
    //                 ),
    //                 from = SqlTable.Join(
    //                     query.tree.from.head,
    //                     SqlJoinType.Inner,
    //                     SqlTable.Ident(tableCte, None, None, None, None),
    //                     Some(SqlJoinCondition.On(cond))
    //                 ) :: Nil
    //             )
    //         val startTree = query.tree
    //             .copy(
    //                 select = query.tree.select :+ SqlSelectItem.Expr(
    //                     SqlExpr.NumberLiteral(1),
    //                     Some(columnPseudoLevel)
    //                 )
    //             )
    //         ConnectBy(query.params, joinTree, startTree)

/**
 * A sorted `SELECT` query.
 */
final case class SortedTableQuery[T, OKS <: Tuple, L <: Int](
    override private[sqala] val params: T,
    override private[sqala] val tree: IrQuery.Select
)(using
    override private[sqala] val qc: QueryContext[L]
) extends SortedSelectQuery[T, OKS, L, ManyRows](params, tree):
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
        c: CombineKindTuple[OKS, e.R],
        an: AggNotNested[a.KS]
    ): SortedTableQuery[T, c.R, L] =
        val sort = a.asSorts(f(params))
        SortedTableQuery(
            params,
            tree.copy(orderBy = tree.orderBy ++ sort.map(_.asIrOrderingItem))
        )

/**
 * A `SELECT` query after `map` projection.
 */
final case class MappedQuery[M, T, OKS <: Tuple, L <: Int, S <: QuerySize](
    override private[sqala] val params: M,
    private[sqala] val tables: T,
    override private[sqala] val tree: IrQuery.Select
)(using
    override private[sqala] val qc: QueryContext[L]
) extends SelectQuery[M, OKS, L, S](params, tree):
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
        c: CombineKindTuple[OKS, e.R],
        an: AggNotNested[a.KS]
    ): MappedSortedQuery[M, T, c.R, L, S] =
        val sort = a.asSorts(f(tables))
        MappedSortedQuery(
            params,
            tables,
            tree.copy(orderBy = tree.orderBy ++ sort.map(_.asIrOrderingItem))
        )

/**
 * A `SELECT` query after `map` and `sortBy`.
 */
final case class MappedSortedQuery[M, T, OKS <: Tuple, L <: Int, S <: QuerySize](
    override private[sqala] val params: M,
    private[sqala] val tables: T,
    override private[sqala] val tree: IrQuery.Select
)(using
    override private[sqala] val qc: QueryContext[L]
) extends SortedSelectQuery[M, OKS, L, S](params, tree):
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
        c: CombineKindTuple[OKS, e.R],
        an: AggNotNested[a.KS]
    ): MappedSortedQuery[M, T, c.R, L, S] =
        val sort = a.asSorts(f(tables))
        MappedSortedQuery(
            params,
            tables,
            tree.copy(orderBy = tree.orderBy ++ sort.map(_.asIrOrderingItem))
        )

/**
 * A `SELECT DISTINCT` query after `mapDistinct` projection.
 */
final case class MappedDistinctQuery[M, D, OKS <: Tuple, L <: Int, S <: QuerySize](
    override private[sqala] val params: M,
    private[sqala] val distinctExprs: D,
    override private[sqala] val tree: IrQuery.Select
)(using
    override private[sqala] val qc: QueryContext[L]
) extends SelectQuery[M, OKS, L, S](params, tree):
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
    ): MappedSortedDistinctQuery[M, D, OKS, L, S] =
        val sort = a.asSorts(f(distinctExprs))
        MappedSortedDistinctQuery(
            params,
            distinctExprs,
            tree.copy(orderBy = tree.orderBy ++ sort.map(_.asIrOrderingItem))
        )

/**
 * A sorted `SELECT DISTINCT` query after `mapDistinct` and `sortBy`.
 */
final case class MappedSortedDistinctQuery[M, D, OKS <: Tuple, L <: Int, S <: QuerySize](
    override private[sqala] val params: M,
    private[sqala] val distinctExprs: D,
    override private[sqala] val tree: IrQuery.Select
)(using
    override private[sqala] val qc: QueryContext[L]
) extends SortedSelectQuery[M, OKS, L, S](params, tree):
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
    ): MappedSortedDistinctQuery[M, D, OKS, L, S] =
        val sort = a.asSorts(f(distinctExprs))
        MappedSortedDistinctQuery(
            params,
            distinctExprs,
            tree.copy(orderBy = tree.orderBy ++ sort.map(_.asIrOrderingItem))
        )

/**
 * A `SELECT` query after `groupBy`.
 */
final case class Grouping[T, OKS <: Tuple, L <: Int](
    private[sqala] val params: T,
    private[sqala] val tree: IrQuery.Select
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
    // TODO SqlBoolean改掉
    def having[F](f: QueryContext[L] ?=> GroupingContext ?=> T => F)(using
        a: AsExpr[F, L],
        b: SqlBoolean[a.R],
        kt: KindToTuple[a.K],
        i: CanInHaving[kt.R],
        e: ExcludeCurrentLevelColumn[kt.R, L],
        c: CombineKindTuple[OKS, e.R],
        an: AggNotNested[kt.R]
    ): Grouping[T, c.R, L] =
        val cond = a.asExpr(f(params))
        Grouping(params, tree.addHaving(cond.asIrExpr))

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
        c: CombineKindTuple[OKS, e.R],
        an: AggNotNested[a.KS]
    ): GroupedQuery[a.R, T, c.R, L] =
        val mapped = f(params)
        val sqlSelect = a.asSelectItems(mapped, 1)
        GroupedQuery(
            a.transform(mapped),
            params,
            tree.copy(select = sqlSelect)
        )

/**
 * A grouped `SELECT` query after `map` projection.
 */
final case class GroupedQuery[M, T, OKS <: Tuple, L <: Int](
    override private[sqala] val params: M,
    private[sqala] val tables: T,
    override private[sqala] val tree: IrQuery.Select
)(using
    override private[sqala] val qc: QueryContext[L]
) extends SelectQuery[M, OKS, L, ManyRows](params, tree):
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
        c: CombineKindTuple[OKS, e.R],
        an: AggNotNested[a.KS]
    ): GroupedSortedQuery[M, T, c.R, L] =
        val sort = a.asSorts(f(tables))
        GroupedSortedQuery(
            params,
            tables,
            tree.copy(orderBy = tree.orderBy ++ sort.map(_.asIrOrderingItem))
        )

/**
 * A grouped and sorted `SELECT` query.
 */
final case class GroupedSortedQuery[M, T, OKS <: Tuple, L <: Int](
    override private[sqala] val params: M,
    private[sqala] val tables: T,
    override private[sqala] val tree: IrQuery.Select
)(using
    override private[sqala] val qc: QueryContext[L]
) extends SortedSelectQuery[M, OKS, L, ManyRows](params, tree):
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
        c: CombineKindTuple[OKS, e.R],
        an: AggNotNested[a.KS]
    ): GroupedSortedQuery[M, T, c.R, L] =
        val sort = a.asSorts(f(tables))
        GroupedSortedQuery(
            params,
            tables,
            tree.copy(orderBy = tree.orderBy ++ sort.map(_.asIrOrderingItem))
        )

/**
 * A query produced by set operations (`union`, `except`, `intersect`).
 */
final case class UnionQuery[T, OKS <: Tuple, L <: Int](
    override private[sqala] val params: T,
    override private[sqala] val tree: IrQuery.Set
)(using
    override private[sqala] val qc: QueryContext[L]
) extends Query[T, OKS, L, ManyRows](params, tree)

/**
 * A hierarchical recursive query created by `connectBy`. Converts to a
 * standard `WITH RECURSIVE` CTE at SQL generation time.
 */
// final case class ConnectBy[T, OKS <: Tuple, L <: Int](
//     private[sqala] val table: Table[T, Column, L],
//     private[sqala] val connectByTree: IrQuery.Select,
//     private[sqala] val startWithTree: IrQuery.Select,
//     private[sqala] val mapTree: IrQuery.Select =
//         SqlQuery.Select(
//             None,
//             Nil,
//             SqlTable.Ident(tableCte, None, None, None, None) :: Nil,
//             None,
//             None,
//             None,
//             Nil,
//             None,
//             Nil,
//             None,
//             None
//         )
// )(using private[sqala] val qc: QueryContext[L]):
//     given ConnectByContext = ConnectByContext()

//     /**
//      * Specifies the root-level condition for starting recursion.
//      * Maps to the base `WHERE` clause of the CTE.
//      *
//      * {{{
//      * from(Comment).connectBy((c, prior) => c.parentId === prior.id)
//      *     .startWith(c => c.parentId.isNull)
//      * }}}
//      */
//     def startWith[F](f: QueryContext[L] ?=> ConnectByContext ?=> Table[T, Column, L] => F)(using
//         a: AsExpr[F, L],
//         kt: KindToTuple[a.K],
//         i: CanInFilter[a.R, kt.R],
//         e: ExcludeCurrentLevelColumn[kt.R, L],
//         c: CombineKindTuple[OKS, e.R]
//     ): ConnectBy[T, c.R, L] =
//         val cond = a.asExpr(f(table))
//         copy(
//             startWithTree = startWithTree.addWhere(cond.asIrExpr)
//         )

//     /**
//      * Limits the recursion depth. Maps to a filter on the pseudo
//      * `LEVEL` column.
//      *
//      * {{{
//      * from(Comment).connectBy(...).startWith(...).maxDepth(5)
//      * }}}
//      */
//     def maxDepth(n: Int): ConnectBy[T, OKS, L] =
//         val cond = SqlExpr.Binary(
//             SqlExpr.Column(Some(tableCte), columnPseudoLevel),
//             SqlBinaryOperator.LessThan,
//             SqlExpr.NumberLiteral(n)
//         )
//         copy(
//             connectByTree = connectByTree.addWhere(cond)
//         )

//     /**
//      * Sorts siblings within each level of the recursion.
//      *
//      * {{{
//      * from(Comment).connectBy(...).startWith(...).sortSiblingsBy(c => c.content)
//      * }}}
//      */
//     def sortSiblingsBy[S](f: QueryContext[L] ?=> ConnectByContext ?=> Table[T, Column, L] => S)(using
//         a: AsSort[S, ManyRows, L],
//         e: ExcludeCurrentLevelColumn[a.KS, L],
//         c: CombineKindTuple[OKS, e.R],
//         ci: CanInSimpleClause[c.R]
//     ): ConnectBy[T, c.R, L] =
//         val sort = f(table)
//         val sqlOrderBy = a.asSorts(sort).map(_.asIrOrderingItem)
//         copy(
//             connectByTree = connectByTree.copy(orderBy = connectByTree.orderBy ++ sqlOrderBy)
//         )

//     /**
//      * Projects the final result from the recursive CTE. Maps to
//      * `SELECT` on the CTE table, generating the complete `WITH RECURSIVE`
//      * query.
//      *
//      * {{{
//      * from(Comment).connectBy((c, prior) => c.parentId === prior.id)
//      *     .startWith(c => c.parentId.isNull)
//      *     .map(c => (id = c.id, content = c.content, level = level()))
//      * }}}
//      */
//     def map[M](f: ConnectByContext ?=> Table[T, Column, L] => M)(using
//         a: AsMap[M, L],
//         i: CanInMap[a.KS],
//         e: ExcludeCurrentLevelColumn[a.KS, L],
//         c: CombineKindTuple[OKS, e.R],
//         an: AggNotNested[a.KS]
//     ): Query[a.R, c.R, L, ManyRows] =
//         val mapped = f(
//             Table[T, Column, L](
//                 tableCte,
//                 table.__metaData__
//             )
//         )
//         val sqlSelect = a.asSelectItems(mapped, 1)
//         val metaData = table.__metaData__
//         val unionQuery = SqlQuery.Set(
//             startWithTree,
//             SqlSetOperator.Union(Some(SqlQuantifier.All)),
//             None,
//             connectByTree,
//             Nil,
//             None
//         )
//         val withItem = SqlWithItem(
//             tableCte, metaData.columns :+ columnPseudoLevel, unionQuery
//         )
//         val cteTree: SqlQuery.With = SqlQuery.With(
//             true,
//             NonEmptyList(withItem, Nil),
//             mapTree.copy(select = sqlSelect)
//         )
//         Query(a.transform(mapped), cteTree)