package sqala.static.dsl.statement.query

import sqala.ast.expr.{SqlBinaryOperator, SqlExpr, SqlSubLinkQuantifier}
import sqala.ast.group.{SqlGroupBy, SqlGroupingItem}
import sqala.ast.limit.{SqlFetch, SqlFetchMode, SqlFetchUnit, SqlLimit}
import sqala.ast.quantifier.SqlQuantifier
import sqala.ast.statement.*
import sqala.ast.table.{SqlJoinCondition, SqlJoinType, SqlTable, SqlTableAlias}
import sqala.printer.Dialect
import sqala.static.dsl.*
import sqala.static.dsl.table.{CanNotInFrom, Table, TransformTableKind}
import sqala.static.metadata.{SqlBoolean, columnPseudoLevel, tableCte}
import sqala.util.queryToString

sealed class Query[T, S <: QuerySize](
    private[sqala] val params: T,
    val tree: SqlQuery
)(using
    private[sqala] val context: QueryContext
):
    def sql(dialect: Dialect, standardEscapeStrings: Boolean = true): String =
        queryToString(tree, dialect, standardEscapeStrings)

    def union[R, RS <: QuerySize](unionQuery: Query[R, RS])(using u: Union[T, R]): UnionQuery[u.R] =
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
        )(using context)

    def unionAll[R, RS <: QuerySize](unionQuery: Query[R, RS])(using u: Union[T, R]): UnionQuery[u.R] =
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
        )(using context)

    def except[R, RS <: QuerySize](unionQuery: Query[R, RS])(using u: Union[T, R]): UnionQuery[u.R] =
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
        )(using context)

    def exceptAll[R, RS <: QuerySize](unionQuery: Query[R, RS])(using u: Union[T, R]): UnionQuery[u.R] =
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
        )(using context)

    def intersect[R, RS <: QuerySize](unionQuery: Query[R, RS])(using u: Union[T, R]): UnionQuery[u.R] =
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
        )(using context)

    def intersectAll[R, RS <: QuerySize](unionQuery: Query[R, RS])(using u: Union[T, R]): UnionQuery[u.R] =
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
        )(using context)

    def drop(n: Int): Query[T, S] =
        val limit = tree match
            case s: SqlQuery.Select => s.limit
            case s: SqlQuery.Set => s.limit
            case SqlQuery.Cte(_, _, s: SqlQuery.Select, _) => s.limit
            case SqlQuery.Cte(_, _, s: SqlQuery.Set, _) => s.limit
            case _ => None
        val sqlLimit = limit
            .map(l => SqlLimit(Some(SqlExpr.NumberLiteral(n)), l.fetch))
            .orElse(Some(SqlLimit(Some(SqlExpr.NumberLiteral(n)), None)))
        val newTree = tree match
            case s: SqlQuery.Select => s.copy(limit = sqlLimit)
            case s: SqlQuery.Set => s.copy(limit = sqlLimit)
            case SqlQuery.Cte(w, r, s: SqlQuery.Select, l) =>
                SqlQuery.Cte(w, r, s.copy(limit = sqlLimit), l)
            case SqlQuery.Cte(w, r, s: SqlQuery.Set, l) =>
                SqlQuery.Cte(w, r, s.copy(limit = sqlLimit), l)
            case _ => tree
        Query(params, newTree)(using context)

    def offset(n: Int): Query[T, S] =
        drop(n)

    private[sqala] def take[TS <: QuerySize](n: Int, unit: SqlFetchUnit, mode: SqlFetchMode): Query[T, TS] =
        val limit = tree match
            case s: SqlQuery.Select => s.limit
            case s: SqlQuery.Set => s.limit
            case SqlQuery.Cte(_, _, s: SqlQuery.Select, _) => s.limit
            case SqlQuery.Cte(_, _, s: SqlQuery.Set, _) => s.limit
            case _ => None
        val sqlLimit = limit
            .map(l => SqlLimit(l.offset, Some(SqlFetch(SqlExpr.NumberLiteral(n), unit, mode))))
            .orElse(Some(SqlLimit(None, Some(SqlFetch(SqlExpr.NumberLiteral(n), unit, mode)))))
        val newTree = tree match
            case s: SqlQuery.Select => s.copy(limit = sqlLimit)
            case s: SqlQuery.Set => s.copy(limit = sqlLimit)
            case SqlQuery.Cte(w, r, s: SqlQuery.Select, l) =>
                SqlQuery.Cte(w, r, s.copy(limit = sqlLimit), l)
            case SqlQuery.Cte(w, r, s: SqlQuery.Set, l) =>
                SqlQuery.Cte(w, r, s.copy(limit = sqlLimit), l)
            case _ => tree
        Query(params, newTree)(using context)

    def take(n: Int)(using s: TakeQuerySize[n.type]): Query[T, s.R] =
        take[s.R](n, SqlFetchUnit.RowCount, SqlFetchMode.Only)

    def limit(n: Int)(using s: TakeQuerySize[n.type]): Query[T, s.R] =
        take(n)

    def forUpdate: Query[T, S] =
        val newTree = tree match
            case s: SqlQuery.Select => s.copy(lock = Some(SqlLock.Update(None)))
            case s: SqlQuery.Set => s.copy(lock = Some(SqlLock.Update(None)))
            case c: SqlQuery.Cte => c.copy(lock = Some(SqlLock.Update(None)))
            case v: SqlQuery.Values => v.copy(lock = Some(SqlLock.Update(None)))
        Query(params, newTree)(using context)

    def forUpdateNoWait: Query[T, S] =
        val newTree = tree match
            case s: SqlQuery.Select => s.copy(lock = Some(SqlLock.Update(Some(SqlLockWaitMode.NoWait))))
            case s: SqlQuery.Set => s.copy(lock = Some(SqlLock.Update(Some(SqlLockWaitMode.NoWait))))
            case c: SqlQuery.Cte => c.copy(lock = Some(SqlLock.Update(Some(SqlLockWaitMode.NoWait))))
            case v: SqlQuery.Values => v.copy(lock = Some(SqlLock.Update(Some(SqlLockWaitMode.NoWait))))
        Query(params, newTree)(using context)

    def forUpdateSkipLocked: Query[T, S] =
        val newTree = tree match
            case s: SqlQuery.Select => s.copy(lock = Some(SqlLock.Update(Some(SqlLockWaitMode.SkipLocked))))
            case s: SqlQuery.Set => s.copy(lock = Some(SqlLock.Update(Some(SqlLockWaitMode.SkipLocked))))
            case c: SqlQuery.Cte => c.copy(lock = Some(SqlLock.Update(Some(SqlLockWaitMode.SkipLocked))))
            case v: SqlQuery.Values => v.copy(lock = Some(SqlLock.Update(Some(SqlLockWaitMode.SkipLocked))))
        Query(params, newTree)(using context)

    def forShare: Query[T, S] =
        val newTree = tree match
            case s: SqlQuery.Select => s.copy(lock = Some(SqlLock.Share(None)))
            case s: SqlQuery.Set => s.copy(lock = Some(SqlLock.Share(None)))
            case c: SqlQuery.Cte => c.copy(lock = Some(SqlLock.Share(None)))
            case v: SqlQuery.Values => v.copy(lock = Some(SqlLock.Share(None)))
        Query(params, newTree)(using context)

    def forShareNoWait: Query[T, S] =
        val newTree = tree match
            case s: SqlQuery.Select => s.copy(lock = Some(SqlLock.Share(Some(SqlLockWaitMode.NoWait))))
            case s: SqlQuery.Set => s.copy(lock = Some(SqlLock.Share(Some(SqlLockWaitMode.NoWait))))
            case c: SqlQuery.Cte => c.copy(lock = Some(SqlLock.Share(Some(SqlLockWaitMode.NoWait))))
            case v: SqlQuery.Values => v.copy(lock = Some(SqlLock.Share(Some(SqlLockWaitMode.NoWait))))
        Query(params, newTree)(using context)

    def forShareSkipLocked: Query[T, S] =
        val newTree = tree match
            case s: SqlQuery.Select => s.copy(lock = Some(SqlLock.Share(Some(SqlLockWaitMode.SkipLocked))))
            case s: SqlQuery.Set => s.copy(lock = Some(SqlLock.Share(Some(SqlLockWaitMode.SkipLocked))))
            case c: SqlQuery.Cte => c.copy(lock = Some(SqlLock.Share(Some(SqlLockWaitMode.SkipLocked))))
            case v: SqlQuery.Values => v.copy(lock = Some(SqlLock.Share(Some(SqlLockWaitMode.SkipLocked))))
        Query(params, newTree)(using context)

    private[sqala] def size: Query[Expr[Long, Column], OneRow] =
        given QueryContext = context
        val countExpr = count()
        val expr = Expr[Long, Column](countExpr.asSqlExpr)
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
                    SqlTable.SubQuery(
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

    private[sqala] def exists: Query[Expr[Boolean, Column], OneRow] =
        given QueryContext = context
        val expr = Expr[Boolean, Column](SqlExpr.SubLink(SqlSubLinkQuantifier.Exists, tree))
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

sealed class SortedQuery[T, S <: QuerySize](
    private[sqala] override val params: T,
    override val tree: SqlQuery
)(using
    private[sqala] override val context: QueryContext
) extends Query[T, S](params, tree):
    override def drop(n: Int): SortedQuery[T, S] =
        val limit = tree match
            case s: SqlQuery.Select => s.limit
            case s: SqlQuery.Set => s.limit
            case SqlQuery.Cte(_, _, s: SqlQuery.Select, _) => s.limit
            case SqlQuery.Cte(_, _, s: SqlQuery.Set, _) => s.limit
            case _ => None
        val sqlLimit = limit
            .map(l => SqlLimit(Some(SqlExpr.NumberLiteral(n)), l.fetch))
            .orElse(Some(SqlLimit(Some(SqlExpr.NumberLiteral(n)), None)))
        val newTree = tree match
            case s: SqlQuery.Select => s.copy(limit = sqlLimit)
            case s: SqlQuery.Set => s.copy(limit = sqlLimit)
            case SqlQuery.Cte(w, r, s: SqlQuery.Select, l) =>
                SqlQuery.Cte(w, r, s.copy(limit = sqlLimit), l)
            case SqlQuery.Cte(w, r, s: SqlQuery.Set, l) =>
                SqlQuery.Cte(w, r, s.copy(limit = sqlLimit), l)
            case _ => tree
        SortedQuery(params, newTree)(using context)

    override def offset(n: Int): SortedQuery[T, S] =
        drop(n)

    private[sqala] override def take[TS <: QuerySize](n: Int, unit: SqlFetchUnit, mode: SqlFetchMode): SortedQuery[T, TS] =
        val limit = tree match
            case s: SqlQuery.Select => s.limit
            case s: SqlQuery.Set => s.limit
            case SqlQuery.Cte(_, _, s: SqlQuery.Select, _) => s.limit
            case SqlQuery.Cte(_, _, s: SqlQuery.Set, _) => s.limit
            case _ => None
        val sqlLimit = limit
            .map(l => SqlLimit(l.offset, Some(SqlFetch(SqlExpr.NumberLiteral(n), unit, mode))))
            .orElse(Some(SqlLimit(None, Some(SqlFetch(SqlExpr.NumberLiteral(n), unit, mode)))))
        val newTree = tree match
            case s: SqlQuery.Select => s.copy(limit = sqlLimit)
            case s: SqlQuery.Set => s.copy(limit = sqlLimit)
            case SqlQuery.Cte(w, r, s: SqlQuery.Select, l) =>
                SqlQuery.Cte(w, r, s.copy(limit = sqlLimit), l)
            case SqlQuery.Cte(w, r, s: SqlQuery.Set, l) =>
                SqlQuery.Cte(w, r, s.copy(limit = sqlLimit), l)
            case _ => tree
        SortedQuery(params, newTree)(using context)

    def takeWithTies(n: Int)(using s: TakeQuerySize[n.type]): SortedQuery[T, s.R] =
        take[s.R](n, SqlFetchUnit.RowCount, SqlFetchMode.WithTies)

    def limitWithTies(n: Int)(using s: TakeQuerySize[n.type]): SortedQuery[T, s.R] =
        takeWithTies(n)

final class SelectQuery[T](
    private[sqala] override val params: T,
    override val tree: SqlQuery.Select
)(using
    private[sqala] override val context: QueryContext
) extends Query[T, ManyRows](params, tree):
    def filter[F: AsExpr as a](f: T => F)(using
        SqlBoolean[a.R],
        CanInFilter[a.K]
    ): SelectQuery[T] =
        val cond = a.asExpr(f(params))
        SelectQuery(params, tree.addWhere(cond.asSqlExpr))(using context)

    def where[F: AsExpr as a](f: T => F)(using
        SqlBoolean[a.R],
        CanInFilter[a.K]
    ): SelectQuery[T] =
        filter(f)

    def filterIf[F: AsExpr as a](test: => Boolean)(f: T => F)(using
        SqlBoolean[a.R],
        CanInFilter[a.K]
    ): SelectQuery[T] =
        if test then filter(f) else this

    def whereIf[F: AsExpr as a](test: => Boolean)(f: T => F)(using
        SqlBoolean[a.R],
        CanInFilter[a.K]
    ): SelectQuery[T] =
        filterIf(test)(f)

    def withFilter[F: AsExpr as a](f: T => F)(using
        SqlBoolean[a.R],
        CanInFilter[a.K]
    ): SelectQuery[T] =
        filter(f)

    def sortBy[S](f: T => S)(using s: AsSort[S, ManyRows]): SortedSelectQuery[T] =
        val sort = s.asSorts(f(params))
        SortedSelectQuery(
            params,
            tree.copy(orderBy = tree.orderBy ++ sort.map(_.asSqlOrderBy))
        )(using context)

    def orderBy[S](f: T => S)(using s: AsSort[S, ManyRows]): SortedSelectQuery[T] =
        sortBy(f)

    def map[M: AsMap as m](f: T => M)(using c: CanInMap[m.K]): MappedSelectQuery[m.R, T, c.R] =
        val mapped = f(params)
        val sqlSelect = m.asSelectItems(mapped, 1)
        MappedSelectQuery(
            m.transform(mapped),
            params,
            tree.copy(select = sqlSelect)
        )(using context)

    def select[M: AsMap as m](f: T => M)(using c: CanInMap[m.K]): MappedSelectQuery[m.R, T, c.R] =
        map(f)

    def mapDistinct[M: AsMap as m](f: T => M)(using
        c: CanInMap[m.K],
        t: TransformKind[m.R, Distinct]
    ): MappedDistinctSelectQuery[t.R, c.R] =
        val mapped = f(params)
        val sqlSelect = m.asSelectItems(mapped, 1)
        MappedDistinctSelectQuery(
            t.transform(m.transform(mapped)),
            tree.copy(quantifier = Some(SqlQuantifier.Distinct), select = sqlSelect)
        )(using context)

    def selectDistinct[M: AsMap as m](f: T => M)(using
        c: CanInMap[m.K],
        t: TransformKind[m.R, Distinct]
    ): MappedDistinctSelectQuery[t.R, c.R] =
        mapDistinct(f)

    def groupBy[G: AsGroup as a](f: T => G)(using
        tu: TransformTableKind[T, Ungrouped],
        t: ToTuple[tu.R]
    ): Grouping[a.R *: t.R] =
        val group = f(params)
        val groupExprs = a.asExprs(group)
        Grouping(
            a.asGroup(group) *: t.toTuple(tu.transform(params)),
            tree.copy(
                groupBy = Some(
                    SqlGroupBy(None, groupExprs.map(g => SqlGroupingItem.Expr(g.asSqlExpr)))
                )
            )
        )(using context)

    def groupByCube[G: AsGroup as a](f: T => G)(using
        to: ToOption[a.R],
        tot: ToOption[T],
        tu: TransformTableKind[tot.R, Ungrouped],
        tt: ToTuple[tu.R]
    ): Grouping[to.R *: tt.R] =
        val group = f(params)
        val groupExprs = a.asExprs(group)
        Grouping(
            to.toOption(a.asGroup(group)) *: tt.toTuple(tu.transform(tot.toOption(params))),
            tree.copy(
                groupBy = Some(
                    SqlGroupBy(None, SqlGroupingItem.Cube(groupExprs.map(_.asSqlExpr)) :: Nil)
                )
            )
        )(using context)

    def groupByRollup[G: AsGroup as a](f: T => G)(using
        to: ToOption[a.R],
        tot: ToOption[T],
        tu: TransformTableKind[tot.R, Ungrouped],
        tt: ToTuple[tu.R]
    ): Grouping[to.R *: tt.R] =
        val group = f(params)
        val groupExprs = a.asExprs(group)
        Grouping(
            to.toOption(a.asGroup(group)) *: tt.toTuple(tu.transform(tot.toOption(params))),
            tree.copy(
                groupBy = Some(
                    SqlGroupBy(None, SqlGroupingItem.Rollup(groupExprs.map(_.asSqlExpr)) :: Nil)
                )
            )
        )(using context)

    def groupBySets[G: AsGroup as a, S: AsGroupingSets as s](f: T => G)(g: G => S)(using
        to: ToOption[a.R],
        tot: ToOption[T],
        tu: TransformTableKind[tot.R, Ungrouped],
        tt: ToTuple[tu.R]
    ): Grouping[to.R *: tt.R] =
        val group = f(params)
        val groupExprs = s.asSqlExprs(g(group))
        Grouping(
            to.toOption(a.asGroup(group)) *: tt.toTuple(tu.transform(tot.toOption(params))),
            tree.copy(
                groupBy = Some(
                    SqlGroupBy(None, SqlGroupingItem.GroupingSets(groupExprs) :: Nil)
                )
            )
        )(using context)

object SelectQuery:
    extension [T](query: SelectQuery[Table[T, Column, CanNotInFrom]])
        def connectBy[F: AsExpr as a](f: (Table[T, Column, CanNotInFrom], Table[T, Column, CanNotInFrom]) => F)(using
            SqlBoolean[a.R],
            CanInFilter[a.K]
        ): ConnectBy[T] =
            given QueryContext = query.context
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

final class SortedSelectQuery[T](
    private[sqala] override val params: T,
    override val tree: SqlQuery.Select
)(using
    private[sqala] override val context: QueryContext
) extends SortedQuery[T, ManyRows](params, tree):
    def sortBy[S](f: T => S)(using s: AsSort[S, ManyRows]): SortedSelectQuery[T] =
        val sort = s.asSorts(f(params))
        SortedSelectQuery(
            params,
            tree.copy(orderBy = tree.orderBy ++ sort.map(_.asSqlOrderBy))
        )(using context)

    def orderBy[S](f: T => S)(using s: AsSort[S, ManyRows]): SortedSelectQuery[T] =
        sortBy(f)

final class MappedSelectQuery[M, T, S <: QuerySize](
    private[sqala] override val params: M,
    private[sqala] val tables: T,
    override val tree: SqlQuery.Select
)(using
    private[sqala] override val context: QueryContext
) extends Query[M, S](params, tree):
    def sortBy[SS](f: T => SS)(using s: AsSort[SS, S]): MappedSortedSelectQuery[M, T, S] =
        val sort = s.asSorts(f(tables))
        MappedSortedSelectQuery(
            params,
            tables,
            tree.copy(orderBy = tree.orderBy ++ sort.map(_.asSqlOrderBy))
        )(using context)

    def orderBy[SS](f: T => SS)(using s: AsSort[SS, S]): MappedSortedSelectQuery[M, T, S] =
        sortBy(f)

final class MappedSortedSelectQuery[M, T, S <: QuerySize](
    private[sqala] override val params: M,
    private[sqala] val tables: T,
    override val tree: SqlQuery.Select
)(using
    private[sqala] override val context: QueryContext
) extends SortedQuery[M, S](params, tree):
    def sortBy[SS](f: T => SS)(using s: AsSort[SS, S]): MappedSortedSelectQuery[M, T, S] =
        val sort = s.asSorts(f(tables))
        MappedSortedSelectQuery(
            params,
            tables,
            tree.copy(orderBy = tree.orderBy ++ sort.map(_.asSqlOrderBy))
        )(using context)

    def orderBy[SS](f: T => SS)(using s: AsSort[SS, S]): MappedSortedSelectQuery[M, T, S] =
        sortBy(f)

final class MappedDistinctSelectQuery[M, S <: QuerySize](
    private[sqala] override val params: M,
    override val tree: SqlQuery.Select
)(using
    private[sqala] override val context: QueryContext
) extends Query[M, S](params, tree):
    def sortBy[SS](f: M => SS)(using s: AsDistinctSort[SS]): MappedSortedDistinctSelectQuery[M, S] =
        val sort = s.asSorts(f(params))
        MappedSortedDistinctSelectQuery(
            params,
            tree.copy(orderBy = tree.orderBy ++ sort.map(_.asSqlOrderBy))
        )(using context)

    def orderBy[SS](f: M => SS)(using s: AsDistinctSort[SS]): MappedSortedDistinctSelectQuery[M, S] =
        sortBy(f)

final class MappedSortedDistinctSelectQuery[M, S <: QuerySize](
    private[sqala] override val params: M,
    override val tree: SqlQuery.Select
)(using
    private[sqala] override val context: QueryContext
) extends SortedQuery[M, S](params, tree):
    def sortBy[SS](f: M => SS)(using s: AsDistinctSort[SS]): MappedSortedDistinctSelectQuery[M, S] =
        val sort = s.asSorts(f(params))
        MappedSortedDistinctSelectQuery(
            params,
            tree.copy(orderBy = tree.orderBy ++ sort.map(_.asSqlOrderBy))
        )(using context)

    def orderBy[SS](f: M => SS)(using s: AsDistinctSort[SS]): MappedSortedDistinctSelectQuery[M, S] =
        sortBy(f)

final class Grouping[T](
    private[sqala] val params: T,
    private[sqala] val tree: SqlQuery.Select
)(using
    private[sqala] val context: QueryContext
):
    given GroupingContext = new GroupingContext

    def having[F: AsExpr as a](f: GroupingContext ?=> T => F)(using
        SqlBoolean[a.R],
        CanInHaving[a.K]
    ): Grouping[T] =
        val cond = a.asExpr(f(params))
        Grouping(params, tree.addHaving(cond.asSqlExpr))(using context)

    def map[M: AsMap as m](f: GroupingContext ?=> T => M)(using CanInGroupedMap[m.K]): GroupedSelectQuery[m.R, T] =
        val mapped = f(params)
        val sqlSelect = m.asSelectItems(mapped, 1)
        GroupedSelectQuery(
            m.transform(mapped),
            params,
            tree.copy(select = sqlSelect)
        )(using context)

    def select[M: AsMap as m](f: GroupingContext ?=> T => M)(using CanInGroupedMap[m.K]): GroupedSelectQuery[m.R, T] =
        map(f)

final class GroupedSelectQuery[M, T](
    private[sqala] override val params: M,
    private[sqala] val tables: T,
    override val tree: SqlQuery.Select
)(using
    private[sqala] override val context: QueryContext
) extends Query[M, ManyRows](params, tree):
    given GroupingContext = new GroupingContext

    def sortBy[S](f: GroupingContext ?=> T => S)(using s: AsGroupedSort[S]): GroupedSortedSelectQuery[M, T] =
        val sort = s.asSorts(f(tables))
        GroupedSortedSelectQuery(
            params,
            tables,
            tree.copy(orderBy = tree.orderBy ++ sort.map(_.asSqlOrderBy))
        )(using context)

    def orderBy[S](f: GroupingContext ?=> T => S)(using s: AsGroupedSort[S]): GroupedSortedSelectQuery[M, T] =
        sortBy(f)

final class GroupedSortedSelectQuery[M, T](
    private[sqala] override val params: M,
    private[sqala] val tables: T,
    override val tree: SqlQuery.Select
)(using
    private[sqala] override val context: QueryContext
) extends SortedQuery[M, ManyRows](params, tree):
    given GroupingContext = new GroupingContext

    def sortBy[S](f: GroupingContext ?=> T => S)(using s: AsGroupedSort[S]): GroupedSortedSelectQuery[M, T] =
        val sort = s.asSorts(f(tables))
        GroupedSortedSelectQuery(
            params,
            tables,
            tree.copy(orderBy = tree.orderBy ++ sort.map(_.asSqlOrderBy))
        )(using context)

    def orderBy[S](f: GroupingContext ?=> T => S)(using s: AsGroupedSort[S]): GroupedSortedSelectQuery[M, T] =
        sortBy(f)

final class UnionQuery[T](
    private[sqala] override val params: T,
    override val tree: SqlQuery.Set
)(using
    QueryContext
) extends Query[T, ManyRows](params, tree):
    def sortBy[S: AsColumnSort as s](f: T => S): SortedUnionQuery[T] =
        val sort = s.asSorts(f(params))
        SortedUnionQuery(
            params,
            tree.copy(orderBy = tree.orderBy ++ sort.map(_.asSqlOrderBy))
        )(using context)

    def orderBy[S: AsColumnSort](f: T => S): SortedUnionQuery[T] =
        sortBy(f)

final class SortedUnionQuery[T](
    private[sqala] override val params: T,
    override val tree: SqlQuery.Set
)(using
    QueryContext
) extends SortedQuery[T, ManyRows](params, tree):
    def sortBy[S: AsColumnSort as s](f: T => S): SortedUnionQuery[T] =
        val sort = s.asSorts(f(params))
        SortedUnionQuery(
            params,
            tree.copy(orderBy = tree.orderBy ++ sort.map(_.asSqlOrderBy))
        )(using context)

    def orderBy[S: AsColumnSort](f: T => S): SortedUnionQuery[T] =
        sortBy(f)

final case class ConnectBy[T](
    private[sqala] val table: Table[T, Column, CanNotInFrom],
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
)(using private[sqala] val context: QueryContext):
    given ConnectByContext = new ConnectByContext

    def startWith[F: AsExpr as a](f: ConnectByContext ?=> Table[T, Column, CanNotInFrom] => F)(using
        SqlBoolean[a.R],
        CanInFilter[a.K]
    ): ConnectBy[T] =
        val cond = a.asExpr(f(table))
        copy(
            startWithTree = startWithTree.addWhere(cond.asSqlExpr)
        )(using context)

    def maxDepth(n: Int): ConnectBy[T] =
        val cond = SqlExpr.Binary(
            SqlExpr.Column(Some(tableCte), columnPseudoLevel),
            SqlBinaryOperator.LessThan,
            SqlExpr.NumberLiteral(n)
        )
        copy(
            connectByTree = connectByTree.addWhere(cond)
        )(using context)

    def sortSiblingsBy[S](f: ConnectByContext ?=> Table[T, Column, CanNotInFrom] => S)(using
        s: AsSort[S, ManyRows]
    ): ConnectBy[T] =
        val sort = f(table)
        val sqlOrderBy = s.asSorts(sort).map(_.asSqlOrderBy)
        copy(
            connectByTree = connectByTree.copy(orderBy = connectByTree.orderBy ++ sqlOrderBy)
        )(using context)

    def orderSiblingsBy[S](f: ConnectByContext ?=> Table[T, Column, CanNotInFrom] => S)(using
        s: AsSort[S, ManyRows]
    ): ConnectBy[T] =
        sortSiblingsBy(f)

    def map[M: AsMap as m](f: ConnectByContext ?=> Table[T, Column, CanNotInFrom] => M)(using
        KindOperation[m.K, Column]
    ): Query[m.R, ManyRows] =
        val mapped = f(
            Table[T, Column, CanNotInFrom](
                Some(tableCte),
                table.__metaData__,
                table.__sqlTable__.copy(
                    alias = table.__sqlTable__.alias.map(_.copy(alias = tableCte))
                )
            )
        )
        val sqlSelect = m.asSelectItems(mapped, 1)
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
            tableCte, unionQuery, metaData.columnNames :+ columnPseudoLevel
        )
        val cteTree: SqlQuery.Cte = SqlQuery.Cte(
            true,
            withItem :: Nil,
            mapTree.copy(select = sqlSelect),
            None
        )
        Query(m.transform(mapped), cteTree)(using context)

    def select[M: AsMap as m](f: ConnectByContext ?=> Table[T, Column, CanNotInFrom] => M)(using
        KindOperation[m.K, Column]
    ): Query[m.R, ManyRows] =
        map(f)

    def mapDistinct[M: AsMap as m](f: ConnectByContext ?=> Table[T, Column, CanNotInFrom] => M)(using
        KindOperation[m.K, Column]
    ): Query[m.R, ManyRows] =
        val mapped = f(
            Table[T, Column, CanNotInFrom](
                Some(tableCte),
                table.__metaData__,
                table.__sqlTable__.copy(
                    alias = table.__sqlTable__.alias.map(_.copy(alias = tableCte))
                )
            )
        )
        val sqlSelect = m.asSelectItems(mapped, 1)
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
            tableCte, unionQuery, metaData.columnNames :+ columnPseudoLevel
        )
        val cteTree: SqlQuery.Cte = SqlQuery.Cte(
            true,
            withItem :: Nil,
            mapTree.copy(select = sqlSelect),
            None
        )
        Query(m.transform(mapped), cteTree)(using context)

    def selectDistinct[M: AsMap as m](f: ConnectByContext ?=> Table[T, Column, CanNotInFrom] => M)(using
        KindOperation[m.K, Column]
    ): Query[m.R, ManyRows] =
        mapDistinct(f)