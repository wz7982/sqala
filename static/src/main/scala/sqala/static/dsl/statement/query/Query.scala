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

sealed class Query[T, OKS <: Tuple, L <: Int, S <: QuerySize](
    private[sqala] val params: T,
    private[sqala] val tree: SqlQuery
)(using
    private[sqala] val qc: QueryContext[L]
):
    def sql(dialect: Dialect, standardEscapeStrings: Boolean = true): String =
        queryToString(tree, dialect, standardEscapeStrings)

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

    def offset(n: Int): Query[T, OKS, L, S] =
        drop(n)

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

    def take(n: Int)(using
        s: TakeSize[n.type]
    ): Query[T, OKS, L, s.R] =
        takeExpr[s.R](n.asExpr.asSqlExpr, SqlFetchUnit.RowCount, SqlFetchMode.Only)

    def limit(n: Int)(using
        s: TakeSize[n.type]
    ): Query[T, OKS, L, s.R] =
        take(n)

    def forUpdate: Query[T, OKS, L, S] =
        val newTree = tree match
            case s: SqlQuery.Select => s.copy(lock = Some(SqlLock.Update(None)))
            case s: SqlQuery.Set => s.copy(lock = Some(SqlLock.Update(None)))
            case c: SqlQuery.Cte => c.copy(lock = Some(SqlLock.Update(None)))
            case v: SqlQuery.Values => v.copy(lock = Some(SqlLock.Update(None)))
        Query(params, newTree)

    def forUpdateNoWait: Query[T, OKS, L, S] =
        val newTree = tree match
            case s: SqlQuery.Select => s.copy(lock = Some(SqlLock.Update(Some(SqlLockWaitMode.NoWait))))
            case s: SqlQuery.Set => s.copy(lock = Some(SqlLock.Update(Some(SqlLockWaitMode.NoWait))))
            case c: SqlQuery.Cte => c.copy(lock = Some(SqlLock.Update(Some(SqlLockWaitMode.NoWait))))
            case v: SqlQuery.Values => v.copy(lock = Some(SqlLock.Update(Some(SqlLockWaitMode.NoWait))))
        Query(params, newTree)

    def forUpdateSkipLocked: Query[T, OKS, L, S] =
        val newTree = tree match
            case s: SqlQuery.Select => s.copy(lock = Some(SqlLock.Update(Some(SqlLockWaitMode.SkipLocked))))
            case s: SqlQuery.Set => s.copy(lock = Some(SqlLock.Update(Some(SqlLockWaitMode.SkipLocked))))
            case c: SqlQuery.Cte => c.copy(lock = Some(SqlLock.Update(Some(SqlLockWaitMode.SkipLocked))))
            case v: SqlQuery.Values => v.copy(lock = Some(SqlLock.Update(Some(SqlLockWaitMode.SkipLocked))))
        Query(params, newTree)

    def forShare: Query[T, OKS, L, S] =
        val newTree = tree match
            case s: SqlQuery.Select => s.copy(lock = Some(SqlLock.Share(None)))
            case s: SqlQuery.Set => s.copy(lock = Some(SqlLock.Share(None)))
            case c: SqlQuery.Cte => c.copy(lock = Some(SqlLock.Share(None)))
            case v: SqlQuery.Values => v.copy(lock = Some(SqlLock.Share(None)))
        Query(params, newTree)

    def forShareNoWait: Query[T, OKS, L, S] =
        val newTree = tree match
            case s: SqlQuery.Select => s.copy(lock = Some(SqlLock.Share(Some(SqlLockWaitMode.NoWait))))
            case s: SqlQuery.Set => s.copy(lock = Some(SqlLock.Share(Some(SqlLockWaitMode.NoWait))))
            case c: SqlQuery.Cte => c.copy(lock = Some(SqlLock.Share(Some(SqlLockWaitMode.NoWait))))
            case v: SqlQuery.Values => v.copy(lock = Some(SqlLock.Share(Some(SqlLockWaitMode.NoWait))))
        Query(params, newTree)

    def forShareSkipLocked: Query[T, OKS, L, S] =
        val newTree = tree match
            case s: SqlQuery.Select => s.copy(lock = Some(SqlLock.Share(Some(SqlLockWaitMode.SkipLocked))))
            case s: SqlQuery.Set => s.copy(lock = Some(SqlLock.Share(Some(SqlLockWaitMode.SkipLocked))))
            case c: SqlQuery.Cte => c.copy(lock = Some(SqlLock.Share(Some(SqlLockWaitMode.SkipLocked))))
            case v: SqlQuery.Values => v.copy(lock = Some(SqlLock.Share(Some(SqlLockWaitMode.SkipLocked))))
        Query(params, newTree)

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

    def takeWithTies(n: Int)(using
        s: TakeSize[n.type]
    ): SortedQuery[T, OKS, L, s.R] =
        takeExpr[s.R](n.asExpr.asSqlExpr, SqlFetchUnit.RowCount, SqlFetchMode.WithTies)

    def limitWithTies(n: Int)(using
        s: TakeSize[n.type]
    ): SortedQuery[T, OKS, L, s.R] =
        takeWithTies(n)

final case class SelectQuery[T, OKS <: Tuple, L <: Int](
    override private[sqala] val params: T,
    override private[sqala] val tree: SqlQuery.Select
)(using
    override private[sqala] val qc: QueryContext[L]
) extends Query[T, OKS, L, ManyRows](params, tree):
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

    def where[F](f: QueryContext[L] ?=> T => F)(using
        a: AsExpr[F, L],
        b: SqlBoolean[a.R],
        kt: KindToTuple[a.K],
        i: CanInFilter[kt.R],
        e: ExcludeCurrentLevelColumn[kt.R, L],
        c: CombineKindTuple[OKS, e.R]
    ): SelectQuery[T, c.R, L] =
        filter(f)

    def filterIf[F](test: => Boolean)(f: QueryContext[L] ?=> T => F)(using
        a: AsExpr[F, L],
        b: SqlBoolean[a.R],
        kt: KindToTuple[a.K],
        i: CanInFilter[kt.R],
        e: ExcludeCurrentLevelColumn[kt.R, L],
        c: CombineKindTuple[OKS, e.R]
    ): SelectQuery[T, c.R, L] =
        if test then filter(f) else this.asInstanceOf[SelectQuery[T, c.R, L]]

    def whereIf[F](test: => Boolean)(f: QueryContext[L] ?=> T => F)(using
        a: AsExpr[F, L],
        b: SqlBoolean[a.R],
        kt: KindToTuple[a.K],
        i: CanInFilter[kt.R],
        e: ExcludeCurrentLevelColumn[kt.R, L],
        c: CombineKindTuple[OKS, e.R]
    ): SelectQuery[T, c.R, L] =
        filterIf(test)(f)

    def withFilter[F](f: QueryContext[L] ?=> T => F)(using
        a: AsExpr[F, L],
        b: SqlBoolean[a.R],
        kt: KindToTuple[a.K],
        i: CanInFilter[kt.R],
        e: ExcludeCurrentLevelColumn[kt.R, L],
        c: CombineKindTuple[OKS, e.R]
    ): SelectQuery[T, c.R, L] =
        filter(f)

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

    def orderBy[S](f: QueryContext[L] ?=> T => S)(using
        a: AsSort[S, ManyRows, L],
        e: ExcludeCurrentLevelColumn[a.KS, L],
        c: CombineKindTuple[OKS, e.R]
    ): SortedSelectQuery[T, c.R, L] =
        sortBy(f)

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

    def select[M](f: QueryContext[L] ?=> T => M)(using
        a: AsMap[M, L],
        i: CanInMap[a.KS],
        e: ExcludeCurrentLevelColumn[a.KS, L],
        c: CombineKindTuple[OKS, e.R]
    ): MappedSelectQuery[a.R, T, c.R, L, i.R] =
        map(f)

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

    def selectDistinct[M](f: QueryContext[L] ?=> T => M)(using
        a: AsMap[M, L],
        i: CanInMap[a.KS],
        e: ExcludeCurrentLevelColumn[a.KS, L],
        c: CombineKindTuple[OKS, e.R],
        td: ToDistinct[M, L]
    ): MappedDistinctSelectQuery[a.R, td.R, c.R, L, i.R] =
        mapDistinct(f)

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

final case class SortedSelectQuery[T, OKS <: Tuple, L <: Int](
    override private[sqala] val params: T,
    override private[sqala] val tree: SqlQuery.Select
)(using
    override private[sqala] val qc: QueryContext[L]
) extends SortedQuery[T, OKS, L, ManyRows](params, tree):
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

    def orderBy[S](f: QueryContext[L] ?=> T => S)(using
        a: AsSort[S, ManyRows, L],
        e: ExcludeCurrentLevelColumn[a.KS, L],
        c: CombineKindTuple[OKS, e.R]
    ): SortedSelectQuery[T, c.R, L] =
        sortBy(f)

final case class MappedSelectQuery[M, T, OKS <: Tuple, L <: Int, S <: QuerySize](
    override private[sqala] val params: M,
    private[sqala] val tables: T,
    override private[sqala] val tree: SqlQuery.Select
)(using
    override private[sqala] val qc: QueryContext[L]
) extends Query[M, OKS, L, S](params, tree):
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

    def orderBy[SS](f: QueryContext[L] ?=> T => SS)(using
        a: AsSort[SS, S, L],
        e: ExcludeCurrentLevelColumn[a.KS, L],
        c: CombineKindTuple[OKS, e.R]
    ): MappedSortedSelectQuery[M, T, c.R, L, S] =
        sortBy(f)

final case class MappedSortedSelectQuery[M, T, OKS <: Tuple, L <: Int, S <: QuerySize](
    override private[sqala] val params: M,
    private[sqala] val tables: T,
    override private[sqala] val tree: SqlQuery.Select
)(using
    override private[sqala] val qc: QueryContext[L]
) extends SortedQuery[M, OKS, L, S](params, tree):
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

    def orderBy[SS](f: QueryContext[L] ?=> T => SS)(using
        a: AsSort[SS, S, L],
        e: ExcludeCurrentLevelColumn[a.KS, L],
        c: CombineKindTuple[OKS, e.R]
    ): MappedSortedSelectQuery[M, T, c.R, L, S] =
        sortBy(f)

final case class MappedDistinctSelectQuery[M, D, OKS <: Tuple, L <: Int, S <: QuerySize](
    override private[sqala] val params: M,
    private[sqala] val distinctExprs: D,
    override private[sqala] val tree: SqlQuery.Select
)(using
    override private[sqala] val qc: QueryContext[L]
) extends Query[M, OKS, L, S](params, tree):
    def sortBy[SS](f: D => SS)(using
        a: AsDistinctSort[SS, L]
    ): MappedSortedDistinctSelectQuery[M, D, OKS, L, S] =
        val sort = a.asSorts(f(distinctExprs))
        MappedSortedDistinctSelectQuery(
            params,
            distinctExprs,
            tree.copy(orderBy = tree.orderBy ++ sort.map(_.asSqlOrderBy))
        )

    def orderBy[SS](f: D => SS)(using
        a: AsDistinctSort[SS, L]
    ): MappedSortedDistinctSelectQuery[M, D, OKS, L, S] =
        sortBy(f)

final case class MappedSortedDistinctSelectQuery[M, D, OKS <: Tuple, L <: Int, S <: QuerySize](
    override private[sqala] val params: M,
    private[sqala] val distinctExprs: D,
    override private[sqala] val tree: SqlQuery.Select
)(using
    override private[sqala] val qc: QueryContext[L]
) extends SortedQuery[M, OKS, L, S](params, tree):
    def sortBy[SS](f: D => SS)(using
        a: AsDistinctSort[SS, L]
    ): MappedSortedDistinctSelectQuery[M, D, OKS, L, S] =
        val sort = a.asSorts(f(distinctExprs))
        MappedSortedDistinctSelectQuery(
            params,
            distinctExprs,
            tree.copy(orderBy = tree.orderBy ++ sort.map(_.asSqlOrderBy))
        )

    def orderBy[SS](f: D => SS)(using
        a: AsDistinctSort[SS, L]
    ): MappedSortedDistinctSelectQuery[M, D, OKS, L, S] =
        sortBy(f)

final case class Grouping[T, OKS <: Tuple, L <: Int](
    private[sqala] val params: T,
    private[sqala] val tree: SqlQuery.Select
)(using
    private[sqala] val qc: QueryContext[L]
):
    given GroupingContext = GroupingContext()

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

    def select[M](f: QueryContext[L] ?=> GroupingContext ?=> T => M)(using
        a: AsMap[M, L],
        i: CanInMap[a.KS],
        e: ExcludeCurrentLevelColumn[a.KS, L],
        c: CombineKindTuple[OKS, e.R]
    ): GroupedSelectQuery[a.R, T, c.R, L] =
        map(f)

final case class GroupedSelectQuery[M, T, OKS <: Tuple, L <: Int](
    override private[sqala] val params: M,
    private[sqala] val tables: T,
    override private[sqala] val tree: SqlQuery.Select
)(using
    override private[sqala] val qc: QueryContext[L]
) extends Query[M, OKS, L, ManyRows](params, tree):
    given GroupingContext = GroupingContext()

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

    def orderBy[S](f: QueryContext[L] ?=> GroupingContext ?=> T => S)(using
        a: AsGroupedSort[S, L],
        e: ExcludeCurrentLevelColumn[a.KS, L],
        c: CombineKindTuple[OKS, e.R]
    ): GroupedSortedSelectQuery[M, T, c.R, L] =
        sortBy(f)

final case class GroupedSortedSelectQuery[M, T, OKS <: Tuple, L <: Int](
    override private[sqala] val params: M,
    private[sqala] val tables: T,
    override private[sqala] val tree: SqlQuery.Select
)(using
    override private[sqala] val qc: QueryContext[L]
) extends SortedQuery[M, OKS, L, ManyRows](params, tree):
    given GroupingContext = GroupingContext()

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

    def orderBy[S](f: QueryContext[L] ?=> GroupingContext ?=> T => S)(using
        a: AsGroupedSort[S, L],
        e: ExcludeCurrentLevelColumn[a.KS, L],
        c: CombineKindTuple[OKS, e.R]
    ): GroupedSortedSelectQuery[M, T, c.R, L] =
        sortBy(f)

final case class UnionQuery[T, OKS <: Tuple, L <: Int](
    override private[sqala] val params: T,
    override private[sqala] val tree: SqlQuery.Set
)(using
    override private[sqala] val qc: QueryContext[L]
) extends Query[T, OKS, L, ManyRows](params, tree):
    def sortBy[S](f: T => S)(using s: AsColumnSort[S, L]): SortedUnionQuery[T, OKS, L] =
        val sort = s.asSorts(f(params))
        SortedUnionQuery(
            params,
            tree.copy(orderBy = tree.orderBy ++ sort.map(_.asSqlOrderingItem))
        )

    def orderBy[S](f: T => S)(using s: AsColumnSort[S, L]): SortedUnionQuery[T, OKS, L] =
        sortBy(f)

final case class SortedUnionQuery[T, OKS <: Tuple, L <: Int](
    override private[sqala] val params: T,
    override private[sqala] val tree: SqlQuery.Set
)(using
    override private[sqala] val qc: QueryContext[L]
) extends SortedQuery[T, OKS, L, ManyRows](params, tree):
    def sortBy[S](f: T => S)(using s: AsColumnSort[S, L]): SortedUnionQuery[T, OKS, L] =
        val sort = s.asSorts(f(params))
        SortedUnionQuery(
            params,
            tree.copy(orderBy = tree.orderBy ++ sort.map(_.asSqlOrderingItem))
        )

    def orderBy[S](f: T => S)(using s: AsColumnSort[S, L]): SortedUnionQuery[T, OKS, L] =
        sortBy(f)

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

    def maxDepth(n: Int): ConnectBy[T, OKS, L] =
        val cond = SqlExpr.Binary(
            SqlExpr.Column(Some(tableCte), columnPseudoLevel),
            SqlBinaryOperator.LessThan,
            SqlExpr.NumberLiteral(n)
        )
        copy(
            connectByTree = connectByTree.addWhere(cond)
        )

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

    def orderSiblingsBy[S](f: QueryContext[L] ?=> ConnectByContext ?=> Table[T, Column, L] => S)(using
        a: AsSort[S, ManyRows, L],
        e: ExcludeCurrentLevelColumn[a.KS, L],
        c: CombineKindTuple[OKS, e.R]
    ): ConnectBy[T, c.R, L] =
        sortSiblingsBy(f)

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

    def select[M](f: ConnectByContext ?=> Table[T, Column, L] => M)(using
        a: AsMap[M, L],
        i: CanInMap[a.KS],
        e: ExcludeCurrentLevelColumn[a.KS, L],
        c: CombineKindTuple[OKS, e.R]
    ): Query[a.R, c.R, L, ManyRows] =
        map(f)