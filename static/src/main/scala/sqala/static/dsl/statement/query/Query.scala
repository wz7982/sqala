package sqala.static.dsl.statement.query

import sqala.ast.expr.{SqlBinaryOperator, SqlExpr, SqlSubLinkQuantifier}
import sqala.ast.group.{SqlGroupBy, SqlGroupingItem}
import sqala.ast.limit.{SqlFetch, SqlFetchMode, SqlFetchUnit, SqlLimit}
import sqala.ast.quantifier.SqlQuantifier
import sqala.ast.statement.*
import sqala.ast.table.{SqlJoinCondition, SqlJoinType, SqlTable, SqlTableAlias}
import sqala.printer.Dialect
import sqala.static.dsl.*
import sqala.static.dsl.table.{AsUngroupedTable, Table}
import sqala.static.metadata.{SqlBoolean, columnPseudoLevel, tableCte}
import sqala.util.queryToString

sealed class Query[T](
    private[sqala] val params: T,
    val tree: SqlQuery
)(using 
    private[sqala] val context: QueryContext
)

object Query:
    extension [T](query: Query[T])
        def sql(dialect: Dialect, standardEscapeStrings: Boolean = true): String =
            queryToString(query.tree, dialect, standardEscapeStrings)

        def union[R](unionQuery: Query[R])(using u: Union[T, R]): UnionQuery[u.R] =
            UnionQuery(
                u.unionQueryItems(query.params, 1),
                SqlQuery.Set(
                    query.tree, 
                    SqlSetOperator.Union(None), 
                    unionQuery.tree,
                    Nil,
                    None,
                    None
                )
            )(using query.context)

        def unionAll[R](unionQuery: Query[R])(using u: Union[T, R]): UnionQuery[u.R] =
            UnionQuery(
                u.unionQueryItems(query.params, 1),
                SqlQuery.Set(
                    query.tree, 
                    SqlSetOperator.Union(Some(SqlQuantifier.All)), 
                    unionQuery.tree,
                    Nil,
                    None,
                    None
                )
            )(using query.context)

        def except[R](unionQuery: Query[R])(using u: Union[T, R]): UnionQuery[u.R] =
            UnionQuery(
                u.unionQueryItems(query.params, 1),
                SqlQuery.Set(
                    query.tree, 
                    SqlSetOperator.Except(None), 
                    unionQuery.tree,
                    Nil,
                    None,
                    None
                )
            )(using query.context)

        def exceptAll[R](unionQuery: Query[R])(using u: Union[T, R]): UnionQuery[u.R] =
            UnionQuery(
                u.unionQueryItems(query.params, 1),
                SqlQuery.Set(
                    query.tree, 
                    SqlSetOperator.Except(Some(SqlQuantifier.All)), 
                    unionQuery.tree,
                    Nil,
                    None,
                    None
                )
            )(using query.context)

        def intersect[R](unionQuery: Query[R])(using u: Union[T, R]): UnionQuery[u.R] =
            UnionQuery(
                u.unionQueryItems(query.params, 1),
                SqlQuery.Set(
                    query.tree, 
                    SqlSetOperator.Intersect(None), 
                    unionQuery.tree,
                    Nil,
                    None,
                    None
                )
            )(using query.context)

        def intersectAll[R](unionQuery: Query[R])(using u: Union[T, R]): UnionQuery[u.R] =
            UnionQuery(
                u.unionQueryItems(query.params, 1),
                SqlQuery.Set(
                    query.tree, 
                    SqlSetOperator.Intersect(Some(SqlQuantifier.All)), 
                    unionQuery.tree,
                    Nil,
                    None,
                    None
                )
            )(using query.context)

        def drop(n: Int): Query[T] =
            val limit = query.tree match
                case s: SqlQuery.Select => s.limit
                case s: SqlQuery.Set => s.limit
                case SqlQuery.Cte(_, _, s: SqlQuery.Select, _) => s.limit
                case SqlQuery.Cte(_, _, s: SqlQuery.Set, _) => s.limit
                case _ => None
            val sqlLimit = limit
                .map(l => SqlLimit(Some(SqlExpr.NumberLiteral(n)), l.fetch))
                .orElse(Some(SqlLimit(Some(SqlExpr.NumberLiteral(n)), None)))
            val newTree = query.tree match
                case s: SqlQuery.Select => s.copy(limit = sqlLimit)
                case s: SqlQuery.Set => s.copy(limit = sqlLimit)
                case SqlQuery.Cte(w, r, s: SqlQuery.Select, l) =>
                    SqlQuery.Cte(w, r, s.copy(limit = sqlLimit), l)
                case SqlQuery.Cte(w, r, s: SqlQuery.Set, l) =>
                    SqlQuery.Cte(w, r, s.copy(limit = sqlLimit), l)
                case _ => query.tree
            Query(query.params, newTree)(using query.context)

        def offset(n: Int): Query[T] = drop(n)

        private[sqala] def take(n: Int, unit: SqlFetchUnit, mode: SqlFetchMode): Query[T] =
            val limit = query.tree match
                case s: SqlQuery.Select => s.limit
                case s: SqlQuery.Set => s.limit
                case SqlQuery.Cte(_, _, s: SqlQuery.Select, _) => s.limit
                case SqlQuery.Cte(_, _, s: SqlQuery.Set, _) => s.limit
                case _ => None
            val sqlLimit = limit
                .map(l => SqlLimit(l.offset, Some(SqlFetch(SqlExpr.NumberLiteral(n), unit, mode))))
                .orElse(Some(SqlLimit(None, Some(SqlFetch(SqlExpr.NumberLiteral(n), unit, mode)))))
            val newTree = query.tree match
                case s: SqlQuery.Select => s.copy(limit = sqlLimit)
                case s: SqlQuery.Set => s.copy(limit = sqlLimit)
                case SqlQuery.Cte(w, r, s: SqlQuery.Select, l) =>
                    SqlQuery.Cte(w, r, s.copy(limit = sqlLimit), l)
                case SqlQuery.Cte(w, r, s: SqlQuery.Set, l) =>
                    SqlQuery.Cte(w, r, s.copy(limit = sqlLimit), l)
                case _ => query.tree
            Query(query.params, newTree)(using query.context)

        def take(n: Int): Query[T] = take(n, SqlFetchUnit.RowCount, SqlFetchMode.Only)

        def limit(n: Int): Query[T] = take(n)

        def takeWithTies(n: Int): Query[T] = take(n, SqlFetchUnit.RowCount, SqlFetchMode.WithTies)

        def limitWithTies(n: Int): Query[T] = takeWithTies(n)

        def forUpdate: Query[T] =
            val newTree = query.tree match
                case s: SqlQuery.Select => s.copy(lock = Some(SqlLock.Update(None)))
                case s: SqlQuery.Set => s.copy(lock = Some(SqlLock.Update(None)))
                case c: SqlQuery.Cte => c.copy(lock = Some(SqlLock.Update(None)))
                case v: SqlQuery.Values => v.copy(lock = Some(SqlLock.Update(None)))
            Query(query.params, newTree)(using query.context)

        def forUpdateNoWait: Query[T] =
            val newTree = query.tree match
                case s: SqlQuery.Select => s.copy(lock = Some(SqlLock.Update(Some(SqlLockWaitMode.NoWait))))
                case s: SqlQuery.Set => s.copy(lock = Some(SqlLock.Update(Some(SqlLockWaitMode.NoWait))))
                case c: SqlQuery.Cte => c.copy(lock = Some(SqlLock.Update(Some(SqlLockWaitMode.NoWait))))
                case v: SqlQuery.Values => v.copy(lock = Some(SqlLock.Update(Some(SqlLockWaitMode.NoWait))))
            Query(query.params, newTree)(using query.context)

        def forUpdateSkipLocked: Query[T] =
            val newTree = query.tree match
                case s: SqlQuery.Select => s.copy(lock = Some(SqlLock.Update(Some(SqlLockWaitMode.SkipLocked))))
                case s: SqlQuery.Set => s.copy(lock = Some(SqlLock.Update(Some(SqlLockWaitMode.SkipLocked))))
                case c: SqlQuery.Cte => c.copy(lock = Some(SqlLock.Update(Some(SqlLockWaitMode.SkipLocked))))
                case v: SqlQuery.Values => v.copy(lock = Some(SqlLock.Update(Some(SqlLockWaitMode.SkipLocked))))
            Query(query.params, newTree)(using query.context)

        def forShare: Query[T] =
            val newTree = query.tree match
                case s: SqlQuery.Select => s.copy(lock = Some(SqlLock.Share(None)))
                case s: SqlQuery.Set => s.copy(lock = Some(SqlLock.Share(None)))
                case c: SqlQuery.Cte => c.copy(lock = Some(SqlLock.Share(None)))
                case v: SqlQuery.Values => v.copy(lock = Some(SqlLock.Share(None)))
            Query(query.params, newTree)(using query.context)

        def forShareNoWait: Query[T] =
            val newTree = query.tree match
                case s: SqlQuery.Select => s.copy(lock = Some(SqlLock.Share(Some(SqlLockWaitMode.NoWait))))
                case s: SqlQuery.Set => s.copy(lock = Some(SqlLock.Share(Some(SqlLockWaitMode.NoWait))))
                case c: SqlQuery.Cte => c.copy(lock = Some(SqlLock.Share(Some(SqlLockWaitMode.NoWait))))
                case v: SqlQuery.Values => v.copy(lock = Some(SqlLock.Share(Some(SqlLockWaitMode.NoWait))))
            Query(query.params, newTree)(using query.context)

        def forShareSkipLocked: Query[T] =
            val newTree = query.tree match
                case s: SqlQuery.Select => s.copy(lock = Some(SqlLock.Share(Some(SqlLockWaitMode.SkipLocked))))
                case s: SqlQuery.Set => s.copy(lock = Some(SqlLock.Share(Some(SqlLockWaitMode.SkipLocked))))
                case c: SqlQuery.Cte => c.copy(lock = Some(SqlLock.Share(Some(SqlLockWaitMode.SkipLocked))))
                case v: SqlQuery.Values => v.copy(lock = Some(SqlLock.Share(Some(SqlLockWaitMode.SkipLocked))))
            Query(query.params, newTree)(using query.context)

        private[sqala] def size: Query[Expr[Long]] =
            given QueryContext = query.context
            val expr = count()
            val tree = query.tree
            tree match
                case s@SqlQuery.Select(p, _, _, _, None, _, _, _, _, _) 
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
                            removeLimitAndOrderBy(tree), 
                            false, 
                            Some(SqlTableAlias("t", Nil)),
                            None
                        ) :: Nil,
                        None,
                        None,
                        None,
                        Nil,
                        Nil,
                        None,
                        None
                    )
                    Query(expr, outerQuery)

        private[sqala] def exists: Query[Expr[Boolean]] =
            given QueryContext = query.context
            val expr = Expr[Boolean](SqlExpr.SubLink(SqlSubLinkQuantifier.Exists, query.tree))
            val outerQuery: SqlQuery.Select = SqlQuery.Select(
                None,
                SqlSelectItem.Expr(expr.asSqlExpr, None) :: Nil,
                Nil,
                None,
                None,
                None,
                Nil,
                Nil,
                None,
                None
            )
            Query(expr, outerQuery)

final class SelectQuery[T](
    private[sqala] override val params: T,
    override val tree: SqlQuery.Select
)(using 
    private[sqala] override val context: QueryContext
) extends Query[T](params, tree)

object SelectQuery:
    extension [T](query: SelectQuery[T])
        def filter[F: AsExpr as a](f: T => F)(using SqlBoolean[a.R]): SelectQuery[T] =
            val cond = a.asExpr(f(query.params))
            SelectQuery(query.params, query.tree.addWhere(cond.asSqlExpr))(using query.context)

        def where[F: AsExpr as a](f: T => F)(using SqlBoolean[a.R]): SelectQuery[T] =
            filter(f)

        def filterIf[F: AsExpr as a](test: => Boolean)(f: T => F)(using SqlBoolean[a.R]): SelectQuery[T] =
            if test then filter(f) else query

        def whereIf[F: AsExpr as a](test: => Boolean)(f: T => F)(using SqlBoolean[a.R]): SelectQuery[T] =
            filterIf(test)(f)

        def withFilter[F: AsExpr as a](f: T => F)(using SqlBoolean[a.R]): SelectQuery[T] =
            filter(f)

        def sortBy[S: AsSort as s](f: T => S): SortedQuery[T] =
            val sort = s.asSort(f(query.params))
            SortedQuery(
                query.params, 
                query.tree.copy(orderBy = query.tree.orderBy ++ sort.map(_.asSqlOrderBy))
            )(using query.context)

        def orderBy[S: AsSort](f: T => S): SortedQuery[T] =
            sortBy(f)

        def map[M: AsMap as m](f: T => M): Query[m.R] =
            val mapped = f(query.params)
            val sqlSelect = m.selectItems(mapped, 1)
            Query(
                m.transform(mapped), 
                query.tree.copy(select = sqlSelect)
            )(using query.context)

        def select[M: AsMap as m](f: T => M): Query[m.R] =
            map(f)

        def mapDistinct[M: AsMap as m](f: T => M): Query[m.R] =
            val mapped = f(query.params)
            val sqlSelect = m.selectItems(mapped, 1)
            Query(
                m.transform(mapped), 
                query.tree.copy(quantifier = Some(SqlQuantifier.Distinct), select = sqlSelect)
            )(using query.context)

        def selectDistinct[M: AsMap as m](f: T => M): Query[m.R] =
            mapDistinct(f)

        def groupBy[G: AsGroup as a](f: T => G)(using
            au: AsUngroupedTable[T], 
            t: ToTuple[au.R]
        ): Grouping[G *: t.R] =
            val group = f(query.params)
            val groupExprs = a.exprs(group)
            Grouping(
                group *: t.toTuple(au.asUngroupedTable(query.params)),
                query.tree.copy(
                    groupBy = Some(
                        SqlGroupBy(None, groupExprs.map(g => SqlGroupingItem.Expr(g.asSqlExpr)))
                    )
                )
            )(using query.context)

        def groupByCube[G: AsGroup as a](f: T => G)(using
            to: ToOption[G],
            tot: ToOption[T],
            au: AsUngroupedTable[tot.R],
            tt: ToTuple[au.R]
        ): Grouping[to.R *: tt.R] =
            val group = f(query.params)
            val groupExprs = a.exprs(group)
            Grouping(
                to.toOption(group) *: tt.toTuple(au.asUngroupedTable(tot.toOption(query.params))),
                query.tree.copy(
                    groupBy = Some(
                        SqlGroupBy(None, SqlGroupingItem.Cube(groupExprs.map(_.asSqlExpr)) :: Nil)
                    )
                )
            )(using query.context)

        def groupByRollup[G: AsGroup as a](f: T => G)(using
            to: ToOption[G],
            tot: ToOption[T],
            au: AsUngroupedTable[tot.R],
            tt: ToTuple[au.R]
        ): Grouping[to.R *: tt.R] =
            val group = f(query.params)
            val groupExprs = a.exprs(group)
            Grouping(
                to.toOption(group) *: tt.toTuple(au.asUngroupedTable(tot.toOption(query.params))),
                query.tree.copy(
                    groupBy = Some(
                        SqlGroupBy(None, SqlGroupingItem.Rollup(groupExprs.map(_.asSqlExpr)) :: Nil)
                    )
                )
            )(using query.context)

        def groupBySets[G: AsGroup as a, S: GroupingSets as s](f: T => G)(g: G => S)(using
            to: ToOption[G],
            tot: ToOption[T],
            au: AsUngroupedTable[tot.R],
            tt: ToTuple[au.R]
        ): Grouping[to.R *: tt.R] =
            val group = f(query.params)
            val groupExprs = s.asSqlExprs(g(group))
            Grouping(
                to.toOption(group) *: tt.toTuple(au.asUngroupedTable(tot.toOption(query.params))),
                query.tree.copy(
                    groupBy = Some(
                        SqlGroupBy(None, SqlGroupingItem.GroupingSets(groupExprs) :: Nil)
                    )
                )
            )(using query.context)

    extension [T](query: SelectQuery[Table[T]])
        def connectBy[F: AsExpr as a](f: (Table[T], Table[T]) => F)(using 
            SqlBoolean[a.R]
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
                        SqlTable.Standard(tableCte, None, None, None, None),
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

final class SortedQuery[T](
    private[sqala] override val params: T,
    override val tree: SqlQuery.Select
)(using 
    private[sqala] override val context: QueryContext
) extends Query[T](params, tree)

object SortedQuery:
    extension [T](query: SortedQuery[T])
        def sortBy[S: AsSort as s](f: T => S): SortedQuery[T] =
            val sort = s.asSort(f(query.params))
            SortedQuery(
                query.params, 
                query.tree.copy(orderBy = query.tree.orderBy ++ sort.map(_.asSqlOrderBy))
            )(using query.context)

        def orderBy[S: AsSort](f: T => S): SortedQuery[T] =
            sortBy(f)

class GroupingContext

final class Grouping[T](
    private[sqala] val params: T,
    private[sqala] val tree: SqlQuery.Select
)(using 
    private[sqala] val context: QueryContext
)

object Grouping:
    given GroupingContext = new GroupingContext

    extension [T](query: Grouping[T])
        def having[F: AsExpr as a](f: GroupingContext ?=> T => F)(using SqlBoolean[a.R]): Grouping[T] =
            val cond = a.asExpr(f(query.params))
            Grouping(query.params, query.tree.addHaving(cond.asSqlExpr))(using query.context)

        def sortBy[S: AsSort as s](f: GroupingContext ?=> T => S): Grouping[T] =
            val sort = s.asSort(f(query.params))
            Grouping(
                query.params, 
                query.tree.copy(orderBy = query.tree.orderBy ++ sort.map(_.asSqlOrderBy))
            )(using query.context)

        def orderBy[S: AsSort](f: GroupingContext ?=> T => S): Grouping[T] =
            sortBy(f)

        def map[M: AsMap as m](f: GroupingContext ?=> T => M): Query[m.R] =
            val mapped = f(query.params)
            val sqlSelect = m.selectItems(mapped, 1)
            Query(
                m.transform(mapped), 
                query.tree.copy(select = sqlSelect)
            )(using query.context)

        def select[M: AsMap as m](f: GroupingContext ?=> T => M): Query[m.R] =
            map(f)

        def mapDistinct[M: AsMap as m](f: GroupingContext ?=> T => M): Query[m.R] =
            val mapped = f(query.params)
            val sqlSelect = m.selectItems(mapped, 1)
            Query(
                m.transform(mapped), 
                query.tree.copy(select = sqlSelect)
            )(using query.context)

        def selectDistinct[M: AsMap as m](f: GroupingContext ?=> T => M): Query[m.R] =
            mapDistinct(f)

final class UnionQuery[T](
    private[sqala] override val params: T,
    override val tree: SqlQuery.Set
)(using QueryContext) extends Query[T](params, tree)

object UnionQuery:
    extension [T](query: UnionQuery[T])
        def sortBy[S: AsSort as s](f: T => S): UnionQuery[T] =
            val sort = s.asSort(f(query.params))
            UnionQuery(
                query.params, 
                query.tree.copy(orderBy = query.tree.orderBy ++ sort.map(_.asSqlOrderBy))
            )(using query.context)

        def orderBy[S: AsSort](f: T => S): UnionQuery[T] =
            sortBy(f)

class ConnectByContext

final case class ConnectBy[T](
    private[sqala] val table: Table[T],
    private[sqala] val connectByTree: SqlQuery.Select,
    private[sqala] val startWithTree: SqlQuery.Select,
    private[sqala] val mapTree: SqlQuery.Select =
        SqlQuery.Select(
            None,
            Nil, 
            SqlTable.Standard(tableCte, None, None, None, None) :: Nil,
            None,
            None,
            None,
            Nil,
            Nil,
            None,
            None
        )
)(using private[sqala] val context: QueryContext)

object ConnectBy:
    given ConnectByContext = new ConnectByContext
    
    extension [T](query: ConnectBy[T])
        def startWith[F: AsExpr as a](f: Table[T] => F)(using SqlBoolean[a.R]): ConnectBy[T] =
            val cond = a.asExpr(f(query.table))
            query.copy(
                startWithTree = query.startWithTree.addWhere(cond.asSqlExpr)
            )(using query.context)

        def sortBy[S: AsSort as s](f: ConnectByContext ?=> Table[T] => S): ConnectBy[T] =
            val sort = f(query.table.copy(__aliasName__ = Some(tableCte)))
            val sqlOrderBy = s.asSort(sort).map(_.asSqlOrderBy)
            query.copy(
                mapTree = query.mapTree.copy(orderBy = query.mapTree.orderBy ++ sqlOrderBy)
            )(using query.context)

        def orderBy[S: AsSort as s](f: ConnectByContext ?=> Table[T] => S): ConnectBy[T] =
            sortBy(f)

        def maxDepth(n: Int): ConnectBy[T] =
            val cond = SqlExpr.Binary(
                SqlExpr.Column(Some(tableCte), columnPseudoLevel),
                SqlBinaryOperator.LessThan,
                SqlExpr.NumberLiteral(n)
            )
            query.copy(
                connectByTree = query.connectByTree.addWhere(cond)
            )(using query.context)

        def sortSiblingsBy[S: AsSort as s](f: ConnectByContext ?=> Table[T] => S): ConnectBy[T] =
            val sort = f(query.table)
            val sqlOrderBy = s.asSort(sort).map(_.asSqlOrderBy)
            query.copy(
                connectByTree = query.connectByTree.copy(orderBy = query.connectByTree.orderBy ++ sqlOrderBy)
            )(using query.context)

        def orderSiblingsBy[S: AsSort as s](f: ConnectByContext ?=> Table[T] => S): ConnectBy[T] =
            sortSiblingsBy(f)

        def map[M: AsMap as m](f: ConnectByContext ?=> Table[T] => M): Query[m.R] =
            val mapped = f(
                Table[T](
                    Some(tableCte), 
                    query.table.__metaData__, 
                    query.table.__sqlTable__.copy(
                        alias = query.table.__sqlTable__.alias.map(_.copy(tableAlias = tableCte))
                    )
                )
            )
            val sqlSelect = m.selectItems(mapped, 1)
            val metaData = query.table.__metaData__
            val unionQuery = SqlQuery.Set(
                query.startWithTree, 
                SqlSetOperator.Union(Some(SqlQuantifier.All)), 
                query.connectByTree,
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
                query.mapTree.copy(select = sqlSelect),
                None
            )
            Query(m.transform(mapped), cteTree)(using query.context)

        def select[M: AsMap as m](f: ConnectByContext ?=> Table[T] => M): Query[m.R] =
            map(f)

        def mapDistinct[M: AsMap as m](f: ConnectByContext ?=> Table[T] => M): Query[m.R] =
            val mapped = f(
                Table[T](
                    Some(tableCte), 
                    query.table.__metaData__, 
                    query.table.__sqlTable__.copy(
                        alias = query.table.__sqlTable__.alias.map(_.copy(tableAlias = tableCte))
                    )
                )
            )
            val sqlSelect = m.selectItems(mapped, 1)
            val metaData = query.table.__metaData__
            val unionQuery = SqlQuery.Set(
                query.startWithTree, 
                SqlSetOperator.Union(Some(SqlQuantifier.All)), 
                query.connectByTree,
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
                query.mapTree.copy(select = sqlSelect),
                None
            )
            Query(m.transform(mapped), cteTree)(using query.context)

        def selectDistinct[M: AsMap as m](f: ConnectByContext ?=> Table[T] => M): Query[m.R] =
            mapDistinct(f)