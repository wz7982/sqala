package sqala.static.dsl.statement.query

import sqala.ast.expr.{SqlBinaryOperator, SqlExpr, SqlSubLinkQuantifier}
import sqala.ast.group.{SqlGroupBy, SqlGroupingItem}
import sqala.ast.limit.SqlLimit
import sqala.ast.quantifier.SqlQuantifier
import sqala.ast.statement.{SqlQuery, SqlSelectItem, SqlSetOperator, SqlWithItem}
import sqala.ast.table.{SqlJoinCondition, SqlJoinType, SqlTable, SqlTableAlias}
import sqala.metadata.TableMacro
import sqala.printer.Dialect
import sqala.static.dsl.*
import sqala.util.queryToString

import scala.NamedTuple.NamedTuple
import scala.Tuple.Append

sealed class Query[T](
    private[sqala] val params: T,
    val tree: SqlQuery
)(using 
    private[sqala] val context: QueryContext
)

object Query:
    extension [T](query: Query[T])
        def sql(dialect: Dialect): (String, Array[Any]) =
            queryToString(query.tree, dialect, true)

        infix def union[R](unionQuery: Query[R])(using u: Union[T, R]): Query[u.R] =
            UnionQuery(
                u.unionQueryItems(query.params), 
                query.tree, 
                SqlSetOperator.Union(None), 
                unionQuery.tree
            )(using query.context)

        infix def unionAll[R](unionQuery: Query[R])(using u: Union[T, R]): Query[u.R] =
            UnionQuery(
                u.unionQueryItems(query.params), 
                query.tree, 
                SqlSetOperator.Union(Some(SqlQuantifier.All)), 
                unionQuery.tree
            )(using query.context)

        infix def except[R](unionQuery: Query[R])(using u: Union[T, R]): Query[u.R] =
            UnionQuery(
                u.unionQueryItems(query.params), 
                query.tree, 
                SqlSetOperator.Except(None), 
                unionQuery.tree
            )(using query.context)

        infix def exceptAll[R](unionQuery: Query[R])(using u: Union[T, R]): Query[u.R] =
            UnionQuery(
                u.unionQueryItems(query.params), 
                query.tree, 
                SqlSetOperator.Except(Some(SqlQuantifier.All)), 
                unionQuery.tree
            )(using query.context)

        infix def intersect[R](unionQuery: Query[R])(using u: Union[T, R]): Query[u.R] =
            UnionQuery(
                u.unionQueryItems(query.params), 
                query.tree, 
                SqlSetOperator.Intersect(None), 
                unionQuery.tree
            )(using query.context)

        infix def intersectAll[R](unionQuery: Query[R])(using u: Union[T, R]): Query[u.R] =
            UnionQuery(
                u.unionQueryItems(query.params), 
                query.tree, 
                SqlSetOperator.Intersect(Some(SqlQuantifier.All)), 
                unionQuery.tree
            )(using query.context)

        def drop(n: Int): Query[T] =
            val limit = query.tree match
                case s: SqlQuery.Select => s.limit
                case s: SqlQuery.Set => s.limit
                case SqlQuery.Cte(_, _, s: SqlQuery.Select) => s.limit
                case SqlQuery.Cte(_, _, s: SqlQuery.Set) => s.limit
                case _ => None
            val sqlLimit = limit
                .map(l => SqlLimit(l.limit, SqlExpr.NumberLiteral(n)))
                .orElse(Some(SqlLimit(SqlExpr.NumberLiteral(Long.MaxValue), SqlExpr.NumberLiteral(n))))
            val newTree = query.tree match
                case s: SqlQuery.Select => s.copy(limit = sqlLimit)
                case s: SqlQuery.Set => s.copy(limit = sqlLimit)
                case SqlQuery.Cte(w, r, s: SqlQuery.Select) =>
                    SqlQuery.Cte(w, r, s.copy(limit = sqlLimit))
                case SqlQuery.Cte(w, r, s: SqlQuery.Set) =>
                    SqlQuery.Cte(w, r, s.copy(limit = sqlLimit))
                case _ => query.tree
            Query(query.params, newTree)(using query.context)

        def offset(n: Int): Query[T] = drop(n)

        def take(n: Int): Query[T] =
            val limit = query.tree match
                case s: SqlQuery.Select => s.limit
                case s: SqlQuery.Set => s.limit
                case SqlQuery.Cte(_, _, s: SqlQuery.Select) => s.limit
                case SqlQuery.Cte(_, _, s: SqlQuery.Set) => s.limit
                case _ => None
            val sqlLimit = limit
                .map(l => SqlLimit(SqlExpr.NumberLiteral(n), l.offset))
                .orElse(Some(SqlLimit(SqlExpr.NumberLiteral(n), SqlExpr.NumberLiteral(0))))
            val newTree = query.tree match
                case s: SqlQuery.Select => s.copy(limit = sqlLimit)
                case s: SqlQuery.Set => s.copy(limit = sqlLimit)
                case SqlQuery.Cte(w, r, s: SqlQuery.Select) =>
                    SqlQuery.Cte(w, r, s.copy(limit = sqlLimit))
                case SqlQuery.Cte(w, r, s: SqlQuery.Set) =>
                    SqlQuery.Cte(w, r, s.copy(limit = sqlLimit))
                case _ => query.tree
            Query(query.params, newTree)(using query.context)

        def limit(n: Int): Query[T] = take(n)

        private[sqala] def size: Query[Expr[Long]] =
            given QueryContext = query.context
            val expr = count()
            val tree = query.tree
            tree match
                case s@SqlQuery.Select(p, _, _, _, None, _, _, _) 
                    if p != Some(SqlQuantifier.Distinct)
                =>
                    Query(
                        expr, 
                        s.copy(
                            select = SqlSelectItem.Item(expr.asSqlExpr, None) :: Nil, 
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
                        select = SqlSelectItem.Item(expr.asSqlExpr, None) :: Nil,
                        from = SqlTable.SubQuery(removeLimitAndOrderBy(tree), false, Some(SqlTableAlias("t"))) :: Nil
                    )
                    Query(expr, outerQuery)

        private[sqala] def exists: Query[Expr[Boolean]] =
            given QueryContext = query.context
            val expr = Expr[Boolean](SqlExpr.SubLink(query.tree, SqlSubLinkQuantifier.Exists))
            val outerQuery: SqlQuery.Select = SqlQuery.Select(
                select = SqlSelectItem.Item(expr.asSqlExpr, None) :: Nil,
                from = Nil
            )
            Query(expr, outerQuery)

final class ProjectionQuery[T](
    private[sqala] override val params: T,
    override val tree: SqlQuery.Select
)(using 
    private[sqala] override val context: QueryContext
) extends Query[T](params, tree)

object ProjectionQuery:
    extension [T](query: ProjectionQuery[T])
        def distinct: Query[T] =
            val newTree = query.tree.copy(quantifier = Some(SqlQuantifier.Distinct))
            Query(query.params, newTree)(using query.context)

final class ConnectByProjectionQuery[T](
    private[sqala] override val params: T,
    override val tree: SqlQuery.Cte
)(using 
    private[sqala] override val context: QueryContext
) extends Query[T](params, tree)

object ConnectByProjectionQuery:
    extension [T](query: ConnectByProjectionQuery[T])
        def distinct: Query[T] =
            val finalTree = query.tree.query.asInstanceOf[SqlQuery.Select]
            val newTree = finalTree.copy(quantifier = Some(SqlQuantifier.Distinct))
            val cteTree = query.tree.copy(query = newTree)
            Query(query.params, cteTree)(using query.context)

sealed class SelectQuery[T](
    private[sqala] override val params: T,
    override val tree: SqlQuery.Select
)(using 
    private[sqala] override val context: QueryContext
) extends Query[T](params, tree)

object SelectQuery:
    extension [T](query: SelectQuery[T])
        def filter[F: AsExpr as a](f: T => F)(using a.R <:< (Boolean | Option[Boolean])): SelectQuery[T] =
            val cond = a.asExpr(f(query.params))
            SelectQuery(query.params, query.tree.addWhere(cond.asSqlExpr))(using query.context)

        def where[F: AsExpr as a](f: T => F)(using a.R <:< (Boolean | Option[Boolean])): SelectQuery[T] =
            filter(f)

        def withFilter[F: AsExpr as a](f: T => F)(using a.R <:< (Boolean | Option[Boolean])): SelectQuery[T] =
            filter(f)

        def filterIf[F: AsExpr as a](test: => Boolean)(f: T => F)(using 
            a.R <:< (Boolean | Option[Boolean])
        ): SelectQuery[T] =
            if test then filter(f) else query

        def whereIf[F: AsExpr as a](test: => Boolean)(f: T => F)(using 
            a.R <:< (Boolean | Option[Boolean])
        ): SelectQuery[T] =
            if test then filter(f) else query

        def sortBy[S: AsSort as s](f: T => S): SelectQuery[T] =
            val sort = s.asSort(f(query.params))
            SelectQuery(
                query.params, 
                query.tree.copy(orderBy = query.tree.orderBy ++ sort.map(_.asSqlOrderBy))
            )(using query.context)

        def orderBy[S: AsSort](f: T => S): SelectQuery[T] =
            sortBy(f)

        def sortByIf[S: AsSort as s](test: => Boolean)(f: T => S): SelectQuery[T] =
            if test then sortBy(f) else query

        def orderByIf[S: AsSort as s](test: => Boolean)(f: T => S): SelectQuery[T] =
            if test then sortBy(f) else query

        def map[M: AsMap as m](f: T => M): ProjectionQuery[m.R] =
            val mapped = f(query.params)
            val sqlSelect = m.selectItems(mapped, 1)
            ProjectionQuery(
                m.transform(mapped), 
                query.tree.copy(select = sqlSelect)
            )(using query.context)

        def select[M: AsMap as m](f: T => M): ProjectionQuery[m.R] =
            map(f)

        def groupBy[G: AsGroup as a](f: T => G): Grouping[T] =
            val group = a.exprs(f(query.params))
            Grouping(
                query.params, 
                query.tree.copy(groupBy = Some(SqlGroupBy(group.map(g => SqlGroupingItem.Expr(g.asSqlExpr)), None)))
            )(using query.context)

        def groupByCube[G](f: T => G)(using
            o: ToOption[T],
            a: AsGroup[G]
        ): Grouping[o.R] =
            val group = a.exprs(f(query.params))
            Grouping(
                o.toOption(query.params), 
                query.tree.copy(groupBy = Some(SqlGroupBy(SqlGroupingItem.Cube(group.map(_.asSqlExpr)) :: Nil, None)))
            )(using query.context)

        def groupByRollup[G](f: T => G)(using
            o: ToOption[T],
            a: AsGroup[G]
        ): Grouping[o.R] =
            val group = a.exprs(f(query.params))
            Grouping(
                o.toOption(query.params), 
                query.tree.copy(groupBy = Some(SqlGroupBy(SqlGroupingItem.Rollup(group.map(_.asSqlExpr)) :: Nil, None)))
            )(using query.context)

        def groupBySets[G](f: T => G)(using
            o: ToOption[T],
            g: GroupingSets[G]
        ): Grouping[o.R] =
            val group = g.asSqlExprs(f(query.params))
            Grouping(
                o.toOption(query.params), 
                query.tree.copy(groupBy = Some(SqlGroupBy(SqlGroupingItem.GroupingSets(group) :: Nil, None)))
            )(using query.context)

    extension [T](query: SelectQuery[Table[T]])
        def connectBy[F: AsExpr as a](f: Table[T] => F)(using a.R <:< (Boolean | Option[Boolean])): ConnectBy[T] =
            given QueryContext = query.context
            val cond = a.asExpr(f(query.params)).asSqlExpr
            val joinTree = query.tree
                .copy(
                    select = query.tree.select :+ SqlSelectItem.Item(
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
                        SqlTable.Range(tableCte, None),
                        Some(SqlJoinCondition.On(cond)),
                        None
                    ) :: Nil
                )
            val startTree = query.tree
                .copy(
                    select = query.tree.select :+ SqlSelectItem.Item(
                        SqlExpr.NumberLiteral(1),
                        Some(columnPseudoLevel)
                    )
                )
            ConnectBy(query.params, joinTree, startTree)

sealed class JoinQuery[T](
    private[sqala] override val params: T,
    override val tree: SqlQuery.Select
)(using 
    private[sqala] override val context: QueryContext
) extends SelectQuery[T](params, tree)

object JoinQuery:
    extension [T](query: JoinQuery[T])
        private inline def joinClause[J, R](
            joinType: SqlJoinType,
            f: Table[J] => R
        )(using
            s: AsSelect[R]
        ): JoinPart[R] =
            val joinTableName = TableMacro.tableName[J]
            query.context.tableIndex += 1
            val joinAliasName = s"t${query.context.tableIndex}"
            val joinMetaData = TableMacro.tableMetaData[J]
            val joinTable = Table[J](joinTableName, joinAliasName, joinMetaData)
            val params = f(joinTable)
            val sqlTable: SqlTable.Join = SqlTable.Join(
                query.tree.from.head,
                joinType,
                SqlTable.Range(joinTableName, Some(SqlTableAlias(joinAliasName))),
                None,
                None
            )
            val tree = SqlQuery.Select(
                select = s.selectItems(params, 1),
                from = sqlTable :: Nil
            )
            JoinPart(params, sqlTable, tree)(using query.context)

        private inline def joinQueryClause[N <: Tuple, V <: Tuple, SV <: Tuple, R](
            joinType: SqlJoinType,
            joinQuery: Query[NamedTuple[N, V]],
            f: SubQuery[N, SV] => R,
            vf: V => SV,
            lateral: Boolean
        )(using
            s: AsSelect[R],
            sq: AsSubQuery[SV]
        ): JoinPart[R] =
            given QueryContext = query.context
            val rightTable = SubQuery[N, SV](vf(joinQuery.params))
            val params = f(rightTable)
            val sqlTable: SqlTable.Join = SqlTable.Join(
                query.tree.from.head,
                joinType,
                SqlTable.SubQuery(joinQuery.tree, lateral, Some(SqlTableAlias(rightTable.__alias__))),
                None,
                None
            )
            val tree = SqlQuery.Select(
                select = s.selectItems(params, 1),
                from = sqlTable :: Nil
            )
            JoinPart(params, sqlTable, tree)

        inline def join[J](using 
            tt: ToTuple[T],
            s: AsSelect[Append[tt.R, Table[J]]]
        ): JoinPart[Append[tt.R, Table[J]]] =
            joinClause[J, Append[tt.R, Table[J]]](
                SqlJoinType.Inner,
                j => tt.toTuple(query.params) :* j
            )

        inline def join[N <: Tuple, V <: Tuple](
            joinQuery: Query[NamedTuple[N, V]]
        )(using 
            tt: ToTuple[T],
            s: AsSelect[Append[tt.R, SubQuery[N, V]]],
            sq: AsSubQuery[V]
        ): JoinPart[Append[tt.R, SubQuery[N, V]]] =
            joinQueryClause[N, V, V, Append[tt.R, SubQuery[N, V]]](
                SqlJoinType.Inner,
                joinQuery,
                j => tt.toTuple(query.params) :* j,
                v => v,
                false
            )

        inline def joinLateral[N <: Tuple, V <: Tuple](
            joinQuery: T => Query[NamedTuple[N, V]]
        )(using 
            tt: ToTuple[T],
            s: AsSelect[Append[tt.R, SubQuery[N, V]]],
            sq: AsSubQuery[V]
        ): JoinPart[Append[tt.R, SubQuery[N, V]]] =
            joinQueryClause[N, V, V, Append[tt.R, SubQuery[N, V]]](
                SqlJoinType.Inner,
                joinQuery(query.params),
                j => tt.toTuple(query.params) :* j,
                v => v,
                true
            )

        inline def leftJoin[J](using 
            o: ToOption[Table[J]], 
            tt: ToTuple[T],
            s: AsSelect[Append[tt.R, o.R]]
        ): JoinPart[Append[tt.R, o.R]] =
            joinClause[J, Append[tt.R, o.R]](
                SqlJoinType.Left,
                j => tt.toTuple(query.params) :* o.toOption(j)
            )

        inline def leftJoin[N <: Tuple, V <: Tuple](
            joinQuery: Query[NamedTuple[N, V]]
        )(using 
            o: ToOption[V], 
            tt: ToTuple[T], 
            to: ToTuple[o.R],
            s: AsSelect[Append[tt.R, SubQuery[N, to.R]]],
            sq: AsSubQuery[to.R]
        ): JoinPart[Append[tt.R, SubQuery[N, to.R]]] =
            joinQueryClause[N, V, to.R, Append[tt.R, SubQuery[N, to.R]]](
                SqlJoinType.Left,
                joinQuery,
                j => tt.toTuple(query.params) :* j,
                v => to.toTuple(o.toOption(v)),
                false
            )

        inline def leftJoinLateral[N <: Tuple, V <: Tuple](
            joinQuery: T => Query[NamedTuple[N, V]]
        )(using 
            o: ToOption[V], 
            tt: ToTuple[T], 
            to: ToTuple[o.R],
            s: AsSelect[Append[tt.R, SubQuery[N, to.R]]],
            sq: AsSubQuery[to.R]
        ): JoinPart[Append[tt.R, SubQuery[N, to.R]]] =
            joinQueryClause[N, V, to.R, Append[tt.R, SubQuery[N, to.R]]](
                SqlJoinType.Left,
                joinQuery(query.params),
                j => tt.toTuple(query.params) :* j,
                v => to.toTuple(o.toOption(v)),
                true
            )

        inline def rightJoin[J](using 
            o: ToOption[T], 
            to: ToTuple[o.R],
            s: AsSelect[Append[to.R, Table[J]]]
        ): JoinPart[Append[to.R, Table[J]]] =
            joinClause[J, Append[to.R, Table[J]]](
                SqlJoinType.Right,
                j => to.toTuple(o.toOption(query.params)) :* j
            )

        inline def rightJoin[N <: Tuple, V <: Tuple](
            joinQuery: Query[NamedTuple[N, V]]
        )(using 
            o: ToOption[T], 
            to: ToTuple[o.R],
            s: AsSelect[Append[to.R, SubQuery[N, V]]],
            sq: AsSubQuery[V]
        ): JoinPart[Append[to.R, SubQuery[N, V]]] =
            joinQueryClause[N, V, V, Append[to.R, SubQuery[N, V]]](
                SqlJoinType.Right,
                joinQuery,
                j => to.toTuple(o.toOption(query.params)) :* j,
                v => v,
                false
            )

sealed class TableQuery[T](
    private[sqala] override val params: Table[T],
    override val tree: SqlQuery.Select
)(using 
    private[sqala] override val context: QueryContext
) extends JoinQuery[Table[T]](params, tree)

class UnionQuery[T](
    private[sqala] override val params: T,
    private[sqala] val left: SqlQuery,
    private[sqala] val operator: SqlSetOperator,
    private[sqala] val right: SqlQuery
)(using QueryContext) extends Query[T](
    params,
    SqlQuery.Set(left, operator, right)
)

final class JoinPart[T](
    private[sqala] val params: T,
    private[sqala] val joinTable: SqlTable.Join,
    private[sqala] val tree: SqlQuery.Select
)(using 
    private[sqala] val context: QueryContext
)

object JoinPart:
    extension [T](query: JoinPart[T])
        def on[F: AsExpr as a](f: T => F)(using a.R <:< (Boolean | Option[Boolean])): JoinQuery[T] =
            val cond = a.asExpr(f(query.params))
            val newTable = query.joinTable.copy(condition = Some(SqlJoinCondition.On(cond.asSqlExpr)))
            JoinQuery(query.params, query.tree.copy(from = newTable :: Nil))(using query.context)

final class Grouping[T](
    private[sqala] val params: T,
    private[sqala] val tree: SqlQuery.Select
)(using 
    private[sqala] val context: QueryContext
)

object Grouping:
    extension [T](query: Grouping[T])
        def having[F: AsExpr as a](f: T => F)(using a.R <:< (Boolean | Option[Boolean])): Grouping[T] =
            val cond = a.asExpr(f(query.params))
            Grouping(query.params, query.tree.addHaving(cond.asSqlExpr))(using query.context)

        def sortBy[S: AsSort as s](f: T => S): Grouping[T] =
            val sort = s.asSort(f(query.params))
            Grouping(
                query.params, 
                query.tree.copy(orderBy = query.tree.orderBy ++ sort.map(_.asSqlOrderBy))
            )(using query.context)

        def orderBy[S: AsSort](f: T => S): Grouping[T] =
            sortBy(f)

        def map[M: AsMap as m](f: T => M): ProjectionQuery[m.R] =
            val mapped = f(query.params)
            val sqlSelect = m.selectItems(mapped, 1)
            ProjectionQuery(
                m.transform(mapped), 
                query.tree.copy(select = sqlSelect)
            )(using query.context)

        def select[M: AsMap as m](f: T => M): ProjectionQuery[m.R] =
            map(f)

final case class ConnectBy[T](
    private[sqala] val table: Table[T],
    private[sqala] val connectByTree: SqlQuery.Select,
    private[sqala] val startWithTree: SqlQuery.Select,
    private[sqala] val mapTree: SqlQuery.Select =
        SqlQuery.Select(select = Nil, from = SqlTable.Range(tableCte, None) :: Nil)
)(using private[sqala] val context: QueryContext)

object ConnectBy:
    extension [T](query: ConnectBy[T])
        def startWith[F: AsExpr as a](f: Table[T] => F)(using a.R <:< (Boolean | Option[Boolean])): ConnectBy[T] =
            val cond = a.asExpr(f(query.table))
            query.copy(
                startWithTree = query.startWithTree.addWhere(cond.asSqlExpr)
            )(using query.context)

        def sortBy[S: AsSort as s](f: Table[T] => S): ConnectBy[T] =
            val sort = f(query.table.copy(__aliasName__ = tableCte))
            val sqlOrderBy = s.asSort(sort).map(_.asSqlOrderBy)
            query.copy(
                mapTree = query.mapTree.copy(orderBy = query.mapTree.orderBy ++ sqlOrderBy)
            )(using query.context)

        def orderBy[S: AsSort as s](f: Table[T] => S): ConnectBy[T] =
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

        def sortSiblingsBy[S: AsSort as s](f: Table[T] => S): ConnectBy[T] =
            val sort = f(query.table)
            val sqlOrderBy = s.asSort(sort).map(_.asSqlOrderBy)
            query.copy(
                connectByTree = query.connectByTree.copy(orderBy = query.connectByTree.orderBy ++ sqlOrderBy)
            )(using query.context)

        def orderSiblingsBy[S: AsSort as s](f: Table[T] => S): ConnectBy[T] =
            sortSiblingsBy(f)

        def map[M: AsMap as m](f: Table[T] => M): ConnectByProjectionQuery[m.R] =
            val mapped = f(Table[T](query.table.__tableName__, tableCte, query.table.__metaData__))
            val sqlSelect = m.selectItems(mapped, 1)
            val metaData = query.table.__metaData__
            val unionQuery = SqlQuery.Set(query.startWithTree, SqlSetOperator.Union(Some(SqlQuantifier.All)), query.connectByTree)
            val withItem = SqlWithItem(tableCte, unionQuery, metaData.columnNames :+ columnPseudoLevel)
            val cteTree: SqlQuery.Cte = SqlQuery.Cte(withItem :: Nil, true, query.mapTree.copy(select = sqlSelect))
            ConnectByProjectionQuery(m.transform(mapped), cteTree)(using query.context)

        def select[M: AsMap as m](f: Table[T] => M): ConnectByProjectionQuery[m.R] =
            map(f)