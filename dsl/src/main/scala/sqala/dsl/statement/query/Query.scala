package sqala.dsl.statement.query

import sqala.ast.expr.SqlExpr
import sqala.ast.group.SqlGroupItem
import sqala.ast.limit.SqlLimit
import sqala.ast.order.SqlOrderBy
import sqala.ast.statement.{SqlQuery, SqlSelectItem, SqlSelectParam, SqlUnionType}
import sqala.ast.table.{SqlJoinType, SqlTableAlias, SqlTable}
import sqala.dsl.*
import sqala.dsl.macros.TableMacro
import sqala.printer.Dialect
import sqala.util.queryToString

import scala.NamedTuple.*
import scala.compiletime.{erasedValue, error}
import scala.deriving.Mirror

sealed class Query[T, S <: ResultSize](private[sqala] val queryItems: T, val ast: SqlQuery):
    def sql(dialect: Dialect, prepare: Boolean = true): (String, Array[Any]) =
        queryToString(ast, dialect, prepare)
    
    def drop(n: Int): Query[T, S] =
        val limit = ast match
            case s: SqlQuery.Select => s.limit
            case u: SqlQuery.Union => u.limit
            case _ => None
        val sqlLimit = limit
            .map(l => SqlLimit(l.limit, SqlExpr.NumberLiteral(n)))
            .orElse(Some(SqlLimit(SqlExpr.NumberLiteral(1), SqlExpr.NumberLiteral(n))))
        val newAst = ast match
            case s: SqlQuery.Select => s.copy(limit = sqlLimit)
            case u: SqlQuery.Union => u.copy(limit = sqlLimit)
            case _ => ast
        Query(queryItems, newAst)

    def take(n: Int): Query[T, QuerySize[n.type]] =
        val limit = ast match
            case s: SqlQuery.Select => s.limit
            case u: SqlQuery.Union => u.limit
            case _ => None
        val sqlLimit = limit
            .map(l => SqlLimit(SqlExpr.NumberLiteral(n), l.offset))
            .orElse(Some(SqlLimit(SqlExpr.NumberLiteral(n), SqlExpr.NumberLiteral(0))))
        val newAst = ast match
            case s: SqlQuery.Select => s.copy(limit = sqlLimit)
            case u: SqlQuery.Union => u.copy(limit = sqlLimit)
            case _ => ast
        Query(queryItems, newAst)

    def size: Query[Expr[Long, ColumnKind], ResultSize.One.type] = 
        val expr = count().asInstanceOf[Expr[Long, ColumnKind]]
        ast match
            case s@SqlQuery.Select(_, _, _, _, Nil, _, _, _, _) =>
                Query(expr, s.copy(select = SqlSelectItem.Item(expr.asSqlExpr, None) :: Nil, limit = None))
            case _ =>
                val outerQuery: SqlQuery.Select = SqlQuery.Select(
                    select = SqlSelectItem.Item(expr.asSqlExpr, None) :: Nil,
                    from = SqlTable.SubQueryTable(ast, false, SqlTableAlias("t")) :: Nil
                )
                Query(expr, outerQuery)

    def exists: Query[Expr[Boolean, ColumnKind], ResultSize.One.type] =
        val expr = sqala.dsl.exists(this).asInstanceOf[Expr[Boolean, ColumnKind]]
        val outerQuery: SqlQuery.Select = SqlQuery.Select(
            select = SqlSelectItem.Item(expr.asSqlExpr, None) :: Nil,
            from = Nil
        )
        Query(expr, outerQuery)

object Query:
    extension [T, K <: ExprKind](query: Query[Expr[T, K], ResultSize.One.type])
        def asExpr: Expr[T, CommonKind] = Expr.SubQuery(query.ast)

    extension [N <: Tuple, V <: Tuple, S <: ResultSize, UN <: Tuple, UV <: Tuple, US <: ResultSize](query: Query[NamedTuple[N, V], S])
        infix def union(unionQuery: Query[NamedTuple[UN, UV], US]): Query[NamedTuple[N, Union[V, UV]], ResultSize.Many.type] =
            UnionQuery(query, SqlUnionType.Union, unionQuery.ast)

        infix def unionAll(unionQuery: Query[NamedTuple[UN, UV], US]): Query[NamedTuple[N, Union[V, UV]], ResultSize.Many.type] =
            UnionQuery(query, SqlUnionType.UnionAll, unionQuery.ast)

        def ++(unionQuery: Query[NamedTuple[UN, UV], US]): Query[NamedTuple[N, Union[V, UV]], ResultSize.Many.type] =
            UnionQuery(query, SqlUnionType.UnionAll, unionQuery.ast)

        infix def except(unionQuery: Query[NamedTuple[UN, UV], US]): Query[NamedTuple[N, Union[V, UV]], ResultSize.Many.type] =
            UnionQuery(query, SqlUnionType.Except, unionQuery.ast)

        infix def exceptAll(unionQuery: Query[NamedTuple[UN, UV], US]): Query[NamedTuple[N, Union[V, UV]], ResultSize.Many.type] =
            UnionQuery(query, SqlUnionType.ExceptAll, unionQuery.ast)

        infix def intersect(unionQuery: Query[NamedTuple[UN, UV], US]): Query[NamedTuple[N, Union[V, UV]], ResultSize.Many.type] =
            UnionQuery(query, SqlUnionType.Intersect, unionQuery.ast)

        infix def intersectAll(unionQuery: Query[NamedTuple[UN, UV], US]): Query[NamedTuple[N, Union[V, UV]], ResultSize.Many.type] =
            UnionQuery(query, SqlUnionType.IntersectAll, unionQuery.ast)

    extension [N <: Tuple, V <: Tuple, S <: ResultSize, L <: Product](query: Query[NamedTuple[N, V], S])
        private inline def unionListClause(unionType: SqlUnionType, list: List[L]): Query[NamedTuple[N, Union[V, Tuple.Map[DropNames[From[L]], [t] =>> Expr[t, ColumnKind]]]], ResultSize.Many.type] =
            val instances = AsSqlExpr.summonInstances[DropNames[From[L]]]
            val values = SqlQuery.Values(
                list.map: v =>
                    val terms = v.productIterator.toList
                    terms.zip(instances).map: (term, instance) =>
                        instance.asInstanceOf[AsSqlExpr[Any]].asSqlExpr(term)
            )
            UnionQuery(query, unionType, values)

        inline infix def union(list: List[L]): Query[NamedTuple[N, Union[V, Tuple.Map[DropNames[From[L]], [t] =>> Expr[t, ColumnKind]]]], ResultSize.Many.type] =
            unionListClause(SqlUnionType.Union, list)

        inline infix def unionAll(list: List[L]): Query[NamedTuple[N, Union[V, Tuple.Map[DropNames[From[L]], [t] =>> Expr[t, ColumnKind]]]], ResultSize.Many.type] =
            unionListClause(SqlUnionType.UnionAll, list)

        inline infix def except(list: List[L]): Query[NamedTuple[N, Union[V, Tuple.Map[DropNames[From[L]], [t] =>> Expr[t, ColumnKind]]]], ResultSize.Many.type] =
            unionListClause(SqlUnionType.Except, list)

        inline infix def exceptAll(list: List[L]): Query[NamedTuple[N, Union[V, Tuple.Map[DropNames[From[L]], [t] =>> Expr[t, ColumnKind]]]], ResultSize.Many.type] =
            unionListClause(SqlUnionType.ExceptAll, list)

        inline infix def intersect(list: List[L]): Query[NamedTuple[N, Union[V, Tuple.Map[DropNames[From[L]], [t] =>> Expr[t, ColumnKind]]]], ResultSize.Many.type] =
            unionListClause(SqlUnionType.Intersect, list)

        inline infix def intersectAll(list: List[L]): Query[NamedTuple[N, Union[V, Tuple.Map[DropNames[From[L]], [t] =>> Expr[t, ColumnKind]]]], ResultSize.Many.type] =
            unionListClause(SqlUnionType.IntersectAll, list)

class ProjectionQuery[T, S <: ResultSize](
    private[sqala] val items: T,
    override val ast: SqlQuery.Select
)(using q: QueryContext) extends Query[T, S](items, ast):
    def distinct(using a: AsExpr[T], c: ChangeKind[T, DistinctKind]): DistinctQuery[c.R, S] =
        DistinctQuery(c.changeKind(items), ast.copy(param = Some(SqlSelectParam.Distinct)))

    inline def sortBy[O, K <: ExprKind](f: T => OrderBy[O, K]): SortQuery[T, S] =
        inline erasedValue[K] match
            case _: AggKind | AggOperationKind | WindowKind =>
                error("Column must appear in the GROUP BY clause or be used in an aggregate function.")
            case _ =>
        val orderBy = f(queryItems)
        val sqlOrderBy = orderBy.asSqlOrderBy
        SortQuery(items, ast.copy(orderBy = ast.orderBy :+ sqlOrderBy))

class SortQuery[T, S <: ResultSize](
    private[sqala] val items: T,
    override val ast: SqlQuery.Select
)(using q: QueryContext) extends Query[T, S](items, ast):
    inline def sortBy[O, K <: ExprKind](f: T => OrderBy[O, K]): SortQuery[T, S] =
        inline erasedValue[K] match
            case _: AggKind | AggOperationKind | WindowKind =>
                error("Column must appear in the GROUP BY clause or be used in an aggregate function.")
            case _ =>
        val orderBy = f(queryItems)
        val sqlOrderBy = orderBy.asSqlOrderBy
        SortQuery(items, ast.copy(orderBy = ast.orderBy :+ sqlOrderBy))

class DistinctQuery[T, S <: ResultSize](
    private[sqala] val items: T,
    override val ast: SqlQuery.Select
)(using q: QueryContext) extends Query[T, S](items, ast):
    inline def sortBy[O, K <: ExprKind](f: T => OrderBy[O, K]): DistinctQuery[T, S] =
        inline erasedValue[K] match
            case _: DistinctKind =>
            case _ =>
                error("For SELECT DISTINCT, ORDER BY expressions must appear in select list.")
        val orderBy = f(queryItems)
        val sqlOrderBy = orderBy.asSqlOrderBy
        DistinctQuery(items, ast.copy(orderBy = ast.orderBy :+ sqlOrderBy))

class SelectQuery[T](
    private[sqala] val items: T,
    override val ast: SqlQuery.Select
)(using q: QueryContext) extends Query[T, ResultSize.Many.type](items, ast):
    inline def filter[K <: ExprKind](f: T => Expr[Boolean, K]): SelectQuery[T] =
        inline erasedValue[K] match
            case _: AggKind | AggOperationKind => error("Aggregate functions are not allowed in WHERE.")
            case _: WindowKind => error("Window functions are not allowed in WHERE.")
            case _: SimpleKind =>
        val condition = f(items).asSqlExpr
        SelectQuery(items, ast.addWhere(condition))

    inline def filterIf[K <: ExprKind](test: Boolean)(f: T => Expr[Boolean, K]): SelectQuery[T] =
        if test then filter(f) else this

    inline def withFilter[K <: ExprKind](f: T => Expr[Boolean, K]): SelectQuery[T] =
        filter(f)

    inline def map[R](f: T => R)(using s: SelectItem[R], i: IsAggKind[R], n: NotAggKind[R], cm: CheckMapKind[i.R, n.R], c: ChangeKind[R, ColumnKind]): ProjectionQuery[c.R, ProjectionSize[i.R]] =
        val mappedItems = f(items)
        val selectItems = s.selectItems(mappedItems, 0)
        ProjectionQuery(c.changeKind(mappedItems), ast.copy(select = selectItems))

    private inline def joinClause[JT, R](joinType: SqlJoinType)(using s: SelectItem[R], m: Mirror.ProductOf[JT]): JoinQuery[R] =
        AsSqlExpr.summonInstances[m.MirroredElemTypes]
        val joinTableName = TableMacro.tableName[JT]
        q.tableIndex += 1
        val joinAliasName = s"t${q.tableIndex}"
        val joinTable = Table(joinTableName, joinAliasName, TableMacro.tableMetaData[JT])
        val tables = (
            inline items match
                case x: Tuple => x ++ Tuple1(joinTable)
                case _ => (items, joinTable)
        ).asInstanceOf[R]
        val selectItems = s.selectItems(tables, 0)
        val sqlTable: Option[SqlTable.JoinTable] = ast.from.headOption.map: i =>
            SqlTable.JoinTable(
                i,
                joinType, 
                SqlTable.IdentTable(joinTableName, Some(SqlTableAlias(joinAliasName))),
                None
            )
        JoinQuery(tables, sqlTable, ast.copy(select = selectItems, from = sqlTable.toList))

    private def joinQueryClause[N <: Tuple, V <: Tuple, S <: ResultSize, R](joinType: SqlJoinType, query: Query[NamedTuple[N, V], S])(using s: SelectItem[R], c: Option[WithContext]): JoinQuery[R] =
        q.tableIndex += 1
        val aliasName = c.map(_.alias).getOrElse(s"t${q.tableIndex}")
        val joinQuery = query
        val rightTable = NamedQuery(joinQuery, aliasName)
        val tables = (
            items match
                case x: Tuple => x ++ Tuple1(rightTable)
                case _ => (items, rightTable)
        ).asInstanceOf[R]
        val sqlTable: Option[SqlTable.JoinTable] = ast.from.headOption.map: i =>
            SqlTable.JoinTable(
                i,
                joinType,
                c match
                    case Some(WithContext(alias)) =>
                        SqlTable.IdentTable(alias, None)
                    case None =>
                        SqlTable.SubQueryTable(joinQuery.ast, false, SqlTableAlias(aliasName))
                ,
                None
            )
        JoinQuery(tables, sqlTable, ast.copy(select = s.selectItems(tables, 0), from = sqlTable.toList))

    private def joinLateralClause[N <: Tuple, V <: Tuple, S <: ResultSize, R](joinType: SqlJoinType, query: T => Query[NamedTuple[N, V], S])(using s: SelectItem[R]): JoinQuery[R] =
        q.tableIndex += 1
        val aliasName = s"t${q.tableIndex}"
        val joinQuery = query(items)
        val rightTable = NamedQuery(joinQuery, aliasName)
        val tables = (
            items match
                case x: Tuple => x ++ Tuple1(rightTable)
                case _ => (items, rightTable)
        ).asInstanceOf[R]
        val sqlTable: Option[SqlTable.JoinTable] = ast.from.headOption.map: i =>
            SqlTable.JoinTable(
                i,
                joinType,
                SqlTable.SubQueryTable(joinQuery.ast, true, SqlTableAlias(aliasName)),
                None
            )
        JoinQuery(tables, sqlTable, ast.copy(select = s.selectItems(tables, 0), from = sqlTable.toList))

    private inline def joinListClause[L <: Product, J <: Tuple, R](joinType: SqlJoinType, list: List[L])(using s: SelectItem[R]): JoinQuery[R] =
        q.tableIndex += 1
        val aliasName = s"t${q.tableIndex}"
        val instances = AsSqlExpr.summonInstances[DropNames[From[L]]]
        val head = list.head
        val terms = head.productIterator.toList
        val selectItems: List[SqlSelectItem.Item] = terms.zip(instances).zip(terms.indices).map: (pair, i) =>
            SqlSelectItem.Item(pair._2.asInstanceOf[AsSqlExpr[Any]].asSqlExpr(pair._1), Some(s"c$i"))
        val columns = (
            inline erasedValue[J] match
                case _: Tuple => Tuple.fromArray(selectItems.map(i => Expr.Column(aliasName, i.alias.get)).toArray)
                case _ => Expr.Column(aliasName, selectItems.head.alias.get)
        ).asInstanceOf[J]
        val valuesAst = SqlQuery.Values(
            list.map: v =>
                val terms = v.productIterator.toList
                terms.zip(instances).map: (term, instance) =>
                    instance.asInstanceOf[AsSqlExpr[Any]].asSqlExpr(term)
        )
        val values = Query[NamedTuple[Names[From[L]], J], ResultSize.Many.type](NamedTuple(columns), valuesAst)
        val rightTable = NamedQuery(values, aliasName)
        val tables = (
            inline items match
                case x: Tuple => x ++ Tuple1(rightTable)
                case _ => (items, rightTable)
        ).asInstanceOf[R]
        val sqlTable: Option[SqlTable.JoinTable] = ast.from.headOption.map: i =>
            SqlTable.JoinTable(
                i,
                joinType,
                SqlTable.SubQueryTable(valuesAst, false, SqlTableAlias(aliasName, selectItems.map(i => i.alias.get))),
                None
            )
        JoinQuery(tables, sqlTable, ast.copy(select = s.selectItems(tables, 0), from = sqlTable.toList))

    inline def join[R](using SelectItem[InnerJoin[T, R]], Mirror.ProductOf[R]): JoinQuery[InnerJoin[T, R]] =
        joinClause[R, InnerJoin[T, R]](SqlJoinType.InnerJoin)

    inline def join[N <: Tuple, V <: Tuple, S <: ResultSize](query: Query[NamedTuple[N, V], S])(using s: SelectItem[InnerJoinQuery[T, V, N]], a: AsExpr[V], c: Option[WithContext] = None): JoinQuery[InnerJoinQuery[T, V, N]] =
        joinQueryClause[N, V, S, InnerJoinQuery[T, V, N]](SqlJoinType.InnerJoin, query)

    inline def join[N <: Tuple, V <: Tuple, S <: ResultSize](query: T => Query[NamedTuple[N, V], S])(using SelectItem[InnerJoinQuery[T, V, N]], AsExpr[V]): JoinQuery[InnerJoinQuery[T, V, N]] =
        joinLateralClause[N, V, S, InnerJoinQuery[T, V, N]](SqlJoinType.InnerJoin, query)

    inline def join[L <: Product](list: List[L])(using SelectItem[InnerJoinQuery[T, Tuple.Map[DropNames[From[L]], [t] =>> Expr[t, ColumnKind]], Names[From[L]]]]): JoinQuery[InnerJoinQuery[T, Tuple.Map[DropNames[From[L]], [t] =>> Expr[t, ColumnKind]], Names[From[L]]]] =
        joinListClause[L, Tuple.Map[DropNames[From[L]], [t] =>> Expr[t, ColumnKind]], InnerJoinQuery[T, Tuple.Map[DropNames[From[L]], [t] =>> Expr[t, ColumnKind]], Names[From[L]]]](SqlJoinType.InnerJoin, list)

    inline def leftJoin[R](using SelectItem[LeftJoin[T, R]], Mirror.ProductOf[R]): JoinQuery[LeftJoin[T, R]] =
        joinClause[R, LeftJoin[T, R]](SqlJoinType.LeftJoin)

    inline def leftJoin[N <: Tuple, V <: Tuple, S <: ResultSize](query: Query[NamedTuple[N, V], S])(using s: SelectItem[LeftJoinQuery[T, V, N]], a: AsExpr[V], c: Option[WithContext] = None): JoinQuery[LeftJoinQuery[T, V, N]] =
        joinQueryClause[N, V, S, LeftJoinQuery[T, V, N]](SqlJoinType.LeftJoin, query)

    inline def leftJoin[N <: Tuple, V <: Tuple, S <: ResultSize](query: T => Query[NamedTuple[N, V], S])(using SelectItem[LeftJoinQuery[T, V, N]], AsExpr[V]): JoinQuery[LeftJoinQuery[T, V, N]] =
        joinLateralClause[N, V, S, LeftJoinQuery[T, V, N]](SqlJoinType.LeftJoin, query)

    inline def leftJoin[L <: Product](list: List[L])(using SelectItem[LeftJoinQuery[T, Tuple.Map[DropNames[From[L]], [t] =>> Expr[t, ColumnKind]], Names[From[L]]]]): JoinQuery[LeftJoinQuery[T, Tuple.Map[DropNames[From[L]], [t] =>> Expr[t, ColumnKind]], Names[From[L]]]] =
        joinListClause[L, Tuple.Map[DropNames[From[L]], [t] =>> Expr[t, ColumnKind]], LeftJoinQuery[T, Tuple.Map[DropNames[From[L]], [t] =>> Expr[t, ColumnKind]], Names[From[L]]]](SqlJoinType.LeftJoin, list)

    inline def rightJoin[R](using SelectItem[RightJoin[T, R]], Mirror.ProductOf[R]): JoinQuery[RightJoin[T, R]] =
        joinClause[R, RightJoin[T, R]](SqlJoinType.RightJoin)

    inline def rightJoin[N <: Tuple, V <: Tuple, S <: ResultSize](query: Query[NamedTuple[N, V], S])(using s: SelectItem[RightJoinQuery[T, V, N]], a: AsExpr[V], c: Option[WithContext] = None): JoinQuery[RightJoinQuery[T, V, N]] =
        joinQueryClause[N, V, S, RightJoinQuery[T, V, N]](SqlJoinType.RightJoin, query)

    inline def rightJoin[N <: Tuple, V <: Tuple, S <: ResultSize](query: T => Query[NamedTuple[N, V], S])(using SelectItem[RightJoinQuery[T, V, N]], AsExpr[V]): JoinQuery[RightJoinQuery[T, V, N]] =
        joinLateralClause[N, V, S, RightJoinQuery[T, V, N]](SqlJoinType.RightJoin, query)

    inline def rightJoin[L <: Product](list: List[L])(using SelectItem[RightJoinQuery[T, Tuple.Map[DropNames[From[L]], [t] =>> Expr[t, ColumnKind]], Names[From[L]]]]): JoinQuery[RightJoinQuery[T, Tuple.Map[DropNames[From[L]], [t] =>> Expr[t, ColumnKind]], Names[From[L]]]] =
        joinListClause[L, Tuple.Map[DropNames[From[L]], [t] =>> Expr[t, ColumnKind]], RightJoinQuery[T, Tuple.Map[DropNames[From[L]], [t] =>> Expr[t, ColumnKind]], Names[From[L]]]](SqlJoinType.RightJoin, list)

    def groupBy[G](f: T => G)(using a: AsExpr[G], na: NotAggKind[G], nw: NotWindowKind[G], nv: NotValueKind[G], c: CheckGroupByKind[na.R, nw.R, nv.R], ca: ChangeKind[G, GroupKind]): GroupByQuery[(ca.R, T)] =
        val groupByItems = f(items)
        val sqlGroupBy = a.asExprs(groupByItems).map(i => SqlGroupItem.Singleton(i.asSqlExpr))
        GroupByQuery((ca.changeKind(groupByItems), items), ast.copy(groupBy = sqlGroupBy))

    def groupByCube[G](f: T => G)(using a: AsExpr[G], na: NotAggKind[G], nw: NotWindowKind[G], nv: NotValueKind[G], c: CheckGroupByKind[na.R, nw.R, nv.R], ca: ChangeKind[G, GroupKind], to: ChangeOption[ca.R]): GroupByQuery[(to.R, T)] =
        val groupByItems = f(items)
        val sqlGroupBy = SqlGroupItem.Cube(a.asExprs(groupByItems).map(_.asSqlExpr))
        GroupByQuery((to.changeOption(ca.changeKind(groupByItems)), items), ast.copy(groupBy = sqlGroupBy :: Nil))

    def groupByRollup[G](f: T => G)(using a: AsExpr[G], na: NotAggKind[G], nw: NotWindowKind[G], nv: NotValueKind[G], c: CheckGroupByKind[na.R, nw.R, nv.R], ca: ChangeKind[G, GroupKind], to: ChangeOption[ca.R]): GroupByQuery[(to.R, T)] =
        val groupByItems = f(items)
        val sqlGroupBy = SqlGroupItem.Rollup(a.asExprs(groupByItems).map(_.asSqlExpr))
        GroupByQuery((to.changeOption(ca.changeKind(groupByItems)), items), ast.copy(groupBy = sqlGroupBy :: Nil))

    inline def groupByGroupingSets[G, S](f: T => G)(using ca: ChangeKind[G, GroupKind])(g: ca.R => S)(using a: AsExpr[G], na: NotAggKind[G], nw: NotWindowKind[G], nv: NotValueKind[G], c: CheckGroupByKind[na.R, nw.R, nv.R], to: ChangeOption[ca.R], s: GroupingSets[S]): GroupByQuery[(to.R, T)] =
        inline erasedValue[CheckGrouping[S]] match
            case _: false => 
                error("For GROUPING SETS, expressions must appear in grouping list.")
            case _: true =>
        val groupByItems = f(items)
        val changedGroupByItems = ca.changeKind(groupByItems)
        val sets = g(changedGroupByItems)
        val sqlGroupBy = SqlGroupItem.GroupingSets(s.asSqlExprs(sets))
        GroupByQuery((to.changeOption(changedGroupByItems), items), ast.copy(groupBy = sqlGroupBy :: Nil))

class UnionQuery[T](
    private[sqala] val left: Query[?, ?],
    private[sqala] val unionType: SqlUnionType,
    private[sqala] val right: SqlQuery
) extends Query[T, ResultSize.Many.type](left.queryItems.asInstanceOf[T], SqlQuery.Union(left.ast, unionType, right))