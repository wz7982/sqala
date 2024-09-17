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

import scala.Tuple.Append
import scala.collection.mutable.ListBuffer
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

    def size: Query[Expr[Long, ColumnKind], ResultSize.OneRow] = 
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

    def exists: Query[Expr[Boolean, ColumnKind], ResultSize.OneRow] =
        val expr = sqala.dsl.exists(this).asInstanceOf[Expr[Boolean, ColumnKind]]
        val outerQuery: SqlQuery.Select = SqlQuery.Select(
            select = SqlSelectItem.Item(expr.asSqlExpr, None) :: Nil,
            from = Nil
        )
        Query(expr, outerQuery)

object Query:
    extension [Q](query: Query[Q, ResultSize.OneRow])(using m: Merge[Q])
        def asExpr: Expr[m.R, CommonKind] = Expr.SubQuery(query.ast)

    extension [T, S <: ResultSize, U, US <: ResultSize](query: Query[T, S])
        infix def union(unionQuery: Query[U, US])(using u: UnionOperation[T, U]): Query[u.R, ResultSize.ManyRows] =
            UnionQuery(query, SqlUnionType.Union, unionQuery.ast)

        infix def unionAll(unionQuery: Query[U, US])(using u: UnionOperation[T, U]): Query[u.R, ResultSize.ManyRows] =
            UnionQuery(query, SqlUnionType.UnionAll, unionQuery.ast)

        def ++(unionQuery: Query[U, US])(using u: UnionOperation[T, U]): Query[u.R, ResultSize.ManyRows] =
            UnionQuery(query, SqlUnionType.UnionAll, unionQuery.ast)

        infix def except(unionQuery: Query[U, US])(using u: UnionOperation[T, U]): Query[u.R, ResultSize.ManyRows] =
            UnionQuery(query, SqlUnionType.Except, unionQuery.ast)

        infix def exceptAll(unionQuery: Query[U, US])(using u: UnionOperation[T, U]): Query[u.R, ResultSize.ManyRows] =
            UnionQuery(query, SqlUnionType.ExceptAll, unionQuery.ast)

        infix def intersect(unionQuery: Query[U, US])(using u: UnionOperation[T, U]): Query[u.R, ResultSize.ManyRows] =
            UnionQuery(query, SqlUnionType.Intersect, unionQuery.ast)

        infix def intersectAll(unionQuery: Query[U, US])(using u: UnionOperation[T, U]): Query[u.R, ResultSize.ManyRows] =
            UnionQuery(query, SqlUnionType.IntersectAll, unionQuery.ast)

class ProjectionQuery[T, S <: ResultSize](
    private[sqala] val items: T,
    override val ast: SqlQuery.Select
)(using q: QueryContext) extends Query[T, S](items, ast):
    def distinct(using t: TransformKindIfNot[T, DistinctKind, ValueKind]): DistinctQuery[t.R, S] =
        DistinctQuery(t.tansform(items), ast.copy(param = Some(SqlSelectParam.Distinct)))

    inline def sortBy[O, K <: ExprKind](f: T => OrderBy[O, K]): SortQuery[T, S] =
        inline erasedValue[K] match
            case _: AggKind =>
                error("Column must appear in the GROUP BY clause or be used in an aggregate function.")
            case _: AggOperationKind =>
                error("Column must appear in the GROUP BY clause or be used in an aggregate function.")
            case _: WindowKind =>
                error("Column must appear in the GROUP BY clause or be used in an aggregate function.")
            case _: ValueKind =>
                error("Constants are not allowed in ORDER BY.")
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
            case _: AggKind =>
                error("Column must appear in the GROUP BY clause or be used in an aggregate function.")
            case _: AggOperationKind =>
                error("Column must appear in the GROUP BY clause or be used in an aggregate function.")
            case _: WindowKind =>
                error("Column must appear in the GROUP BY clause or be used in an aggregate function.")
            case _: ValueKind =>
                error("Constants are not allowed in ORDER BY.")
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
            case _: ValueKind =>
                error("Constants are not allowed in ORDER BY.")
            case _ =>
                error("For SELECT DISTINCT, ORDER BY expressions must appear in select list.")
        val orderBy = f(queryItems)
        val sqlOrderBy = orderBy.asSqlOrderBy
        DistinctQuery(items, ast.copy(orderBy = ast.orderBy :+ sqlOrderBy))

class SelectQuery[T](
    private[sqala] val items: T,
    private[sqala] val columnCursor: Int,
    override val ast: SqlQuery.Select
)(using q: QueryContext) extends Query[T, ResultSize.ManyRows](items, ast):
    inline def filter[K <: ExprKind](f: T => Expr[Boolean, K]): SelectQuery[T] =
        inline erasedValue[K] match
            case _: AggKind => error("Aggregate functions are not allowed in WHERE.")
            case _: AggOperationKind => error("Aggregate functions are not allowed in WHERE.")
            case _: WindowKind => error("Window functions are not allowed in WHERE.")
            case _: SimpleKind =>
        val condition = f(items).asSqlExpr
        SelectQuery(items, columnCursor, ast.addWhere(condition))

    inline def filterIf[K <: ExprKind](test: Boolean)(f: T => Expr[Boolean, K]): SelectQuery[T] =
        if test then filter(f) else this

    inline def withFilter[K <: ExprKind](f: T => Expr[Boolean, K]): SelectQuery[T] =
        filter(f)

    inline def map[R](f: T => R)(using s: SelectItem[R], h: HasAgg[R], n: NotAgg[R], c: CheckMapKind[h.R, n.R]): ProjectionQuery[R, ProjectionSize[h.R]] =
        val mappedItems = f(items)
        val selectItems = s.selectItems(mappedItems, 0)
        ProjectionQuery(mappedItems, ast.copy(select = selectItems))

    private inline def joinClause[JT, R](joinType: SqlJoinType)(using m: Mirror.ProductOf[JT]): JoinQuery[R] =
        AsSqlExpr.summonInstances[m.MirroredElemTypes]
        val joinTableName = TableMacro.tableName[JT]
        q.tableIndex += 1
        val joinAliasName = s"t${q.tableIndex}"
        val joinTable = Table[JT](joinTableName, joinAliasName, TableMacro.tableMetaData[JT])
        val selectItems = joinTable.__selectItems__(columnCursor)
        val tables = (
            inline items match
                case x: Tuple => x :* joinTable
                case _ => Tuple1(items) :* joinTable
        ).asInstanceOf[R]
        val sqlTable: Option[SqlTable.JoinTable] = ast.from.headOption.map: i =>
            SqlTable.JoinTable(
                i,
                joinType, 
                SqlTable.IdentTable(joinTableName, Some(SqlTableAlias(joinAliasName))),
                None
            )
        JoinQuery(tables, sqlTable, columnCursor + selectItems.size, ast.copy(select = ast.select ++ selectItems, from = sqlTable.toList))

    private def joinQueryClause[Q, S <: ResultSize, R](joinType: SqlJoinType, query: Query[Q, S])(using s: SelectItem[Q], c: Option[WithContext]): JoinQuery[R] =
        q.tableIndex += 1
        val aliasName = c.map(_.alias).getOrElse(s"t${q.tableIndex}")
        val newItems = (
            items match
                case x: Tuple => x :* s.subQueryItems(query.queryItems, 0, aliasName)
                case _ => Tuple1(items) :* s.subQueryItems(query.queryItems, 0, aliasName)
        ).asInstanceOf[R]
        val selectItems = s.selectItems(query.queryItems, 0)
        var tmpCursor = columnCursor
        val tmpItems = ListBuffer[SqlSelectItem.Item]()
        for field <- selectItems.map(_.alias.get) do
            tmpItems.addOne(SqlSelectItem.Item(SqlExpr.Column(Some(aliasName), field), Some(s"c${tmpCursor}")))
            tmpCursor += 1
        val queryItems = tmpItems.toList
        val sqlTable: Option[SqlTable.JoinTable] = ast.from.headOption.map: i =>
            SqlTable.JoinTable(
                i,
                joinType,
                c match
                    case Some(WithContext(alias)) =>
                        SqlTable.IdentTable(alias, None)
                    case None =>
                        SqlTable.SubQueryTable(query.ast, false, SqlTableAlias(aliasName))
                ,
                None
            )
        JoinQuery(newItems, sqlTable, columnCursor + selectItems.size, ast.copy(select = ast.select ++ queryItems, from = sqlTable.toList))

    private def joinLateralQueryClause[Q, S <: ResultSize, R](joinType: SqlJoinType, f: T => Query[Q, S])(using s: SelectItem[Q]): JoinQuery[R] =
        q.tableIndex += 1
        val aliasName = s"t${q.tableIndex}"
        val query = f(items)
        val newItems = (
            items match
                case x: Tuple => x :* s.subQueryItems(query.queryItems, 0, aliasName)
                case _ => Tuple1(items) :* s.subQueryItems(query.queryItems, 0, aliasName)
        ).asInstanceOf[R]
        val selectItems = s.selectItems(query.queryItems, 0)
        var tmpCursor = columnCursor
        val tmpItems = ListBuffer[SqlSelectItem.Item]()
        for field <- selectItems.map(_.alias.get) do
            tmpItems.addOne(SqlSelectItem.Item(SqlExpr.Column(Some(aliasName), field), Some(s"c${tmpCursor}")))
            tmpCursor += 1
        val queryItems = tmpItems.toList
        val sqlTable: Option[SqlTable.JoinTable] = ast.from.headOption.map: i =>
            SqlTable.JoinTable(
                i,
                joinType,
                SqlTable.SubQueryTable(query.ast, true, SqlTableAlias(aliasName)),
                None
            )
        JoinQuery(newItems, sqlTable, columnCursor + selectItems.size, ast.copy(select = ast.select ++ queryItems, from = sqlTable.toList))

    inline def join[JT](using a: AsTable[JT], m: Mirror.ProductOf[JT]): JoinQuery[Append[ToTuple[T], a.R]] =
        joinClause[JT, Append[ToTuple[T], a.R]](SqlJoinType.InnerJoin)

    inline def join[Q, S <: ResultSize](query: Query[Q, S])(using s: SelectItem[Q], c: Option[WithContext] = None): JoinQuery[Append[ToTuple[T], s.R]] =
        joinQueryClause[Q, S, Append[ToTuple[T], s.R]](SqlJoinType.InnerJoin, query)

    inline def joinLateral[Q, S <: ResultSize](f: T => Query[Q, S])(using s: SelectItem[Q]): JoinQuery[Append[ToTuple[T], s.R]] =
        joinLateralQueryClause[Q, S, Append[ToTuple[T], s.R]](SqlJoinType.InnerJoin, f)

    inline def leftJoin[JT](using a: AsTable[JT])(using o: ToOption[a.R], m: Mirror.ProductOf[JT]): JoinQuery[Append[ToTuple[T], o.R]] =
        joinClause[JT, Append[ToTuple[T], o.R]](SqlJoinType.LeftJoin)

    inline def leftJoin[Q, S <: ResultSize](query: Query[Q, S])(using s: SelectItem[Q])(using o: ToOption[s.R], c: Option[WithContext] = None): JoinQuery[Append[ToTuple[T], o.R]] =
        joinQueryClause[Q, S, Append[ToTuple[T], o.R]](SqlJoinType.LeftJoin, query)

    inline def leftJoinLateral[Q, S <: ResultSize](f: T => Query[Q, S])(using s: SelectItem[Q])(using o: ToOption[s.R]): JoinQuery[Append[ToTuple[T], o.R]] =
        joinLateralQueryClause[Q, S, Append[ToTuple[T], o.R]](SqlJoinType.LeftJoin, f)

    inline def rightJoin[JT](using a: AsTable[JT], o: ToOption[T], m: Mirror.ProductOf[JT]): JoinQuery[Append[ToTuple[o.R], a.R]] =
        joinClause[JT, Append[ToTuple[o.R], a.R]](SqlJoinType.RightJoin)

    inline def rightJoin[Q, S <: ResultSize](query: Query[Q, S])(using s: SelectItem[Q], o: ToOption[T], c: Option[WithContext] = None): JoinQuery[Append[ToTuple[o.R], s.R]] =
        joinQueryClause[Q, S, Append[ToTuple[o.R], s.R]](SqlJoinType.RightJoin, query)

    def groupBy[G](f: T => G)(using a: AsExpr[G], na: NotAgg[G], nw: NotWindow[G], nv: NotValue[G], c: CheckGroupByKind[na.R, nw.R, nv.R], t: TransformKind[G, GroupKind]): GroupByQuery[(t.R, T)] =
        val groupByItems = f(items)
        val sqlGroupBy = a.asExprs(groupByItems).map(i => SqlGroupItem.Singleton(i.asSqlExpr))
        GroupByQuery((t.tansform(groupByItems), items), ast.copy(groupBy = sqlGroupBy))

    def groupByCube[G](f: T => G)(using a: AsExpr[G], na: NotAgg[G], nw: NotWindow[G], nv: NotValue[G], c: CheckGroupByKind[na.R, nw.R, nv.R], t: TransformKind[G, GroupKind], to: ToOption[t.R]): GroupByQuery[(to.R, T)] =
        val groupByItems = f(items)
        val sqlGroupBy = SqlGroupItem.Cube(a.asExprs(groupByItems).map(_.asSqlExpr))
        GroupByQuery((to.toOption(t.tansform(groupByItems)), items), ast.copy(groupBy = sqlGroupBy :: Nil))

    def groupByRollup[G](f: T => G)(using a: AsExpr[G], na: NotAgg[G], nw: NotWindow[G], nv: NotValue[G], c: CheckGroupByKind[na.R, nw.R, nv.R], t: TransformKind[G, GroupKind], to: ToOption[t.R]): GroupByQuery[(to.R, T)] =
        val groupByItems = f(items)
        val sqlGroupBy = SqlGroupItem.Rollup(a.asExprs(groupByItems).map(_.asSqlExpr))
        GroupByQuery((to.toOption(t.tansform(groupByItems)), items), ast.copy(groupBy = sqlGroupBy :: Nil))

    inline def groupByGroupingSets[G, S](f: T => G)(using t: TransformKind[G, GroupKind])(g: t.R => S)(using a: AsExpr[G], na: NotAgg[G], nw: NotWindow[G], nv: NotValue[G], c: CheckGroupByKind[na.R, nw.R, nv.R], to: ToOption[t.R], s: GroupingSets[S]): GroupByQuery[(to.R, T)] =
        inline erasedValue[CheckGrouping[S]] match
            case _: false => 
                error("For GROUPING SETS, expressions must appear in grouping list.")
            case _: true =>
        val groupByItems = f(items)
        val changedGroupByItems = t.tansform(groupByItems)
        val sets = g(changedGroupByItems)
        val sqlGroupBy = SqlGroupItem.GroupingSets(s.asSqlExprs(sets))
        GroupByQuery((to.toOption(changedGroupByItems), items), ast.copy(groupBy = sqlGroupBy :: Nil))

class UnionQuery[T](
    private[sqala] val left: Query[?, ?],
    private[sqala] val unionType: SqlUnionType,
    private[sqala] val right: SqlQuery
) extends Query[T, ResultSize.ManyRows](left.queryItems.asInstanceOf[T], SqlQuery.Union(left.ast, unionType, right))