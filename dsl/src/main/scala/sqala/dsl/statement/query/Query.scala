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

import scala.NamedTuple.NamedTuple
import scala.Tuple.Append
import scala.compiletime.{erasedValue, error}
import scala.deriving.Mirror

sealed class Query[T, S <: ResultSize](private[sqala] val queryItems: T, val ast: SqlQuery)(using val qc: QueryContext):
    def sql(dialect: Dialect, prepare: Boolean = true, indent: Int = 4): (String, Array[Any]) =
        queryToString(ast, dialect, prepare, indent)

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
            case s@SqlQuery.Select(_, _, _, _, Nil, _, _, _) =>
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
        infix def union(unionQuery: QueryContext ?=> Query[U, US])(using u: UnionOperation[T, U]): Query[u.R, ResultSize.ManyRows] =
            given QueryContext = query.qc
            UnionQuery(query, SqlUnionType.Union, unionQuery.ast)

        infix def unionAll(unionQuery: QueryContext ?=> Query[U, US])(using u: UnionOperation[T, U]): Query[u.R, ResultSize.ManyRows] =
            given QueryContext = query.qc
            UnionQuery(query, SqlUnionType.UnionAll, unionQuery.ast)

        def ++(unionQuery: QueryContext ?=> Query[U, US])(using u: UnionOperation[T, U]): Query[u.R, ResultSize.ManyRows] =
            given QueryContext = query.qc
            UnionQuery(query, SqlUnionType.UnionAll, unionQuery.ast)

        infix def except(unionQuery: QueryContext ?=> Query[U, US])(using u: UnionOperation[T, U]): Query[u.R, ResultSize.ManyRows] =
            given QueryContext = query.qc
            UnionQuery(query, SqlUnionType.Except, unionQuery.ast)

        infix def exceptAll(unionQuery: QueryContext ?=> Query[U, US])(using u: UnionOperation[T, U]): Query[u.R, ResultSize.ManyRows] =
            given QueryContext = query.qc
            UnionQuery(query, SqlUnionType.ExceptAll, unionQuery.ast)

        infix def intersect(unionQuery: QueryContext ?=> Query[U, US])(using u: UnionOperation[T, U]): Query[u.R, ResultSize.ManyRows] =
            given QueryContext = query.qc
            UnionQuery(query, SqlUnionType.Intersect, unionQuery.ast)

        infix def intersectAll(unionQuery: QueryContext ?=> Query[U, US])(using u: UnionOperation[T, U]): Query[u.R, ResultSize.ManyRows] =
            given QueryContext = query.qc
            UnionQuery(query, SqlUnionType.IntersectAll, unionQuery.ast)

class ProjectionQuery[T, S <: ResultSize](
    private[sqala] override val queryItems: T,
    override val ast: SqlQuery.Select
)(using QueryContext) extends Query[T, S](queryItems, ast):
    def distinct(using t: TransformKindIfNot[T, DistinctKind, ValueKind]): DistinctQuery[t.R, S] =
        DistinctQuery(t.tansform(queryItems), ast.copy(param = Some(SqlSelectParam.Distinct)))

    inline def sortBy[O, K <: ExprKind](f: QueryContext ?=> T => OrderBy[O, K]): SortQuery[T, S] =
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
        SortQuery(queryItems, ast.copy(orderBy = ast.orderBy :+ sqlOrderBy))

class SortQuery[T, S <: ResultSize](
    private[sqala] override val queryItems: T,
    override val ast: SqlQuery.Select
)(using QueryContext) extends Query[T, S](queryItems, ast):
    inline def sortBy[O, K <: ExprKind](f: QueryContext ?=> T => OrderBy[O, K]): SortQuery[T, S] =
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
        SortQuery(queryItems, ast.copy(orderBy = ast.orderBy :+ sqlOrderBy))

class DistinctQuery[T, S <: ResultSize](
    private[sqala] override val queryItems: T,
    override val ast: SqlQuery.Select
)(using QueryContext) extends Query[T, S](queryItems, ast):
    inline def sortBy[O, K <: ExprKind](f: QueryContext ?=> T => OrderBy[O, K]): DistinctQuery[T, S] =
        inline erasedValue[K] match
            case _: DistinctKind =>
            case _: ValueKind =>
                error("Constants are not allowed in ORDER BY.")
            case _ =>
                error("For SELECT DISTINCT, ORDER BY expressions must appear in select list.")
        val orderBy = f(queryItems)
        val sqlOrderBy = orderBy.asSqlOrderBy
        DistinctQuery(queryItems, ast.copy(orderBy = ast.orderBy :+ sqlOrderBy))

class SelectQuery[T](
    private[sqala] override val queryItems: T,
    override val ast: SqlQuery.Select
)(using QueryContext) extends Query[T, ResultSize.ManyRows](queryItems, ast):
    inline def filter[K <: ExprKind](f: QueryContext ?=> T => Expr[Boolean, K]): SelectQuery[T] =
        inline erasedValue[K] match
            case _: AggKind => error("Aggregate functions are not allowed in WHERE.")
            case _: AggOperationKind => error("Aggregate functions are not allowed in WHERE.")
            case _: WindowKind => error("Window functions are not allowed in WHERE.")
            case _: SimpleKind =>
        val condition = f(queryItems).asSqlExpr
        SelectQuery(queryItems, ast.addWhere(condition))

    inline def withFilter[K <: ExprKind](f: QueryContext ?=> T => Expr[Boolean, K]): SelectQuery[T] =
        filter(f)

    inline def filterIf[K <: ExprKind](test: Boolean)(f: T => Expr[Boolean, K]): SelectQuery[T] =
        if test then filter(f) else this

    inline def map[R](f: QueryContext ?=> T => R)(using s: SelectItem[R], a: SelectItemAsExpr[R], h: HasAgg[R], n: NotAgg[R], c: CheckMapKind[h.R, n.R]): ProjectionQuery[R, ProjectionSize[h.R]] =
        val mappedItems = f(queryItems)
        val selectItems = s.selectItems(mappedItems, 0)
        ProjectionQuery(mappedItems, ast.copy(select = selectItems))

    private inline def joinClause[J, R](joinType: SqlJoinType)(using m: Mirror.ProductOf[J], s: SelectItem[R]): JoinQuery[R] =
        AsSqlExpr.summonInstances[m.MirroredElemTypes]
        val joinTableName = TableMacro.tableName[J]
        qc.tableIndex += 1
        val joinAliasName = s"t${qc.tableIndex}"
        val joinTable = Table[J](joinTableName, joinAliasName, TableMacro.tableMetaData[J])
        val tables = (
            inline queryItems match
                case x: Tuple => x :* joinTable
                case _ => Tuple1(queryItems) :* joinTable
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

    private def joinQueryClause[N <: Tuple, V <: Tuple, S <: ResultSize, R](joinType: SqlJoinType, query: QueryContext ?=> Query[NamedTuple[N, V], S], lateral: Boolean = false)(using s: SelectItem[R], sq: SelectItem[NamedTuple[N, V]]): JoinQuery[R] =
        qc.tableIndex += 1
        val aliasName = s"t${qc.tableIndex}"
        val joinQuery = query
        val rightTable = SubQuery(aliasName, sq.offset(query.queryItems))
        val tables = (
            queryItems match
                case x: Tuple => x :* rightTable
                case _ => Tuple1(queryItems) :* rightTable
        ).asInstanceOf[R]
        val sqlTable: Option[SqlTable.JoinTable] = ast.from.headOption.map: i =>
            SqlTable.JoinTable(
                i,
                joinType,
                SqlTable.SubQueryTable(joinQuery.ast, lateral, SqlTableAlias(aliasName)),
                None
            )
        JoinQuery(tables, sqlTable, ast.copy(select = s.selectItems(tables, 0), from = sqlTable.toList))

    inline def join[J](using m: Mirror.ProductOf[J], tt: ToTuple[T])(using SelectItem[Append[tt.R, Table[J]]]): JoinQuery[Append[tt.R, Table[J]]] =
        joinClause(SqlJoinType.InnerJoin)

    inline def joinQuery[N <: Tuple, V <: Tuple, S <: ResultSize](query: QueryContext ?=> Query[NamedTuple[N, V], S])(using tt: ToTuple[T])(using SelectItem[Append[tt.R, SubQuery[N, V]]], SelectItem[NamedTuple[N, V]]): JoinQuery[Append[tt.R, SubQuery[N, V]]] =
        joinQueryClause(SqlJoinType.InnerJoin, query)

    inline def joinLateral[N <: Tuple, V <: Tuple, S <: ResultSize](f: QueryContext ?=> T => Query[NamedTuple[N, V], S])(using tt: ToTuple[T])(using SelectItem[Append[tt.R, SubQuery[N, V]]], SelectItem[NamedTuple[N, V]]): JoinQuery[Append[tt.R, SubQuery[N, V]]] =
        joinQueryClause(SqlJoinType.InnerJoin, f(queryItems), true)

    inline def leftJoin[J](using o: ToOption[Table[J]], m: Mirror.ProductOf[J])(using tt: ToTuple[T], s: SelectItem[Append[tt.R, o.R]]): JoinQuery[Append[tt.R, o.R]] =
        joinClause(SqlJoinType.LeftJoin)

    inline def leftJoinQuery[N <: Tuple, V <: Tuple, S <: ResultSize](query: QueryContext ?=> Query[NamedTuple[N, V], S])(using o: ToOption[SubQuery[N, V]], tt: ToTuple[T])(using SelectItem[Append[tt.R, o.R]], SelectItem[NamedTuple[N, V]]): JoinQuery[Append[tt.R, o.R]] =
        joinQueryClause(SqlJoinType.LeftJoin, query)

    inline def leftJoinLateral[N <: Tuple, V <: Tuple, S <: ResultSize](f: QueryContext ?=> T => Query[NamedTuple[N, V], S])(using o: ToOption[SubQuery[N, V]], tt: ToTuple[T])(using SelectItem[Append[tt.R, o.R]], SelectItem[NamedTuple[N, V]]): JoinQuery[Append[tt.R, o.R]] =
        joinQueryClause(SqlJoinType.LeftJoin, f(queryItems), true)

    inline def rightJoin[J](using o: ToOption[T], m: Mirror.ProductOf[J], tt: ToTuple[o.R])(using SelectItem[Append[tt.R, Table[J]]]): JoinQuery[Append[tt.R, Table[J]]] =
        joinClause(SqlJoinType.RightJoin)

    inline def rightJoinQuery[N <: Tuple, V <: Tuple, S <: ResultSize](query: QueryContext ?=> Query[NamedTuple[N, V], S])(using o: ToOption[T], tt: ToTuple[o.R])(using SelectItem[Append[tt.R, SubQuery[N, V]]], SelectItem[NamedTuple[N, V]]): JoinQuery[Append[tt.R, SubQuery[N, V]]] =
        joinQueryClause(SqlJoinType.RightJoin, query)

    inline def rightJoinLateral[N <: Tuple, V <: Tuple, S <: ResultSize](f: QueryContext ?=> T => Query[NamedTuple[N, V], S])(using o: ToOption[SubQuery[N, V]], tt: ToTuple[o.R])(using SelectItem[Append[tt.R, o.R]], SelectItem[NamedTuple[N, V]]): JoinQuery[Append[tt.R, o.R]] =
        joinQueryClause(SqlJoinType.LeftJoin, f(queryItems), true)

    def groupBy[G](f: QueryContext ?=> T => G)(using a: AsExpr[G], na: NotAgg[G], nw: NotWindow[G], nv: NotValue[G], c: CheckGroupByKind[na.R, nw.R, nv.R], t: TransformKind[G, GroupKind]): GroupByQuery[(t.R, T)] =
        val groupByItems = f(queryItems)
        val sqlGroupBy = a.asExprs(groupByItems).map(i => SqlGroupItem.Singleton(i.asSqlExpr))
        GroupByQuery((t.tansform(groupByItems), queryItems), ast.copy(groupBy = sqlGroupBy))

    def groupByCube[G](f: QueryContext ?=> T => G)(using a: AsExpr[G], na: NotAgg[G], nw: NotWindow[G], nv: NotValue[G], c: CheckGroupByKind[na.R, nw.R, nv.R], t: TransformKind[G, GroupKind], to: ToOption[t.R]): GroupByQuery[(to.R, T)] =
        val groupByItems = f(queryItems)
        val sqlGroupBy = SqlGroupItem.Cube(a.asExprs(groupByItems).map(_.asSqlExpr))
        GroupByQuery((to.toOption(t.tansform(groupByItems)), queryItems), ast.copy(groupBy = sqlGroupBy :: Nil))

    def groupByRollup[G](f: QueryContext ?=> T => G)(using a: AsExpr[G], na: NotAgg[G], nw: NotWindow[G], nv: NotValue[G], c: CheckGroupByKind[na.R, nw.R, nv.R], t: TransformKind[G, GroupKind], to: ToOption[t.R]): GroupByQuery[(to.R, T)] =
        val groupByItems = f(queryItems)
        val sqlGroupBy = SqlGroupItem.Rollup(a.asExprs(groupByItems).map(_.asSqlExpr))
        GroupByQuery((to.toOption(t.tansform(groupByItems)), queryItems), ast.copy(groupBy = sqlGroupBy :: Nil))

    inline def groupByGroupingSets[G, S](f: QueryContext ?=> T => G)(using t: TransformKind[G, GroupKind])(g: t.R => S)(using a: AsExpr[G], na: NotAgg[G], nw: NotWindow[G], nv: NotValue[G], c: CheckGroupByKind[na.R, nw.R, nv.R], to: ToOption[t.R], s: GroupingSets[S]): GroupByQuery[(to.R, T)] =
        inline erasedValue[CheckGrouping[S]] match
            case _: false =>
                error("For GROUPING SETS, expressions must appear in grouping list.")
            case _: true =>
        val groupByItems = f(queryItems)
        val changedGroupByItems = t.tansform(groupByItems)
        val sets = g(changedGroupByItems)
        val sqlGroupBy = SqlGroupItem.GroupingSets(s.asSqlExprs(sets))
        GroupByQuery((to.toOption(changedGroupByItems), queryItems), ast.copy(groupBy = sqlGroupBy :: Nil))

class UnionQuery[T](
    private[sqala] val left: Query[?, ?],
    private[sqala] val unionType: SqlUnionType,
    private[sqala] val right: SqlQuery
)(using QueryContext) extends Query[T, ResultSize.ManyRows](left.queryItems.asInstanceOf[T], SqlQuery.Union(left.ast, unionType, right))