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

sealed class Query[T, S <: ResultSize](
    private[sqala] val queryItems: T, 
    val ast: SqlQuery
)(using val qc: QueryContext):
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

    def take(n: Int)(using s: QuerySize[n.type]): Query[T, s.R] =
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

    def size: Query[Expr[Long, AggKind], OneRow] =
        val expr = count()
        ast match
            case s@SqlQuery.Select(_, _, _, _, Nil, _, _, _) =>
                Query(expr, s.copy(select = SqlSelectItem.Item(expr.asSqlExpr, None) :: Nil, limit = None))
            case _ =>
                val outerQuery: SqlQuery.Select = SqlQuery.Select(
                    select = SqlSelectItem.Item(expr.asSqlExpr, None) :: Nil,
                    from = SqlTable.SubQueryTable(ast, false, SqlTableAlias("t")) :: Nil
                )
                Query(expr, outerQuery)

    def exists: Query[Expr[Boolean, CommonKind], OneRow] =
        val expr = sqala.dsl.exists(this)
        val outerQuery: SqlQuery.Select = SqlQuery.Select(
            select = SqlSelectItem.Item(expr.asSqlExpr, None) :: Nil,
            from = Nil
        )
        Query(expr, outerQuery)

object Query:
    extension [Q](query: Query[Q, OneRow])(using m: Merge[Q])
        def asExpr: Expr[m.R, CommonKind] = Expr.SubQuery(query.ast)

    extension [T, S <: ResultSize, U, US <: ResultSize](query: Query[T, S])
        infix def union(unionQuery: QueryContext ?=> Query[U, US])(using 
            u: UnionOperation[T, U]
        ): Query[u.R, ManyRows] =
            given QueryContext = query.qc
            UnionQuery(u.unionQueryItems(query.queryItems), query.ast, SqlUnionType.Union, unionQuery.ast)

        infix def unionAll(unionQuery: QueryContext ?=> Query[U, US])(using 
            u: UnionOperation[T, U]
        ): Query[u.R, ManyRows] =
            given QueryContext = query.qc
            UnionQuery(u.unionQueryItems(query.queryItems), query.ast, SqlUnionType.UnionAll, unionQuery.ast)

        def ++(unionQuery: QueryContext ?=> Query[U, US])(using 
            u: UnionOperation[T, U]
        ): Query[u.R, ManyRows] = unionAll(unionQuery)

        infix def except(unionQuery: QueryContext ?=> Query[U, US])(using 
            u: UnionOperation[T, U]
        ): Query[u.R, ManyRows] =
            given QueryContext = query.qc
            UnionQuery(u.unionQueryItems(query.queryItems), query.ast, SqlUnionType.Except, unionQuery.ast)

        infix def exceptAll(unionQuery: QueryContext ?=> Query[U, US])(using 
            u: UnionOperation[T, U]
        ): Query[u.R, ManyRows] =
            given QueryContext = query.qc
            UnionQuery(u.unionQueryItems(query.queryItems), query.ast, SqlUnionType.ExceptAll, unionQuery.ast)

        infix def intersect(unionQuery: QueryContext ?=> Query[U, US])(using 
            u: UnionOperation[T, U]
        ): Query[u.R, ManyRows] =
            given QueryContext = query.qc
            UnionQuery(u.unionQueryItems(query.queryItems), query.ast, SqlUnionType.Intersect, unionQuery.ast)

        infix def intersectAll(unionQuery: QueryContext ?=> Query[U, US])(using 
            u: UnionOperation[T, U]
        ): Query[u.R, ManyRows] =
            given QueryContext = query.qc
            UnionQuery(u.unionQueryItems(query.queryItems), query.ast, SqlUnionType.Intersect, unionQuery.ast)

class ProjectionQuery[T, S <: ResultSize](
    private[sqala] override val queryItems: T,
    override val ast: SqlQuery.Select
)(using QueryContext) extends Query[T, S](queryItems, ast):
    def distinct(using t: TransformKind[T, DistinctKind]): DistinctQuery[t.R, S] =
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
)(using QueryContext) extends Query[T, ManyRows](queryItems, ast):
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

    inline def map[R](f: QueryContext ?=> T => R)(using 
        s: SelectItem[R], 
        a: AsExpr[R], 
        h: HasAgg[R], 
        n: NotAgg[R], 
        c: CheckMapKind[h.R, n.R],
        p: ProjectionSize[h.R]
    ): ProjectionQuery[R, p.R] =
        val mappedItems = f(queryItems)
        val selectItems = s.selectItems(mappedItems, 0)
        ProjectionQuery(mappedItems, ast.copy(select = selectItems))

    private inline def joinClause[J, R](
        joinType: SqlJoinType, 
        f: Table[J] => R
    )(using m: Mirror.ProductOf[J], s: SelectItem[R]): JoinQuery[R] =
        AsSqlExpr.summonInstances[m.MirroredElemTypes]
        val joinTableName = TableMacro.tableName[J]
        qc.tableIndex += 1
        val joinAliasName = s"t${qc.tableIndex}"
        val joinTable = Table[J](joinTableName, joinAliasName, TableMacro.tableMetaData[J])
        val tables = f(joinTable)
        val selectItems = s.selectItems(tables, 0)
        val sqlTable: Option[SqlTable.JoinTable] = ast.from.headOption.map: i =>
            SqlTable.JoinTable(
                i,
                joinType,
                SqlTable.IdentTable(joinTableName, Some(SqlTableAlias(joinAliasName))),
                None
            )
        JoinQuery(tables, sqlTable, ast.copy(select = selectItems, from = sqlTable.toList))

    private def joinQueryClause[N <: Tuple, V <: Tuple, SV <: Tuple, S <: ResultSize, R](
        joinType: SqlJoinType, 
        query: QueryContext ?=> Query[NamedTuple[N, V], S], 
        f: SubQuery[N, SV] => R
    )(using s: SelectItem[R], sq: SelectItem[NamedTuple[N, V]]): JoinQuery[R] =
        qc.tableIndex += 1
        val aliasName = s"t${qc.tableIndex}"
        val joinQuery = query
        val rightTable = SubQuery[N, SV](aliasName, sq.offset(query.queryItems))
        val tables = f(rightTable)
        val sqlTable: Option[SqlTable.JoinTable] = ast.from.headOption.map: i =>
            SqlTable.JoinTable(
                i,
                joinType,
                SqlTable.SubQueryTable(joinQuery.ast, false, SqlTableAlias(aliasName)),
                None
            )
        JoinQuery(tables, sqlTable, ast.copy(select = s.selectItems(tables, 0), from = sqlTable.toList))

    inline def join[J](using 
        m: Mirror.ProductOf[J], 
        tt: ToTuple[T],
        s: SelectItem[Append[tt.R, Table[J]]]
    ): JoinQuery[Append[tt.R, Table[J]]] =
        joinClause[J, Append[tt.R, Table[J]]](
            SqlJoinType.InnerJoin, 
            j => tt.toTuple(queryItems) :* j
        )

    inline def joinQuery[N <: Tuple, V <: Tuple, S <: ResultSize](
        query: QueryContext ?=> Query[NamedTuple[N, V], S]
    )(using 
        tt: ToTuple[T], 
        s: SubQueryKind[V], 
        ts: ToTuple[s.R], 
        si: SelectItem[Append[tt.R, SubQuery[N, ts.R]]], 
        ti: SelectItem[NamedTuple[N, V]]
    ): JoinQuery[Append[tt.R, SubQuery[N, ts.R]]] =
        joinQueryClause[N, V, ts.R, S, Append[tt.R, SubQuery[N, ts.R]]](
            SqlJoinType.InnerJoin, 
            query, 
            j => tt.toTuple(queryItems) :* j
        )

    inline def leftJoin[J](using 
        o: ToOption[Table[J]], 
        m: Mirror.ProductOf[J],
        tt: ToTuple[T], 
        s: SelectItem[Append[tt.R, o.R]]
    ): JoinQuery[Append[tt.R, o.R]] =
        joinClause[J, Append[tt.R, o.R]](
            SqlJoinType.LeftJoin, 
            j => tt.toTuple(queryItems) :* o.toOption(j)
        )

    inline def leftJoinQuery[N <: Tuple, V <: Tuple, S <: ResultSize](
        query: QueryContext ?=> Query[NamedTuple[N, V], S]
    )(using 
        o: ToOption[V], 
        tt: ToTuple[T], 
        s: SubQueryKind[o.R], 
        ts: ToTuple[s.R], 
        si: SelectItem[Append[tt.R, SubQuery[N, ts.R]]], 
        st: SelectItem[NamedTuple[N, V]]
    ): JoinQuery[Append[tt.R, SubQuery[N, ts.R]]] =
        joinQueryClause[N, V, ts.R, S, Append[tt.R, SubQuery[N, ts.R]]](
            SqlJoinType.LeftJoin, query, 
            j => tt.toTuple(queryItems) :* j
        )

    inline def rightJoin[J](using 
        o: ToOption[T], 
        m: Mirror.ProductOf[J], 
        tt: ToTuple[o.R], 
        s: SelectItem[Append[tt.R, Table[J]]]
    ): JoinQuery[Append[tt.R, Table[J]]] =
        joinClause[J, Append[tt.R, Table[J]]](
            SqlJoinType.RightJoin, 
            j => tt.toTuple(o.toOption(queryItems)) :* j
        )

    inline def rightJoinQuery[N <: Tuple, V <: Tuple, S <: ResultSize](
        query: QueryContext ?=> Query[NamedTuple[N, V], S]
    )(using 
        o: ToOption[T], 
        tt: ToTuple[o.R], 
        s: SubQueryKind[V], 
        ts: ToTuple[s.R], 
        si: SelectItem[Append[tt.R, SubQuery[N, ts.R]]], 
        st: SelectItem[NamedTuple[N, V]]
    ): JoinQuery[Append[tt.R, SubQuery[N, ts.R]]] =
        joinQueryClause[N, V, ts.R, S, Append[tt.R, SubQuery[N, ts.R]]](
            SqlJoinType.RightJoin, 
            query, 
            j => tt.toTuple(o.toOption(queryItems)) :* j
        )

    def groupBy[G](f: QueryContext ?=> T => G)(using 
        a: AsExpr[G], 
        na: NotAgg[G], 
        nw: NotWindow[G], 
        nv: NotValue[G], 
        c: CheckGroupByKind[na.R, nw.R, nv.R], 
        t: TransformKind[G, GroupKind]
    ): GroupByQuery[(t.R, T)] =
        val groupByItems = f(queryItems)
        val sqlGroupBy = a.asExprs(groupByItems).map(i => SqlGroupItem.Singleton(i.asSqlExpr))
        GroupByQuery((t.tansform(groupByItems), queryItems), ast.copy(groupBy = sqlGroupBy))

    def groupByCube[G](f: QueryContext ?=> T => G)(using 
        a: AsExpr[G], 
        na: NotAgg[G], 
        nw: NotWindow[G], 
        nv: NotValue[G], 
        c: CheckGroupByKind[na.R, nw.R, nv.R], 
        t: TransformKind[G, GroupKind],
        to: ToOption[t.R]
    ): GroupByQuery[(to.R, T)] =
        val groupByItems = f(queryItems)
        val sqlGroupBy = SqlGroupItem.Cube(a.asExprs(groupByItems).map(_.asSqlExpr)) :: Nil
        GroupByQuery((to.toOption(t.tansform(groupByItems)), queryItems), ast.copy(groupBy = sqlGroupBy))

    def groupByRollup[G](f: QueryContext ?=> T => G)(using 
        a: AsExpr[G], 
        na: NotAgg[G], 
        nw: NotWindow[G], 
        nv: NotValue[G], 
        c: CheckGroupByKind[na.R, nw.R, nv.R], 
        t: TransformKind[G, GroupKind],
        to: ToOption[t.R]
    ): GroupByQuery[(to.R, T)] =
        val groupByItems = f(queryItems)
        val sqlGroupBy = SqlGroupItem.Rollup(a.asExprs(groupByItems).map(_.asSqlExpr)) :: Nil
        GroupByQuery((to.toOption(t.tansform(groupByItems)), queryItems), ast.copy(groupBy = sqlGroupBy))

    def groupByGroupingSets[G, S](f: QueryContext ?=> T => G)(using 
        a: AsExpr[G], 
        na: NotAgg[G], 
        nw: NotWindow[G], 
        nv: NotValue[G], 
        c: CheckGroupByKind[na.R, nw.R, nv.R], 
        t: TransformKind[G, GroupKind],
        to: ToOption[t.R]
    )(g: t.R => S)(using
        gs: GroupingSets[S]
    ): GroupByQuery[(to.R, T)] =
        val groupByItems = t.tansform(f(queryItems))
        val sqlGroupBy = SqlGroupItem.GroupingSets(
            gs.asSqlExprs(g(groupByItems))
        ) :: Nil
        GroupByQuery((to.toOption(groupByItems), queryItems), ast.copy(groupBy = sqlGroupBy))

class UnionQuery[T](
    private[sqala] override val queryItems: T,
    private[sqala] val left: SqlQuery,
    private[sqala] val unionType: SqlUnionType,
    private[sqala] val right: SqlQuery
)(using QueryContext) extends Query[T, ManyRows](
    queryItems,
    SqlQuery.Union(left, unionType, right)
)