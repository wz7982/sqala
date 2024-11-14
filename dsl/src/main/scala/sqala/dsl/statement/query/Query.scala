package sqala.dsl.statement.query

import sqala.ast.expr.SqlExpr
import sqala.ast.group.SqlGroupItem
import sqala.ast.limit.SqlLimit
import sqala.ast.order.SqlOrderBy
import sqala.ast.statement.{SqlQuery, SqlSelectItem, SqlSelectParam, SqlUnionType}
import sqala.ast.table.{SqlJoinType, SqlTableAlias, SqlTable}
import sqala.dsl.*
import sqala.dsl.macros.*
import sqala.printer.Dialect
import sqala.util.queryToString

import scala.NamedTuple.NamedTuple
import scala.Tuple.Append
import scala.compiletime.summonInline
import scala.deriving.Mirror
import scala.util.TupledFunction

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

    def size: Query[Expr[Long], OneRow] =
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

    def exists: Query[Expr[Boolean], OneRow] =
        val expr = sqala.dsl.exists(this)
        val outerQuery: SqlQuery.Select = SqlQuery.Select(
            select = SqlSelectItem.Item(expr.asSqlExpr, None) :: Nil,
            from = Nil
        )
        Query(expr, outerQuery)

object Query:
    extension [N <: Tuple, V <: Tuple, S <: ResultSize, UN <: Tuple, UV <: Tuple, US <: ResultSize](
        query: Query[NamedTuple[N, V], S]
    )
        infix def union(unionQuery: QueryContext ?=> Query[NamedTuple[UN, UV], US])(using
            u: UnionOperation[V, UV],
            tt: ToTuple[u.R]
        ): Query[NamedTuple[N, tt.R], ManyRows] =
            given QueryContext = query.qc
            UnionQuery(tt.toTuple(u.unionQueryItems(query.queryItems)), query.ast, SqlUnionType.Union, unionQuery.ast)

        infix def unionAll(unionQuery: QueryContext ?=> Query[NamedTuple[UN, UV], US])(using
            u: UnionOperation[V, UV],
            tt: ToTuple[u.R]
        ): Query[NamedTuple[N, tt.R], ManyRows] =
            given QueryContext = query.qc
            UnionQuery(tt.toTuple(u.unionQueryItems(query.queryItems)), query.ast, SqlUnionType.UnionAll, unionQuery.ast)

        def ++(unionQuery: QueryContext ?=> Query[NamedTuple[UN, UV], US])(using
            u: UnionOperation[V, UV],
            tt: ToTuple[u.R]
        ): Query[NamedTuple[N, tt.R], ManyRows] = unionAll(unionQuery)

        infix def except(unionQuery: QueryContext ?=> Query[NamedTuple[UN, UV], US])(using
            u: UnionOperation[V, UV],
            tt: ToTuple[u.R]
        ): Query[NamedTuple[N, tt.R], ManyRows] =
            given QueryContext = query.qc
            UnionQuery(tt.toTuple(u.unionQueryItems(query.queryItems)), query.ast, SqlUnionType.Except, unionQuery.ast)

        infix def exceptAll(unionQuery: QueryContext ?=> Query[NamedTuple[UN, UV], US])(using
            u: UnionOperation[V, UV],
            tt: ToTuple[u.R]
        ): Query[NamedTuple[N, tt.R], ManyRows] =
            given QueryContext = query.qc
            UnionQuery(tt.toTuple(u.unionQueryItems(query.queryItems)), query.ast, SqlUnionType.ExceptAll, unionQuery.ast)

        infix def intersect(unionQuery: QueryContext ?=> Query[NamedTuple[UN, UV], US])(using
            u: UnionOperation[V, UV],
            tt: ToTuple[u.R]
        ): Query[NamedTuple[N, tt.R], ManyRows] =
            given QueryContext = query.qc
            UnionQuery(tt.toTuple(u.unionQueryItems(query.queryItems)), query.ast, SqlUnionType.Intersect, unionQuery.ast)

        infix def intersectAll(unionQuery: QueryContext ?=> Query[NamedTuple[UN, UV], US])(using
            u: UnionOperation[V, UV],
            tt: ToTuple[u.R]
        ): Query[NamedTuple[N, tt.R], ManyRows] =
            given QueryContext = query.qc
            UnionQuery(tt.toTuple(u.unionQueryItems(query.queryItems)), query.ast, SqlUnionType.Intersect, unionQuery.ast)

class GroupedProjectionQuery[N <: Tuple, V <: Tuple, S <: ResultSize](
    private[sqala] override val queryItems: NamedTuple[N, V],
    override val ast: SqlQuery.Select
)(using QueryContext) extends Query[NamedTuple[N, V], S](queryItems, ast):
    inline def sortBy[O](
        inline f: QueryContext ?=> Sort[N, V] => OrderBy[O]
    ): GroupedSortQuery[N, V, S] =
        AnalysisMacro.analysisGroupedOrder(f)
        val sort = Sort[N, V](queryItems.toTuple.toList.map(_.asInstanceOf[Expr[?]]))
        val orderBy = f(sort)
        val sqlOrderBy = orderBy.asSqlOrderBy
        GroupedSortQuery(queryItems, ast.copy(orderBy = ast.orderBy :+ sqlOrderBy))

class GroupedSortQuery[N <: Tuple, V <: Tuple, S <: ResultSize](
    private[sqala] override val queryItems: NamedTuple[N, V],
    override val ast: SqlQuery.Select
)(using QueryContext) extends Query[NamedTuple[N, V], S](queryItems, ast):
    inline def sortBy[O](
        inline f: QueryContext ?=> Sort[N, V] => OrderBy[O]
    ): GroupedSortQuery[N, V, S] =
        AnalysisMacro.analysisGroupedOrder(f)
        val sort = Sort[N, V](queryItems.toTuple.toList.map(_.asInstanceOf[Expr[?]]))
        val orderBy = f(sort)
        val sqlOrderBy = orderBy.asSqlOrderBy
        GroupedSortQuery(queryItems, ast.copy(orderBy = ast.orderBy :+ sqlOrderBy))

class ProjectionQuery[N <: Tuple, V <: Tuple, S <: ResultSize](
    private[sqala] override val queryItems: NamedTuple[N, V],
    override val ast: SqlQuery.Select
)(using QueryContext) extends Query[NamedTuple[N, V], S](queryItems, ast):
    def distinct: DistinctQuery[N, V, S] =
        DistinctQuery(queryItems, ast.copy(param = Some(SqlSelectParam.Distinct)))

    inline def sortBy[O](
        inline f: QueryContext ?=> Sort[N, V] => OrderBy[O]
    ): SortQuery[N, V, S] =
        AnalysisMacro.analysisOrder(f)
        val sort = Sort[N, V](queryItems.toTuple.toList.map(_.asInstanceOf[Expr[?]]))
        val orderBy = f(sort)
        val sqlOrderBy = orderBy.asSqlOrderBy
        SortQuery(queryItems, ast.copy(orderBy = ast.orderBy :+ sqlOrderBy))

class SortQuery[N <: Tuple, V <: Tuple, S <: ResultSize](
    private[sqala] override val queryItems: NamedTuple[N, V],
    override val ast: SqlQuery.Select
)(using QueryContext) extends Query[NamedTuple[N, V], S](queryItems, ast):
    inline def sortBy[O](
        inline f: QueryContext ?=> Sort[N, V] => OrderBy[O]
    ): SortQuery[N, V, S] =
        AnalysisMacro.analysisOrder(f)
        val sort = Sort[N, V](queryItems.toTuple.toList.map(_.asInstanceOf[Expr[?]]))
        val orderBy = f(sort)
        val sqlOrderBy = orderBy.asSqlOrderBy
        SortQuery(queryItems, ast.copy(orderBy = ast.orderBy :+ sqlOrderBy))

class DistinctQuery[N <: Tuple, V <: Tuple, S <: ResultSize](
    private[sqala] override val queryItems: NamedTuple[N, V],
    override val ast: SqlQuery.Select
)(using QueryContext) extends Query[NamedTuple[N, V], S](queryItems, ast):
    inline def sortBy[O](
        inline f: QueryContext ?=> Sort[N, V] => OrderBy[O]
    ): DistinctQuery[N, V, S] =
        AnalysisMacro.analysisDistinctOrder(f)
        val sort = Sort[N, V](queryItems.toTuple.toList.map(_.asInstanceOf[Expr[?]]))
        val orderBy = f(sort)
        val sqlOrderBy = orderBy.asSqlOrderBy
        DistinctQuery(queryItems, ast.copy(orderBy = ast.orderBy :+ sqlOrderBy))

class SelectQuery[T](
    private[sqala] override val queryItems: T,
    override val ast: SqlQuery.Select
)(using override val qc: QueryContext) extends Query[T, ManyRows](queryItems, ast):
    inline def filter[F](using
        tt: ToTuple[T],
        t: TupledFunction[F, tt.R => Expr[Boolean]]
    )(inline f: QueryContext ?=> F): SelectQuery[T] =
        AnalysisMacro.analysisFilter(f)
        val func = t.tupled(f)
        val condition = func(tt.toTuple(queryItems)).asSqlExpr
        SelectQuery(queryItems, ast.addWhere(condition))

    inline def withFilter[F](using
        tt: ToTuple[T],
        t: TupledFunction[F, tt.R => Expr[Boolean]]
    )(inline f: QueryContext ?=> F): SelectQuery[T] =
        filter(f)

    inline def filterIf[F](using
        tt: ToTuple[T],
        t: TupledFunction[F, tt.R => Expr[Boolean]]
    )(test: Boolean)(inline f: QueryContext ?=> F): SelectQuery[T] =
        if test then filter(f) else this

    transparent inline def map[F, N <: Tuple, V <: Tuple](using
        tt: ToTuple[T],
        t: TupledFunction[F, tt.R => NamedTuple[N, V]],
    )(inline f: QueryContext ?=> F)(using 
        s: SelectItem[V],
        a: SelectItemAsExpr[V]
    ): ProjectionQuery[N, a.R, ?] =
        val func = t.tupled(f)
        val mappedItems = func(tt.toTuple(queryItems))
        val selectItems = s.selectItems(mappedItems, 0)
        val newAst: SqlQuery.Select = ast.copy(select = selectItems)
        AnalysisMacro.analysisSelect(f, a.asExpr(mappedItems), newAst, qc)

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
    )(using s: SelectItem[R], sq: SelectItem[V]): JoinQuery[R] =
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
        ts: ToTuple[V],
        si: SelectItem[Append[tt.R, SubQuery[N, ts.R]]],
        ti: SelectItem[V]
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
        ts: ToTuple[o.R],
        si: SelectItem[Append[tt.R, SubQuery[N, ts.R]]],
        st: SelectItem[V]
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
        ts: ToTuple[V],
        si: SelectItem[Append[tt.R, SubQuery[N, ts.R]]],
        st: SelectItem[V]
    ): JoinQuery[Append[tt.R, SubQuery[N, ts.R]]] =
        joinQueryClause[N, V, ts.R, S, Append[tt.R, SubQuery[N, ts.R]]](
            SqlJoinType.RightJoin,
            query,
            j => tt.toTuple(o.toOption(queryItems)) :* j
        )

    inline def groupBy[F, N <: Tuple, V <: Tuple](using
        tt: ToTuple[T],
        t: TupledFunction[F, tt.R => NamedTuple[N, V]]
    )(inline f: QueryContext ?=> F)(using 
        a: AsExpr[V],
        tu: ToUngroup[T],
        tut: ToTuple[tu.R]
    ): GroupByQuery[Group[N, V] *: tut.R] =
        AnalysisMacro.analysisGroup(f)
        val func = t.tupled(f)
        val groupByItems = func(tt.toTuple(queryItems))
        val groupExprs = a.asExprs(groupByItems)
        val group: Group[N, V] = Group(groupExprs)
        val sqlGroupBy = groupExprs
            .map(i => SqlGroupItem.Singleton(i.asSqlExpr))
        GroupByQuery(group *: tut.toTuple(tu.toUngroup(queryItems)), ast.copy(groupBy = sqlGroupBy))

    inline def groupByCube[F, N <: Tuple, V <: Tuple](using
        tt: ToTuple[T],
        t: TupledFunction[F, tt.R => NamedTuple[N, V]]
    )(inline f: QueryContext ?=> F)(using 
        a: AsExpr[V],
        tu: ToUngroup[T],
        tut: ToTuple[tu.R],
        to: ToOption[V],
        tot: ToTuple[to.R]
    ): GroupByQuery[Group[N, tot.R] *: tut.R] =
        AnalysisMacro.analysisGroup(f)
        val func = t.tupled(f)
        val groupByItems = func(tt.toTuple(queryItems))
        val groupExprs = a.asExprs(groupByItems)
        val group: Group[N, tot.R] = Group(groupExprs)
        val sqlGroupBy =
            SqlGroupItem.Cube(groupExprs.map(_.asSqlExpr)) :: Nil
        GroupByQuery(group *: tut.toTuple(tu.toUngroup(queryItems)), ast.copy(groupBy = sqlGroupBy))

    inline def groupByRollup[F, N <: Tuple, V <: Tuple](using
        tt: ToTuple[T],
        t: TupledFunction[F, tt.R => NamedTuple[N, V]]
    )(inline f: QueryContext ?=> F)(using 
        a: AsExpr[V],
        tu: ToUngroup[T],
        tut: ToTuple[tu.R],
        to: ToOption[V],
        tot: ToTuple[to.R]
    ): GroupByQuery[Group[N, tot.R] *: tut.R] =
        AnalysisMacro.analysisGroup(f)
        val func = t.tupled(f)
        val groupByItems = func(tt.toTuple(queryItems))
        val groupExprs = a.asExprs(groupByItems)
        val group: Group[N, tot.R] = Group(groupExprs)
        val sqlGroupBy =
            SqlGroupItem.Rollup(groupExprs.map(_.asSqlExpr)) :: Nil
        GroupByQuery(group *: tut.toTuple(tu.toUngroup(queryItems)), ast.copy(groupBy = sqlGroupBy))

    inline def groupByGroupingSets[F, N <: Tuple, V <: Tuple, S](using
        tt: ToTuple[T],
        t: TupledFunction[F, tt.R => NamedTuple[N, V]]
    )(inline f: QueryContext ?=> F)(using 
        a: AsExpr[V],
        tu: ToUngroup[T],
        tut: ToTuple[tu.R],
        to: ToOption[V],
        tot: ToTuple[to.R]
    )(g: Group[N, tot.R] => S)(using
        gs: GroupingSets[S]
    ): GroupByQuery[Group[N, tot.R] *: tut.R] =
        AnalysisMacro.analysisGroup(f)
        val func = t.tupled(f)
        val groupByItems = func(tt.toTuple(queryItems))
        val groupExprs = a.asExprs(groupByItems)
        val group: Group[N, tot.R] = Group(groupExprs)
        val sqlGroupBy =
            SqlGroupItem.GroupingSets(gs.asSqlExprs(g(group))) :: Nil
        GroupByQuery(group *: tut.toTuple(tu.toUngroup(queryItems)), ast.copy(groupBy = sqlGroupBy))

class UnionQuery[T](
    private[sqala] override val queryItems: T,
    private[sqala] val left: SqlQuery,
    private[sqala] val unionType: SqlUnionType,
    private[sqala] val right: SqlQuery
)(using QueryContext) extends Query[T, ManyRows](
    queryItems,
    SqlQuery.Union(left, unionType, right)
)