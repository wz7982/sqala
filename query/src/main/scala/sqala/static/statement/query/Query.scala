package sqala.static.statement.query

import sqala.ast.expr.*
import sqala.ast.limit.SqlLimit
import sqala.ast.group.SqlGroupItem
import sqala.ast.param.SqlParam
import sqala.ast.statement.*
import sqala.ast.table.*
import sqala.common.AsSqlExpr
import sqala.macros.TableMacro
import sqala.printer.Dialect
import sqala.static.dsl.*
import sqala.util.queryToString

import scala.NamedTuple.NamedTuple
import scala.Tuple.Append
import scala.deriving.Mirror

sealed class Query[T](
    private[sqala] val queryParam: T,
    val ast: SqlQuery
)(using private[sqala] val context: QueryContext):
    def sql(dialect: Dialect, enableJdbcPrepare: Boolean): (String, Array[Any]) =
        queryToString(ast, dialect, enableJdbcPrepare)

    def drop(n: Int): Query[T] =
        val limit = ast match
            case s: SqlQuery.Select => s.limit
            case u: SqlQuery.Union => u.limit
            case SqlQuery.Cte(_, _, s: SqlQuery.Select) => s.limit
            case SqlQuery.Cte(_, _, u: SqlQuery.Union) => u.limit
            case _ => None
        val sqlLimit = limit
            .map(l => SqlLimit(l.limit, SqlExpr.NumberLiteral(n)))
            .orElse(Some(SqlLimit(SqlExpr.NumberLiteral(Long.MaxValue), SqlExpr.NumberLiteral(n))))
        val newAst = ast match
            case s: SqlQuery.Select => s.copy(limit = sqlLimit)
            case u: SqlQuery.Union => u.copy(limit = sqlLimit)
            case SqlQuery.Cte(w, r, s: SqlQuery.Select) =>
                SqlQuery.Cte(w, r, s.copy(limit = sqlLimit))
            case SqlQuery.Cte(w, r, u: SqlQuery.Union) =>
                SqlQuery.Cte(w, r, u.copy(limit = sqlLimit))
            case _ => ast
        Query(queryParam, newAst)

    def offset(n: Int): Query[T] = drop(n)

    def take(n: Int): Query[T] =
        val limit = ast match
            case s: SqlQuery.Select => s.limit
            case u: SqlQuery.Union => u.limit
            case SqlQuery.Cte(_, _, s: SqlQuery.Select) => s.limit
            case SqlQuery.Cte(_, _, u: SqlQuery.Union) => u.limit
            case _ => None
        val sqlLimit = limit
            .map(l => SqlLimit(SqlExpr.NumberLiteral(n), l.offset))
            .orElse(Some(SqlLimit(SqlExpr.NumberLiteral(n), SqlExpr.NumberLiteral(0))))
        val newAst = ast match
            case s: SqlQuery.Select => s.copy(limit = sqlLimit)
            case u: SqlQuery.Union => u.copy(limit = sqlLimit)
            case SqlQuery.Cte(w, r, s: SqlQuery.Select) =>
                SqlQuery.Cte(w, r, s.copy(limit = sqlLimit))
            case SqlQuery.Cte(w, r, u: SqlQuery.Union) =>
                SqlQuery.Cte(w, r, u.copy(limit = sqlLimit))
            case _ => ast
        Query(queryParam, newAst)

    def limit(n: Int): Query[T] = take(n)

    def distinct: Query[T] =
        val newAst = ast match
            case s: SqlQuery.Select =>
                s.copy(param = Some(SqlParam.Distinct))
            case _ => ast
        Query(queryParam, newAst)

    private[sqala] def size: Query[Expr[Long]] =
        val expr = count()
        ast match
            case s@SqlQuery.Select(p, _, _, _, Nil, _, _, _) if p != Some(SqlParam.Distinct) =>
                Query(expr, s.copy(select = SqlSelectItem.Item(expr.asSqlExpr, None) :: Nil, limit = None))
            case _ =>
                val outerQuery: SqlQuery.Select = SqlQuery.Select(
                    select = SqlSelectItem.Item(expr.asSqlExpr, None) :: Nil,
                    from = SqlTable.SubQuery(ast, false, Some(SqlTableAlias("t"))) :: Nil
                )
                Query(expr, outerQuery)

    private[sqala] def exists: Query[Expr[Boolean]] =
        val expr = Expr.SubLink[Boolean](this.ast, SqlSubLinkType.Exists)
        val outerQuery: SqlQuery.Select = SqlQuery.Select(
            select = SqlSelectItem.Item(expr.asSqlExpr, None) :: Nil,
            from = Nil
        )
        Query(expr, outerQuery)

    infix def union[R](query: Query[R])(using u: UnionOperation[T, R]): Query[u.R] =
        UnionQuery(u.unionQueryItems(queryParam), this.ast, SqlUnionType.Union, query.ast)

    infix def unionAll[R](query: Query[R])(using u: UnionOperation[T, R]): Query[u.R] =
        UnionQuery(u.unionQueryItems(queryParam), this.ast, SqlUnionType.UnionAll, query.ast)

    def ++[R](query: Query[R])(using u: UnionOperation[T, R]): Query[u.R] =
        UnionQuery(u.unionQueryItems(queryParam), this.ast, SqlUnionType.UnionAll, query.ast)

    infix def except[R](query: Query[R])(using u: UnionOperation[T, R]): Query[u.R] =
        UnionQuery(u.unionQueryItems(queryParam), this.ast, SqlUnionType.Except, query.ast)

    infix def exceptAll[R](query: Query[R])(using u: UnionOperation[T, R]): Query[u.R] =
        UnionQuery(u.unionQueryItems(queryParam), this.ast, SqlUnionType.ExceptAll, query.ast)

    infix def intersect[R](query: Query[R])(using u: UnionOperation[T, R]): Query[u.R] =
        UnionQuery(u.unionQueryItems(queryParam), this.ast, SqlUnionType.Intersect, query.ast)

    infix def intersectAll[R](query: Query[R])(using u: UnionOperation[T, R]): Query[u.R] =
        UnionQuery(u.unionQueryItems(queryParam), this.ast, SqlUnionType.IntersectAll, query.ast)

class SortQuery[T](
    private[sqala] override val queryParam: T,
    override val ast: SqlQuery.Select
)(using
    private[sqala] override val context: QueryContext
) extends Query[T](queryParam, ast)(using context):
    def sortBy[S](f: QueryContext ?=> T => S)(using s: AsSort[S]): SortQuery[T] =
        val sort = f(queryParam)
        val orderBy = s.asSort(sort).map(_.asSqlOrderBy)
        SortQuery(queryParam, ast.copy(orderBy = ast.orderBy ++ orderBy))

    def orderBy[S](f: QueryContext ?=> T => S)(using s: AsSort[S]): SortQuery[T] =
        sortBy(f)

    def map[M](f: QueryContext ?=> T => M)(using s: AsSelect[M]): Query[s.R] =
        val mapped = f(queryParam)
        val sqlSelect = s.selectItems(mapped, 1)
        Query(s.transform(mapped), ast.copy(select = sqlSelect))

    def select[M](f: QueryContext ?=> T => M)(using s: AsSelect[M]): Query[s.R] =
        map(f)

class SelectQuery[T](
    private[sqala] override val queryParam: T,
    override val ast: SqlQuery.Select
)(using
    private[sqala] override val context: QueryContext
) extends Query[T](queryParam, ast)(using context):
    def filter(f: QueryContext ?=> T => Expr[Boolean]): SelectQuery[T] =
        val cond = f(queryParam)
        SelectQuery(queryParam, ast.addWhere(cond.asSqlExpr))

    def where(f: QueryContext ?=> T => Expr[Boolean]): SelectQuery[T] =
        filter(f)

    def withFilter(f: QueryContext ?=> T => Expr[Boolean]): SelectQuery[T] =
        filter(f)

    def filterIf(test: => Boolean)(f: QueryContext ?=> T => Expr[Boolean]): SelectQuery[T] =
        if test then filter(f) else this

    def whereIf(test: => Boolean)(f: QueryContext ?=> T => Expr[Boolean]): SelectQuery[T] =
        if test then where(f) else this

    def sortBy[S](f: QueryContext ?=> T => S)(using s: AsSort[S]): SortQuery[T] =
        val sort = f(queryParam)
        val sqlOrderBy = s.asSort(sort).map(_.asSqlOrderBy)
        SortQuery(queryParam, ast.copy(orderBy = ast.orderBy ++ sqlOrderBy))

    def orderBy[S](f: QueryContext ?=> T => S)(using s: AsSort[S]): SortQuery[T] =
        sortBy(f)

    def groupBy[G](f: QueryContext ?=> T => G)(using
        e: AsExpr[G]
    ): Grouping[T] =
        val group = f(queryParam)
        val sqlGroupBy = e.exprs(group).map(_.asSqlExpr)
        Grouping(queryParam, ast.copy(groupBy = sqlGroupBy.map(g => SqlGroupItem.Singleton(g))))

    def groupByCube[G](f: QueryContext ?=> T => G)(using
        o: ToOption[T],
        e: AsExpr[G]
    ): Grouping[o.R] =
        val group = f(queryParam)
        val sqlGroupBy = e.exprs(group).map(_.asSqlExpr)
        Grouping(o.toOption(queryParam), ast.copy(groupBy = SqlGroupItem.Cube(sqlGroupBy) :: Nil))

    def groupByRollup[G](f: QueryContext ?=> T => G)(using
        o: ToOption[T],
        e: AsExpr[G]
    ): Grouping[o.R] =
        val group = f(queryParam)
        val sqlGroupBy = e.exprs(group).map(_.asSqlExpr)
        Grouping(o.toOption(queryParam), ast.copy(groupBy = SqlGroupItem.Rollup(sqlGroupBy) :: Nil))

    def groupBySets[G](f: QueryContext ?=> T => G)(using
        o: ToOption[T],
        g: GroupingSets[G]
    ): Grouping[o.R] =
        val group = f(queryParam)
        val sqlGroupBy = g.asSqlExprs(group)
        Grouping(o.toOption(queryParam), ast.copy(groupBy = SqlGroupItem.GroupingSets(sqlGroupBy) :: Nil))

    def map[M](f: QueryContext ?=> T => M)(using s: AsSelect[M]): Query[s.R] =
        val mapped = f(queryParam)
        val sqlSelect = s.selectItems(mapped, 1)
        Query(s.transform(mapped), ast.copy(select = sqlSelect))

    def select[M](f: QueryContext ?=> T => M)(using s: AsSelect[M]): Query[s.R] =
        map(f)

    def pivot[N <: Tuple, V <: Tuple : AsExpr as a](f: QueryContext ?=> T => NamedTuple[N, V]): Pivot[T, N, V] =
        val functions = a.exprs(f(queryParam).toTuple)
            .map(e => e.asSqlExpr.asInstanceOf[SqlExpr.Func])
        Pivot[T, N, V](queryParam, functions, ast)

object SelectQuery:
    extension [T](query: SelectQuery[Table[T]])
        def connectBy(f: QueryContext ?=> Table[T] => Expr[Boolean]): ConnectBy[T] =
            given QueryContext = query.context
            val cond = f(query.queryParam).asSqlExpr
            val joinAst = query.ast
                .copy(
                    select = query.ast.select :+ SqlSelectItem.Item(
                        SqlExpr.Binary(
                            SqlExpr.Column(Some(tableCte), columnPseudoLevel),
                            SqlBinaryOperator.Plus,
                            SqlExpr.NumberLiteral(1)
                        ),
                        Some(columnPseudoLevel)
                    ),
                    from = SqlTable.Join(
                        query.ast.from.head,
                        SqlJoinType.Inner,
                        SqlTable.Range(tableCte, None),
                        Some(SqlJoinCondition.On(cond)),
                        None
                    ) :: Nil
                )
            val startAst = query.ast
                .copy(
                    select = query.ast.select :+ SqlSelectItem.Item(
                        SqlExpr.NumberLiteral(1),
                        Some(columnPseudoLevel)
                    )
                )
            ConnectBy(query.queryParam, joinAst, startAst)

class JoinQuery[T](
    private[sqala] override val queryParam: T,
    override val ast: SqlQuery.Select
)(using
    private[sqala] override val context: QueryContext
) extends SelectQuery[T](queryParam, ast)(using context):
    private inline def joinClause[J, R](
        joinType: SqlJoinType,
        f: Table[J] => R
    )(using
        m: Mirror.ProductOf[J],
        s: AsSelect[R]
    ): JoinPart[R] =
        AsSqlExpr.summonInstances[m.MirroredElemTypes]
        val joinTableName = TableMacro.tableName[J]
        context.tableIndex += 1
        val joinAliasName = s"t${context.tableIndex}"
        val joinTable = Table[J](joinTableName, joinAliasName, TableMacro.tableMetaData[J])
        val tables = f(joinTable)
        val selectItems = s.selectItems(tables, 1)
        val sqlTable: SqlTable.Join =
            SqlTable.Join(
                ast.from.head,
                joinType,
                SqlTable.Range(joinTableName, Some(SqlTableAlias(joinAliasName))),
                None,
                None
            )
        JoinPart(tables, sqlTable, ast.copy(select = selectItems, from = sqlTable :: Nil))

    private def joinQueryClause[N <: Tuple, V <: Tuple, SV <: Tuple, R](
        joinType: SqlJoinType,
        query: QueryContext ?=> Query[NamedTuple[N, V]],
        f: SubQuery[N, SV] => R,
        vf: V => SV,
        lateral: Boolean = false
    )(using
        s: AsSelect[R],
        sq: AsSubQuery[SV]
    ): JoinPart[R] =
        val q = query
        val rightTable = SubQuery[N, SV](vf(q.queryParam))
        val tables = f(rightTable)
        val sqlTable: SqlTable.Join =
            SqlTable.Join(
                ast.from.head,
                joinType,
                SqlTable.SubQuery(q.ast, lateral, Some(SqlTableAlias(rightTable.__alias__))),
                None,
                None
            )
        JoinPart(tables, sqlTable, ast.copy(select = s.selectItems(tables, 1), from = sqlTable :: Nil))

    inline def join[J](using
        m: Mirror.ProductOf[J],
        tt: ToTuple[T],
        s: AsSelect[Append[tt.R, Table[J]]]
    ): JoinPart[Append[tt.R, Table[J]]] =
        joinClause[J, Append[tt.R, Table[J]]](
            SqlJoinType.Inner,
            j => tt.toTuple(queryParam) :* j
        )

    def joinQuery[N <: Tuple, V <: Tuple](
        query: QueryContext ?=> Query[NamedTuple[N, V]]
    )(using
        tt: ToTuple[T],
        ts: ToTuple[V],
        s: AsSelect[Append[tt.R, SubQuery[N, ts.R]]],
        sq: AsSubQuery[ts.R]
    ): JoinPart[Append[tt.R, SubQuery[N, ts.R]]] =
        joinQueryClause[N, V, ts.R, Append[tt.R, SubQuery[N, ts.R]]](
            SqlJoinType.Inner,
            query,
            j => tt.toTuple(queryParam) :* j,
            v => ts.toTuple(v)
        )

    def joinLateral[N <: Tuple, V <: Tuple](
        query: QueryContext ?=> T => Query[NamedTuple[N, V]]
    )(using
        tt: ToTuple[T],
        ts: ToTuple[V],
        s: AsSelect[Append[tt.R, SubQuery[N, ts.R]]],
        sq: AsSubQuery[ts.R]
    ): JoinPart[Append[tt.R, SubQuery[N, ts.R]]] =
        joinQueryClause[N, V, ts.R, Append[tt.R, SubQuery[N, ts.R]]](
            SqlJoinType.Inner,
            query(queryParam),
            j => tt.toTuple(queryParam) :* j,
            v => ts.toTuple(v)
        )

    inline def leftJoin[J](using
        o: ToOption[Table[J]],
        m: Mirror.ProductOf[J],
        tt: ToTuple[T],
        s: AsSelect[Append[tt.R, o.R]]
    ): JoinPart[Append[tt.R, o.R]] =
        joinClause[J, Append[tt.R, o.R]](
            SqlJoinType.Left,
            j => tt.toTuple(queryParam) :* o.toOption(j)
        )

    def leftJoinQuery[N <: Tuple, V <: Tuple](
        query: QueryContext ?=> Query[NamedTuple[N, V]]
    )(using
        o: ToOption[V],
        tt: ToTuple[T],
        ts: ToTuple[o.R],
        si: AsSelect[Append[tt.R, SubQuery[N, ts.R]]],
        sq: AsSubQuery[ts.R]
    ): JoinPart[Append[tt.R, SubQuery[N, ts.R]]] =
        joinQueryClause[N, V, ts.R, Append[tt.R, SubQuery[N, ts.R]]](
            SqlJoinType.Left,
            query,
            j => tt.toTuple(queryParam) :* j,
            v => ts.toTuple(o.toOption(v))
        )

    def leftJoinLateral[N <: Tuple, V <: Tuple](
        query: QueryContext ?=> T => Query[NamedTuple[N, V]]
    )(using
        o: ToOption[V],
        tt: ToTuple[T],
        ts: ToTuple[o.R],
        si: AsSelect[Append[tt.R, SubQuery[N, ts.R]]],
        sq: AsSubQuery[ts.R]
    ): JoinPart[Append[tt.R, SubQuery[N, ts.R]]] =
        joinQueryClause[N, V, ts.R, Append[tt.R, SubQuery[N, ts.R]]](
            SqlJoinType.Left,
            query(queryParam),
            j => tt.toTuple(queryParam) :* j,
            v => ts.toTuple(o.toOption(v))
        )

    inline def rightJoin[J](using
        o: ToOption[T],
        m: Mirror.ProductOf[J],
        tt: ToTuple[o.R],
        s: AsSelect[Append[tt.R, Table[J]]]
    ): JoinPart[Append[tt.R, Table[J]]] =
        joinClause[J, Append[tt.R, Table[J]]](
            SqlJoinType.Right,
            j => tt.toTuple(o.toOption(queryParam)) :* j
        )

    def rightJoinQuery[N <: Tuple, V <: Tuple](
        query: QueryContext ?=> Query[NamedTuple[N, V]]
    )(using
        o: ToOption[T],
        tt: ToTuple[o.R],
        ts: ToTuple[V],
        si: AsSelect[Append[tt.R, SubQuery[N, ts.R]]],
        sq: AsSubQuery[ts.R]
    ): JoinPart[Append[tt.R, SubQuery[N, ts.R]]] =
        joinQueryClause[N, V, ts.R, Append[tt.R, SubQuery[N, ts.R]]](
            SqlJoinType.Right,
            query,
            j => tt.toTuple(o.toOption(queryParam)) :* j,
            v => ts.toTuple(v)
        )

class TableQuery[T](
    private[sqala] val table: Table[T],
    override val ast: SqlQuery.Select
)(using
    private[sqala] override val context: QueryContext
) extends JoinQuery[Table[T]](table, ast)(using context)

class UnionQuery[T](
    private[sqala] override val queryParam: T,
    private[sqala] val left: SqlQuery,
    private[sqala] val unionType: SqlUnionType,
    private[sqala] val right: SqlQuery
)(using QueryContext) extends Query[T](
    queryParam,
    SqlQuery.Union(left, unionType, right)
)