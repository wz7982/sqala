package sqala.static.statement.query

import sqala.ast.expr.*
import sqala.ast.limit.SqlLimit
import sqala.ast.group.SqlGroupItem
import sqala.ast.statement.*
import sqala.ast.table.*
import sqala.printer.Dialect
import sqala.static.common.*
import sqala.static.macros.*
import sqala.util.queryToString

import scala.NamedTuple.NamedTuple
import scala.compiletime.constValueTuple
import scala.deriving.Mirror
import scala.util.TupledFunction

sealed class Query[T, S <: ResultSize](val ast: SqlQuery):
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
        Query(newAst)

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
        Query(newAst)

    private[sqala] def size: Query[Long, OneRow] =
        ast match
            case s@SqlQuery.Select(p, _, _, _, Nil, _, _, _) if p != Some(SqlSelectParam.Distinct) =>
                Query(
                    s.copy(
                        select = SqlSelectItem.Item(SqlExpr.Func("COUNT", Nil), None) :: Nil, 
                        limit = None,
                        orderBy = Nil
                    )
                )
            case _ =>
                val outerQuery: SqlQuery.Select = SqlQuery.Select(
                    select = SqlSelectItem.Item(SqlExpr.Func("COUNT", Nil), None) :: Nil,
                    from = SqlTable.SubQueryTable(ast, false, SqlTableAlias("__subquery__")) :: Nil
                )
                Query(outerQuery)

    private[sqala] def exists: Query[Boolean, OneRow] =
        val outerQuery: SqlQuery.Select = SqlQuery.Select(
            select = SqlSelectItem.Item(SqlExpr.SubLink(this.ast, SqlSubLinkType.Exists), None) :: Nil,
            from = Nil
        )
        Query(outerQuery)

    infix def union[R, RS <: ResultSize](query: Query[R, RS])(using 
        u: UnionOperation[T, R]
    ): Query[u.R, ManyRows] =
        UnionQuery(ast, SqlUnionType.Union, query.ast)

    infix def unionAll[R, RS <: ResultSize](query: Query[R, RS])(using 
        u: UnionOperation[T, R]
    ): Query[u.R, ManyRows] =
        UnionQuery(ast, SqlUnionType.UnionAll, query.ast)

    def ++[R, RS <: ResultSize](query: Query[R, RS])(using 
        u: UnionOperation[T, R]
    ): Query[u.R, ManyRows] =
        UnionQuery(ast, SqlUnionType.UnionAll, query.ast)

    infix def intersect[R, RS <: ResultSize](query: Query[R, RS])(using 
        u: UnionOperation[T, R]
    ): Query[u.R, ManyRows] =
        UnionQuery(ast, SqlUnionType.Intersect, query.ast)

    infix def intersectAll[R, RS <: ResultSize](query: Query[R, RS])(using 
        u: UnionOperation[T, R]
    ): Query[u.R, ManyRows] =
        UnionQuery(ast, SqlUnionType.IntersectAll, query.ast)

    infix def except[R, RS <: ResultSize](query: Query[R, RS])(using 
        u: UnionOperation[T, R]
    ): Query[u.R, ManyRows] =
        UnionQuery(ast, SqlUnionType.Except, query.ast)

    infix def exceptAll[R, RS <: ResultSize](query: Query[R, RS])(using 
        u: UnionOperation[T, R]
    ): Query[u.R, ManyRows] =
        UnionQuery(ast, SqlUnionType.ExceptAll, query.ast)

class SelectQuery[T: SelectItem](
    private[sqala] val tables: T,
    private[sqala] val tableNames: List[String],
    override val ast: SqlQuery.Select
)(using val queryContext: QueryContext) extends Query[T, ManyRows](ast):
    inline def filter[F](using
        tt: ToTuple[T],
        tf: TupledFunction[F, tt.R => Boolean]
    )(inline f: F): SelectQuery[T] =
        val args = ClauseMacro.fetchArgNames(f)
        val newParam = if tableNames.isEmpty then args else tableNames
        val newAst = replaceTableName(tables, newParam, ast)
        val cond = ClauseMacro.fetchFilter(f, false, newParam, queryContext)
        SelectQuery(tables, newParam, newAst.addWhere(cond))

    inline def withFilter[F](using
        tt: ToTuple[T],
        tf: TupledFunction[F, tt.R => Boolean]
    )(inline f: F): SelectQuery[T] =
        filter(f)

    inline def filterIf[F](test: => Boolean)(using
        tt: ToTuple[T],
        tf: TupledFunction[F, tt.R => Boolean]
    )(inline f: F): SelectQuery[T] =
        val args = ClauseMacro.fetchArgNames(f)
        val newParam = if tableNames.isEmpty then args else tableNames
        val newAst = replaceTableName(tables, newParam, ast)
        val cond = ClauseMacro.fetchFilter(f, false, newParam, queryContext)
        SelectQuery(tables, newParam, if test then newAst.addWhere(cond) else newAst)

    inline def sortBy[F, S: AsSort](using
        tt: ToTuple[T],
        tf: TupledFunction[F, tt.R => S]
    )(inline f: F): SelectQuery[T] =
        val args = ClauseMacro.fetchArgNames(f)
        val newParam = if tableNames.isEmpty then args else tableNames
        val newAst = replaceTableName(tables, newParam, ast)
        val sortBy = ClauseMacro.fetchSortBy(f, newParam, queryContext)
        SelectQuery(tables, newParam, newAst.copy(orderBy = newAst.orderBy ++ sortBy))

    inline def groupBy[F, N <: Tuple, V <: Tuple : AsExpr](using
        tt: ToTuple[T],
        tf: TupledFunction[F, tt.R => NamedTuple[N, V]],
        tu: ToUngrouped[T],
        tut: ToTuple[tu.R]
    )(inline f: F): GroupBy[Group[N, V] *: tut.R] =
        val args = ClauseMacro.fetchArgNames(f)
        val newParam = if tableNames.isEmpty then args else tableNames
        val newAst = replaceTableName(tables, newParam, ast)
        val groupBy = ClauseMacro.fetchGroupBy(f, newParam, queryContext)
        GroupBy(
            groupBy,
            newAst.copy(groupBy = groupBy.map(g => SqlGroupItem.Singleton(g)))
        )

    inline def groupByCube[F, N <: Tuple, V <: Tuple : AsExpr](using
        tt: ToTuple[T],
        tf: TupledFunction[F, tt.R => NamedTuple[N, V]],
        tu: ToUngrouped[T],
        tut: ToTuple[tu.R]
    )(inline f: F)(using 
        to: ToOption[V],
        tot: ToTuple[to.R]
    ): GroupBy[Group[N, tot.R] *: tut.R] =
        val args = ClauseMacro.fetchArgNames(f)
        val newParam = if tableNames.isEmpty then args else tableNames
        val newAst = replaceTableName(tables, newParam, ast)
        val groupBy = ClauseMacro.fetchGroupBy(f, newParam, queryContext)
        GroupBy(
            groupBy,
            newAst.copy(groupBy = SqlGroupItem.Cube(groupBy) :: Nil)
        )

    inline def groupByRollup[F, N <: Tuple, V <: Tuple : AsExpr](using
        tt: ToTuple[T],
        tf: TupledFunction[F, tt.R => NamedTuple[N, V]],
        tu: ToUngrouped[T],
        tut: ToTuple[tu.R]
    )(inline f: F)(using 
        to: ToOption[V],
        tot: ToTuple[to.R]
    ): GroupBy[Group[N, tot.R] *: tut.R] =
        val args = ClauseMacro.fetchArgNames(f)
        val newParam = if tableNames.isEmpty then args else tableNames
        val newAst = replaceTableName(tables, newParam, ast)
        val groupBy = ClauseMacro.fetchGroupBy(f, newParam, queryContext)
        GroupBy(
            groupBy,
            newAst.copy(groupBy = SqlGroupItem.Rollup(groupBy) :: Nil)
        )

    inline def groupBySets[F, N <: Tuple, V <: Tuple : AsExpr, S](using
        tt: ToTuple[T],
        tf: TupledFunction[F, tt.R => NamedTuple[N, V]],
        tu: ToUngrouped[T],
        tut: ToTuple[tu.R]
    )(inline f: F)(using
        to: ToOption[V],
        tot: ToTuple[to.R]
    )(inline g: Group[N, tot.R] => S)(using
        GroupingSets[S]
    ): GroupBy[Group[N, tot.R] *: tut.R] =
        val args = ClauseMacro.fetchArgNames(f)
        val newParam = if tableNames.isEmpty then args else tableNames
        val newAst = replaceTableName(tables, newParam, ast)
        val groupBy = ClauseMacro.fetchGroupBy(f, newParam, queryContext)
        val groupingArgs = ClauseMacro.fetchArgNames(g)
        queryContext.groups.prepend((groupingArgs.head, groupBy))
        val groupingSets = ClauseMacro.fetchGroupBy(g, groupingArgs, queryContext)
        GroupBy(
            groupBy,
            newAst.copy(groupBy = SqlGroupItem.GroupingSets(groupingSets) :: Nil)
        )

    transparent inline def map[F, M: AsSelectItem](using
        tt: ToTuple[T],
        tf: TupledFunction[F, tt.R => M]
    )(inline f: F): Query[M, ?] =
        val args = ClauseMacro.fetchArgNames(f)
        val newParam = if tableNames.isEmpty then args else tableNames
        val newAst = replaceTableName(tables, newParam, ast)
        ClauseMacro.fetchMap(f, newParam, newAst, queryContext)

class JoinQuery[T: SelectItem](
    private[sqala] override val tables: T,
    private[sqala] override val tableNames: List[String],
    override val ast: SqlQuery.Select
)(using
    override val queryContext: QueryContext
) extends SelectQuery[T](tables, tableNames, ast):
    private inline def joinClause[J, R](
        joinType: SqlJoinType,
        f: Table[J] => R
    )(using
        m: Mirror.ProductOf[J]
    ): JoinPart[R] =
        AsSqlExpr.summonInstances[m.MirroredElemTypes]
        val joinTable = Table[J](TableMacro.tableMetaData[J])
        val joinTableName = TableMacro.tableName[J]
        val tables = f(joinTable)
        val sqlTable = ast.from.headOption.map: i =>
            SqlTable.JoinTable(
                i,
                joinType,
                SqlTable.IdentTable(joinTableName, Some(SqlTableAlias(joinTableName))),
                None
            )
        JoinPart(tables, tableNames, ast.copy(from = sqlTable.toList))

    private inline def joinQueryClause[N <: Tuple, V <: Tuple, S <: ResultSize, R](
        joinType: SqlJoinType,
        query: Query[NamedTuple[N, V], S],
        f: SubQuery[N, V] => R,
        lateral: Boolean = false
    ): JoinPart[R] =
        val joinQuery = SubQuery[N, V](constValueTuple[N].toList.map(_.asInstanceOf[String]))
        val tables = f(joinQuery)
        val sqlTable = ast.from.headOption.map: i =>
            SqlTable.JoinTable(
                i,
                joinType,
                SqlTable.SubQueryTable(query.ast, lateral, SqlTableAlias("")),
                None
            )
        JoinPart(tables, tableNames, ast.copy(from = sqlTable.toList))

    private inline def joinTableQueryClause[J, S <: ResultSize, R](
        joinType: SqlJoinType,
        query: Query[Table[J], S],
        f: TableSubQuery[J] => R,
        lateral: Boolean = false
    ): JoinPart[R] =
        val joinQuery = TableSubQuery[J](TableMacro.tableMetaData[J])
        val tables = f(joinQuery)
        val sqlTable = ast.from.headOption.map: i =>
            SqlTable.JoinTable(
                i,
                joinType,
                SqlTable.SubQueryTable(query.ast, lateral, SqlTableAlias("")),
                None
            )
        JoinPart(tables, tableNames, ast.copy(from = sqlTable.toList))

    inline def join[J](using
        m: Mirror.ProductOf[J],
        tt: ToTuple[T]
    ): JoinPart[Tuple.Append[tt.R, Table[J]]] =
        joinClause[J, Tuple.Append[tt.R, Table[J]]](
            SqlJoinType.Inner,
            j => tt.toTuple(tables) :* j
        )

    inline def join[N <: Tuple, V <: Tuple, S <: ResultSize](joinQuery: Query[NamedTuple[N, V], S])(using
        tt: ToTuple[T]
    ): JoinPart[Tuple.Append[tt.R, SubQuery[N, V]]] =
        joinQueryClause[N, V, S, Tuple.Append[tt.R, SubQuery[N, V]]](
            SqlJoinType.Inner,
            joinQuery,
            j => tt.toTuple(tables) :* j
        )
    
    inline def join[J, S <: ResultSize](joinQuery: Query[Table[J], S])(using
        tt: ToTuple[T]
    ): JoinPart[Tuple.Append[tt.R, TableSubQuery[J]]] =
        joinTableQueryClause[J, S, Tuple.Append[tt.R, TableSubQuery[J]]](
            SqlJoinType.Inner,
            joinQuery,
            j => tt.toTuple(tables) :* j
        )

    inline def leftJoin[J](using
        o: ToOption[Table[J]],
        m: Mirror.ProductOf[J],
        tt: ToTuple[T]
    ): JoinPart[Tuple.Append[tt.R, o.R]] =
        joinClause[J, Tuple.Append[tt.R, o.R]](
            SqlJoinType.Left,
            j => tt.toTuple(tables) :* o.toOption(j)
        )

    inline def leftJoin[N <: Tuple, V <: Tuple, S <: ResultSize](joinQuery: Query[NamedTuple[N, V], S])(using
        o: ToOption[SubQuery[N, V]],
        tt: ToTuple[T]
    ): JoinPart[Tuple.Append[tt.R, o.R]] =
        joinQueryClause[N, V, S, Tuple.Append[tt.R, o.R]](
            SqlJoinType.Left,
            joinQuery,
            j => tt.toTuple(tables) :* o.toOption(j)
        )
    
    inline def leftJoin[J, S <: ResultSize](joinQuery: Query[Table[J], S])(using
        o: ToOption[TableSubQuery[J]],
        tt: ToTuple[T]
    ): JoinPart[Tuple.Append[tt.R, o.R]] =
        joinTableQueryClause[J, S, Tuple.Append[tt.R, o.R]](
            SqlJoinType.Left,
            joinQuery,
            j => tt.toTuple(tables) :* o.toOption(j)
        )
        
    inline def rightJoin[J](using
        o: ToOption[T],
        m: Mirror.ProductOf[J],
        tt: ToTuple[o.R]
    ): JoinPart[Tuple.Append[tt.R, Table[J]]] =
        joinClause[J, Tuple.Append[tt.R, Table[J]]](
            SqlJoinType.Left,
            j => tt.toTuple(o.toOption(tables)) :* j
        )

    inline def rightJoin[N <: Tuple, V <: Tuple, S <: ResultSize](joinQuery: Query[NamedTuple[N, V], S])(using
        o: ToOption[T],
        tt: ToTuple[o.R]
    ): JoinPart[Tuple.Append[tt.R, SubQuery[N, V]]] =
        joinQueryClause[N, V, S, Tuple.Append[tt.R, SubQuery[N, V]]](
            SqlJoinType.Right,
            joinQuery,
            j => tt.toTuple(o.toOption(tables)) :* j
        )
    
    inline def rightJoin[J, S <: ResultSize](joinQuery: Query[Table[J], S])(using
        o: ToOption[T],
        tt: ToTuple[o.R]
    ): JoinPart[Tuple.Append[tt.R, TableSubQuery[J]]] =
        joinTableQueryClause[J, S, Tuple.Append[tt.R, TableSubQuery[J]]](
            SqlJoinType.Right,
            joinQuery,
            j => tt.toTuple(o.toOption(tables)) :* j
        )

class TableQuery[T](
    private[sqala] val table: Table[T],
    override val ast: SqlQuery.Select
)(using 
    override val queryContext: QueryContext
) extends JoinQuery[Table[T]](table, Nil, ast)

class UnionQuery[T](
    private[sqala] val left: SqlQuery,
    private[sqala] val unionType: SqlUnionType,
    private[sqala] val right: SqlQuery
) extends Query[T, ManyRows](
    SqlQuery.Union(left, unionType, right)
)