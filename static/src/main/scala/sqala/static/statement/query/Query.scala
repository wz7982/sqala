package sqala.static.statement.query

import sqala.ast.expr.*
import sqala.ast.limit.SqlLimit
import sqala.ast.group.SqlGroupItem
import sqala.ast.param.SqlParam
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
            case SqlQuery.Cte(_, _, s: SqlQuery.Select) => s.limit
            case SqlQuery.Cte(_, _, u: SqlQuery.Union) => u.limit
            case _ => None
        val sqlLimit = limit
            .map(l => SqlLimit(l.limit, SqlExpr.NumberLiteral(n)))
            .orElse(Some(SqlLimit(SqlExpr.NumberLiteral(1), SqlExpr.NumberLiteral(n))))
        val newAst = ast match
            case s: SqlQuery.Select => s.copy(limit = sqlLimit)
            case u: SqlQuery.Union => u.copy(limit = sqlLimit)
            case SqlQuery.Cte(w, r, s: SqlQuery.Select) =>
                SqlQuery.Cte(w, r, s.copy(limit = sqlLimit))
            case SqlQuery.Cte(w, r, u: SqlQuery.Union) =>
                SqlQuery.Cte(w, r, u.copy(limit = sqlLimit))
            case _ => ast
        Query(newAst)

    def take(n: Int)(using s: QuerySize[n.type]): Query[T, s.R] =
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
        Query(newAst)

    private[sqala] def size: Query[Long, OneRow] =
        ast match
            case s@SqlQuery.Select(p, _, _, _, Nil, _, _, _) if p != Some(SqlParam.Distinct) =>
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
                    from = SqlTable.SubQuery(ast, false, Some(SqlTableAlias(tableSubquery))) :: Nil
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

class SortQuery[T](
    private[sqala] val tables: T,
    private[sqala] val tableNames: List[String],
    override val ast: SqlQuery.Select
)(using val queryContext: QueryContext) extends Query[T, ManyRows](ast):
    inline def sortBy[F, S: AsSort](using
        tt: ToTuple[T],
        tf: TupledFunction[F, tt.R => S]
    )(inline f: F): SortQuery[T] =
        val sortBy = ClauseMacro.fetchSortBy(f, false, false, tableNames, queryContext)
        SortQuery(tables, tableNames, ast.copy(orderBy = ast.orderBy ++ sortBy))

    transparent inline def map[F, M: AsSelectItem](using
        tt: ToTuple[T],
        tf: TupledFunction[F, tt.R => M]
    )(inline f: F): Query[M, ?] =
        ClauseMacro.fetchMap(f, false, tableNames, ast, queryContext)

class ProjectionQuery[T, S <: ResultSize](
    override val ast: SqlQuery.Select
)(using val queryContext: QueryContext) extends Query[T, S](ast):
    def distinct: Query[T, S] =
        val newSortBy = ast.orderBy.filter: o =>
            val expr = o.expr
            ast.select.exists:
                case SqlSelectItem.Item(e, _) if e == expr => true
                case _ => false
        Query(ast.copy(param = Some(SqlParam.Distinct), orderBy =  newSortBy))

object ProjectionQuery:
    extension [N <: Tuple, V <: Tuple, S <: ResultSize](query: ProjectionQuery[NamedTuple[N, V], S])
        inline def qualify(inline f: SubQuery[N, V] => Boolean): QualifyQuery[N, V, ManyRows] =
            val selectItems = query.ast.select.map:
                case SqlSelectItem.Item(_, Some(n)) =>
                    SqlSelectItem.Item(SqlExpr.Column(Some(tableSubquery), n), Some(n))
                case i => i
            val from = SqlTable.SubQuery(query.ast, false, Some(SqlTableAlias(tableSubquery)))
            val cond = ClauseMacro.fetchFilter(f, false, false, tableSubquery :: Nil, query.queryContext)
            QualifyQuery(SqlQuery.Select(select = selectItems, from = from :: Nil, where = Some(cond)))(using query.queryContext)

class QualifyQuery[N <: Tuple, V <: Tuple, S <: ResultSize](
    override val ast: SqlQuery.Select
)(using val queryContext: QueryContext) extends Query[NamedTuple[N, V], S](ast):
    inline def qualify(inline f: SubQuery[N, V] => Boolean): QualifyQuery[N, V, ManyRows] =
        val cond = ClauseMacro.fetchFilter(f, false, false, tableSubquery :: Nil, queryContext)
        QualifyQuery(ast.addWhere(cond))

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
        val cond = ClauseMacro.fetchFilter(f, false, false, newParam, queryContext)
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
        val cond = ClauseMacro.fetchFilter(f, false, false, newParam, queryContext)
        SelectQuery(tables, newParam, if test then newAst.addWhere(cond) else newAst)

    inline def sortBy[F, S: AsSort](using
        tt: ToTuple[T],
        tf: TupledFunction[F, tt.R => S]
    )(inline f: F): SortQuery[T] =
        val args = ClauseMacro.fetchArgNames(f)
        val newParam = if tableNames.isEmpty then args else tableNames
        val newAst = replaceTableName(tables, newParam, ast)
        val sortBy = ClauseMacro.fetchSortBy(f, false, false, newParam, queryContext)
        SortQuery(tables, newParam, newAst.copy(orderBy = newAst.orderBy ++ sortBy))

    inline def groupBy[F, N <: Tuple, V <: Tuple : AsExpr](using
        tt: ToTuple[T],
        tf: TupledFunction[F, tt.R => NamedTuple[N, V]],
        tu: ToUngrouped[T],
        tut: ToTuple[tu.R]
    )(inline f: F): GroupByQuery[Group[N, V] *: tut.R] =
        val args = ClauseMacro.fetchArgNames(f)
        val newParam = if tableNames.isEmpty then args else tableNames
        val newAst = replaceTableName(tables, newParam, ast)
        val groupBy = ClauseMacro.fetchGroupBy(f, newParam, queryContext)
        GroupByQuery(
            groupBy,
            newParam,
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
    ): GroupByQuery[Group[N, tot.R] *: tut.R] =
        val args = ClauseMacro.fetchArgNames(f)
        val newParam = if tableNames.isEmpty then args else tableNames
        val newAst = replaceTableName(tables, newParam, ast)
        val groupBy = ClauseMacro.fetchGroupBy(f, newParam, queryContext)
        GroupByQuery(
            groupBy,
            newParam,
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
    ): GroupByQuery[Group[N, tot.R] *: tut.R] =
        val args = ClauseMacro.fetchArgNames(f)
        val newParam = if tableNames.isEmpty then args else tableNames
        val newAst = replaceTableName(tables, newParam, ast)
        val groupBy = ClauseMacro.fetchGroupBy(f, newParam, queryContext)
        GroupByQuery(
            groupBy,
            newParam,
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
    ): GroupByQuery[Group[N, tot.R] *: tut.R] =
        val args = ClauseMacro.fetchArgNames(f)
        val newParam = if tableNames.isEmpty then args else tableNames
        val newAst = replaceTableName(tables, newParam, ast)
        val groupBy = ClauseMacro.fetchGroupBy(f, newParam, queryContext)
        val groupingArgs = ClauseMacro.fetchArgNames(g)
        queryContext.groups.prepend((groupingArgs.head, groupBy))
        val groupingSets = ClauseMacro.fetchGroupBy(g, groupingArgs, queryContext)
        GroupByQuery(
            groupBy,
            newParam,
            newAst.copy(groupBy = SqlGroupItem.GroupingSets(groupingSets) :: Nil)
        )

    inline def distinctOn[F, N <: Tuple, V <: Tuple : AsExpr](using
        tt: ToTuple[T],
        tf: TupledFunction[F, tt.R => NamedTuple[N, V]],
        tu: ToUngrouped[T],
        tut: ToTuple[tu.R]
    )(inline f: F): DistinctOnQuery[Group[N, V] *: tut.R] =
        val args = ClauseMacro.fetchArgNames(f)
        val newParam = if tableNames.isEmpty then args else tableNames
        val newAst = replaceTableName(tables, newParam, ast)
        val groupBy = ClauseMacro.fetchGroupBy(f, newParam, queryContext)
        DistinctOnQuery(
            groupBy,
            newParam,
            newAst.copy(groupBy = groupBy.map(g => SqlGroupItem.Singleton(g)))
        )

    transparent inline def map[F, M: AsSelectItem](using
        tt: ToTuple[T],
        tf: TupledFunction[F, tt.R => M]
    )(inline f: F): Query[M, ?] =
        val args = ClauseMacro.fetchArgNames(f)
        val newParam = if tableNames.isEmpty then args else tableNames
        val newAst = replaceTableName(tables, newParam, ast)
        ClauseMacro.fetchMap(f, false, newParam, newAst, queryContext)

    inline def pivot[F, N <: Tuple, V <: Tuple](using
        tt: ToTuple[T],
        tf: TupledFunction[F, tt.R => NamedTuple[N, V]]
    )(inline f: F): PivotQuery[T, N, V] =
        val args = ClauseMacro.fetchArgNames(f)
        val newParam = if tableNames.isEmpty then args else tableNames
        val newAst = replaceTableName(tables, newParam, ast)
        val functions = ClauseMacro.fetchPivot(f, newParam, queryContext)
        PivotQuery[T, N, V](tables, newParam, functions, newAst)

object SelectQuery:
    extension [T](query: SelectQuery[Table[T]])
        inline def connectBy(inline f: Table[T] => Boolean): ConnectByQuery[T] =
            val args = ClauseMacro.fetchArgNames(f)
            val newParam = if query.tableNames.isEmpty then args else query.tableNames
            val newAst = replaceTableName(query.tables, newParam, query.ast)
            val cond = ClauseMacro.fetchFilter(f, false, true, newParam, query.queryContext)
            val joinAst = newAst
                .copy(
                    select = newAst.select :+ SqlSelectItem.Item(
                        SqlExpr.Binary(
                            SqlExpr.Column(Some(tableCte), columnPseudoLevel),
                            SqlBinaryOperator.Plus,
                            SqlExpr.NumberLiteral(1)
                        ),
                        Some(columnPseudoLevel)
                    ),
                    from = SqlTable.Join(
                        newAst.from.head,
                        SqlJoinType.Inner,
                        SqlTable.Range(tableCte, None),
                        Some(SqlJoinCondition.On(cond)),
                        None
                    ) :: Nil
                )
            val startAst = newAst
                .copy(
                    select = newAst.select :+ SqlSelectItem.Item(
                        SqlExpr.NumberLiteral(1),
                        Some(columnPseudoLevel)
                    )
                )
            ConnectByQuery(query.tables, newParam.head, joinAst, startAst)(using query.queryContext)

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
            SqlTable.Join(
                i,
                joinType,
                SqlTable.Range(joinTableName, Some(SqlTableAlias(joinTableName))),
                None,
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
            SqlTable.Join(
                i,
                joinType,
                SqlTable.SubQuery(query.ast, lateral, Some(SqlTableAlias(""))),
                None,
                None
            )
        JoinPart(tables, tableNames, ast.copy(from = sqlTable.toList))

    private inline def joinFunctionClause[J, R](
        joinType: SqlJoinType,
        inline function: J,
        f: Table[J] => R
    )(using
        m: Mirror.ProductOf[J]
    ): JoinPart[R] =
        AsSqlExpr.summonInstances[m.MirroredElemTypes]
        val joinTable = Table[J](TableMacro.tableMetaData[J])
        val functionTable = ClauseMacro.fetchFunctionTable(function, queryContext)
        val tables = f(joinTable)
        val sqlTable = ast.from.headOption.map: i =>
            SqlTable.Join(
                i,
                joinType,
                functionTable,
                None,
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

    inline def join[N <: Tuple, V <: Tuple, S <: ResultSize](joinQuery: T => Query[NamedTuple[N, V], S])(using
        tt: ToTuple[T]
    ): JoinPart[Tuple.Append[tt.R, SubQuery[N, V]]] =
        joinQueryClause[N, V, S, Tuple.Append[tt.R, SubQuery[N, V]]](
            SqlJoinType.Inner,
            joinQuery(tables),
            j => tt.toTuple(tables) :* j,
            true
        )

    inline def join[J](inline function: J)(using
        m: Mirror.ProductOf[J],
        tt: ToTuple[T]
    ): JoinPart[Tuple.Append[tt.R, Table[J]]] =
        joinFunctionClause[J, Tuple.Append[tt.R, Table[J]]](
            SqlJoinType.Inner,
            function,
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

    inline def leftJoin[N <: Tuple, V <: Tuple, S <: ResultSize](joinQuery: T => Query[NamedTuple[N, V], S])(using
        o: ToOption[SubQuery[N, V]],
        tt: ToTuple[T]
    ): JoinPart[Tuple.Append[tt.R, o.R]] =
        joinQueryClause[N, V, S, Tuple.Append[tt.R, o.R]](
            SqlJoinType.Left,
            joinQuery(tables),
            j => tt.toTuple(tables) :* o.toOption(j),
            true
        )

    inline def leftJoin[J](inline function: J)(using
        o: ToOption[Table[J]],
        m: Mirror.ProductOf[J],
        tt: ToTuple[T]
    ): JoinPart[Tuple.Append[tt.R, o.R]] =
        joinFunctionClause[J, Tuple.Append[tt.R, o.R]](
            SqlJoinType.Left,
            function,
            j => tt.toTuple(tables) :* o.toOption(j)
        )

    inline def rightJoin[J](using
        o: ToOption[T],
        m: Mirror.ProductOf[J],
        tt: ToTuple[o.R]
    ): JoinPart[Tuple.Append[tt.R, Table[J]]] =
        joinClause[J, Tuple.Append[tt.R, Table[J]]](
            SqlJoinType.Right,
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

    inline def rightJoin[J](inline function: J)(using
        o: ToOption[T],
        m: Mirror.ProductOf[J],
        tt: ToTuple[o.R]
    ): JoinPart[Tuple.Append[tt.R, Table[J]]] =
        joinFunctionClause[J, Tuple.Append[tt.R, Table[J]]](
            SqlJoinType.Right,
            function,
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