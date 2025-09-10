package sqala.static.dsl

import sqala.static.dsl.statement.query.*
import sqala.ast.statement.SqlQuery
import sqala.ast.table.SqlTable
import sqala.ast.table.SqlJoinType
import sqala.static.metadata.TableMetaData
import sqala.ast.table.SqlTableAlias
import sqala.ast.expr.SqlExpr
import sqala.ast.expr.SqlTimeUnit
import sqala.ast.expr.SqlType
import sqala.static.metadata.SqlString
import sqala.static.metadata.AsSqlExpr
import scala.NamedTuple.NamedTuple
import sqala.ast.expr.SqlJsonTableColumn

inline def query[T](inline q: QueryContext ?=> T): T =
    given QueryContext = QueryContext(0)
    q

// TODO lateral
extension [A](a: => A)(using c: QueryContext)
    infix def join[B](b: => B)(using fa: AsFrom[A], fb: AsFrom[B], j: TableJoin[fa.R, fb.R]): JoinPart[j.R] = 
        val (leftTable, leftSqlTable) = fa.table(a)
        val (rightTable, rightSqlTable) = fb.table(b)
        val params = j.join(leftTable, rightTable)
        JoinPart(params, SqlTable.Join(leftSqlTable, SqlJoinType.Inner, rightSqlTable, None))

    infix def crossJoin[B](b: => B)(using fa: AsFrom[A], fb: AsFrom[B], j: TableJoin[fa.R, fb.R]): JoinTable[j.R] = 
        val (leftTable, leftSqlTable) = fa.table(a)
        val (rightTable, rightSqlTable) = fb.table(b)
        val params = j.join(leftTable, rightTable)
        JoinTable(params, SqlTable.Join(leftSqlTable, SqlJoinType.Cross, rightSqlTable, None))

    infix def leftJoin[B](b: => B)(using fa: AsFrom[A], fb: AsFrom[B], j: TableLeftJoin[fa.R, fb.R]): JoinPart[j.R] = 
        val (leftTable, leftSqlTable) = fa.table(a)
        val (rightTable, rightSqlTable) = fb.table(b)
        val params = j.join(leftTable, rightTable)
        JoinPart(params, SqlTable.Join(leftSqlTable, SqlJoinType.Left, rightSqlTable, None))

    infix def rightJoin[B](b: => B)(using fa: AsFrom[A], fb: AsFrom[B], j: TableRightJoin[fa.R, fb.R]): JoinPart[j.R] = 
        val (leftTable, leftSqlTable) = fa.table(a)
        val (rightTable, rightSqlTable) = fb.table(b)
        val params = j.join(leftTable, rightTable)
        JoinPart(params, SqlTable.Join(leftSqlTable, SqlJoinType.Right, rightSqlTable, None))

    infix def fullJoin[B](b: => B)(using fa: AsFrom[A], fb: AsFrom[B], j: TableFullJoin[fa.R, fb.R]): JoinPart[j.R] = 
        val (leftTable, leftSqlTable) = fa.table(a)
        val (rightTable, rightSqlTable) = fb.table(b)
        val params = j.join(leftTable, rightTable)
        JoinPart(params, SqlTable.Join(leftSqlTable, SqlJoinType.Full, rightSqlTable, None))

case class Unnest[T](x: Option[T])

def unnest[T: AsExpr as a](x: T)(using
    c: QueryContext
): FuncTable[Unnest[UnnestFlatten[T]]] =
    c.tableIndex += 1
    val aliasName = s"t${c.tableIndex}"
    val sqlTable: SqlTable.Func = SqlTable.Func(
        "UNNEST",
        a.asExpr(x).asSqlExpr :: Nil,
        false,
        false,
        Some(SqlTableAlias(aliasName, "x" :: Nil)),
        None
    )
    FuncTable(aliasName, "x" :: Nil, "x" :: Nil, sqlTable)

case class UnnestWithOrdinal[T](x: Option[T], ordinal: Int)

def unnestWithOrdinal[T: AsExpr as a](x: T)(using
    c: QueryContext
): FuncTable[UnnestWithOrdinal[UnnestFlatten[T]]] =
    c.tableIndex += 1
    val aliasName = s"t${c.tableIndex}"
    val sqlTable: SqlTable.Func = SqlTable.Func(
        "UNNEST",
        a.asExpr(x).asSqlExpr :: Nil,
        false,
        true,
        Some(SqlTableAlias(aliasName, "x" :: "ordinal" :: Nil)),
        None
    )
    FuncTable(aliasName, "x" :: "ordinal" :: Nil, "x" :: "ordinal" :: Nil, sqlTable)

def jsonTable[E: AsExpr as ae, P: AsExpr as ap, N <: Tuple, V <: Tuple](
    expr: E,
    path: P,
    columns: JsonTableColumns[N, V]
)(using
    s: SqlString[ap.R],
    p: AsTableParam[JsonTableColumnFlatten[V]],
    c: QueryContext
): JsonTable[JsonTableColumnNameFlatten[N, V], JsonTableColumnFlatten[V]] =
    c.tableIndex += 1
    val aliasName = s"t${c.tableIndex}"
    JsonTable(ae.asExpr(expr).asSqlExpr, ap.asExpr(path).asSqlExpr, aliasName, columns)

class JsonTableColumnContext

def ordinalColumn(using QueryContext, JsonTableColumnContext): JsonTableOrdinalColumn =
    new JsonTableOrdinalColumn

class JsonTablePathColumnPart[T]:
    def apply[P: AsExpr as ap](path: P)(using sp: AsSqlExpr[T]): JsonTablePathColumn[T] =
        JsonTablePathColumn(ap.asExpr(path).asSqlExpr, sp.sqlType)

def pathColumn[T: AsSqlExpr](using QueryContext, JsonTableColumnContext): JsonTablePathColumnPart[T] =
    new JsonTablePathColumnPart

def existsColumn[P: AsExpr as ap](path: P): JsonTableExistsColumn =
    JsonTableExistsColumn(ap.asExpr(path).asSqlExpr)

def columns[N <: Tuple, V <: Tuple](c: JsonTableColumnContext ?=> NamedTuple[N, V])(using
    QueryContext
): JsonTableColumns[N, V] =
    given JsonTableColumnContext = new JsonTableColumnContext
    val columnList: List[Any] = c.toList
    val jsonColumns = columnList.map:
        case p: JsonTablePathColumn[?] => p
        case o: JsonTableOrdinalColumn => o
        case e: JsonTableExistsColumn => e
        case n: JsonTableNestedColumns[?, ?] => n
    JsonTableColumns(jsonColumns)

def nestedColumns[P: AsExpr as ap, N <: Tuple, V <: Tuple](
    path: P
)(
    c: JsonTableColumnContext ?=> NamedTuple[N, V]
)(using QueryContext, JsonTableColumnContext): JsonTableNestedColumns[N, V] =
    given JsonTableColumnContext = new JsonTableColumnContext
    val columnList: List[Any] = c.toList
    val jsonColumns = columnList.map:
        case p: JsonTablePathColumn[?] => p
        case o: JsonTableOrdinalColumn => o
        case e: JsonTableExistsColumn => e
        case n: JsonTableNestedColumns[?, ?] => n
    JsonTableNestedColumns(ap.asExpr(path).asSqlExpr, jsonColumns)

def from[T](tables: T)(using
    f: AsFrom[T],
    s: AsSelect[f.R],
    c: QueryContext
): SelectQuery[f.R] =
    val (params, fromTable) = f.table(tables)
    val selectItems = s.selectItems(params, 1)
    val tree: SqlQuery.Select = SqlQuery.Select(
        None,
        selectItems,
        fromTable :: Nil,
        None,
        None,
        None,
        Nil,
        Nil,
        None,
        None
    )
    SelectQuery(params, tree)

def grouping[T: AsExpr as a](x: T): Expr[Int] =
    Expr(
        SqlExpr.Grouping(
            a.exprs(x).map(_.asSqlExpr)
        )
    )

sealed class TimeUnit(val unit: SqlTimeUnit)
case object Year extends TimeUnit(SqlTimeUnit.Year)
case object Month extends TimeUnit(SqlTimeUnit.Month)
case object Day extends TimeUnit(SqlTimeUnit.Day)
case object Hour extends TimeUnit(SqlTimeUnit.Hour)
case object Minute extends TimeUnit(SqlTimeUnit.Minute)
case object Second extends TimeUnit(SqlTimeUnit.Second)

// TODO interval coa nullif 