package sqala.static.dsl

import sqala.ast.expr.*
import sqala.ast.statement.*
import sqala.ast.table.*
import sqala.static.common.*
import sqala.static.macros.*
import sqala.static.statement.dml.*
import sqala.static.statement.query.*

import scala.NamedTuple.NamedTuple
import scala.compiletime.constValueTuple
import scala.deriving.Mirror

def queryContext[T](f: QueryContext ?=> T): T =
    given QueryContext = new QueryContext
    f

inline def query[T](using
    p: Mirror.ProductOf[T],
    c: QueryContext
): TableQuery[T] =
    AsSqlExpr.summonInstances[p.MirroredElemTypes]
    val tableName = TableMacro.tableName[T]
    val metaData = TableMacro.tableMetaData[T]
    val table = Table[T](metaData)
    val selectItems = metaData.columnNames.map: n =>
        SqlSelectItem.Item(SqlExpr.Column(Some(metaData.tableName), n), None)
    val ast = SqlQuery.Select(
        select = selectItems,
        from = SqlTable.Range(tableName, Some(SqlTableAlias(metaData.tableName, Nil))) :: Nil
    )
    TableQuery(table, ast)

inline def query[T](inline function: T)(using
    p: Mirror.ProductOf[T],
    c: QueryContext
): TableQuery[T] =
    AsSqlExpr.summonInstances[p.MirroredElemTypes]
    val metaData = TableMacro.tableMetaData[T]
    val table = Table[T](metaData)
    val selectItems = metaData.columnNames.map: n =>
        SqlSelectItem.Item(SqlExpr.Column(Some(metaData.tableName), n), None)
    val sqlTable = ClauseMacro.fetchFunctionTable(function, c)
    val ast = SqlQuery.Select(
        select = selectItems,
        from = sqlTable :: Nil
    )
    TableQuery(table, ast)

inline def query[N <: Tuple, V <: Tuple, S <: ResultSize](
    subQuery: Query[NamedTuple[N, V], S]
)(using
    s: SelectItem[SubQuery[N, V]],
    c: QueryContext
): SelectQuery[SubQuery[N, V]] =
    val alias = tableSubquery
    val innerQuery = SubQuery[N, V](constValueTuple[N].toList.map(_.asInstanceOf[String]))
    val ast = SqlQuery.Select(
        select = s.selectItems(innerQuery, alias :: Nil),
        from = SqlTable.SubQuery(subQuery.ast, false, Some(SqlTableAlias(alias, Nil))) :: Nil
    )
    SelectQuery(innerQuery, Nil, ast)

inline def query[T <: Product](
    values: List[T]
)(using
    m: Mirror.ProductOf[T],
    c: QueryContext
): SelectQuery[Table[T]] =
    val instances = AsSqlExpr.summonInstances[m.MirroredElemTypes]
    val metaData = TableMacro.tableMetaData[T]
    val table = Table[T](metaData)
    val alias = tableValues
    val selectItems: List[SqlSelectItem.Item] =
        metaData.columnNames.map: n =>
            SqlSelectItem.Item(SqlExpr.Column(Some(alias), n), None)
    val exprList = values.map: datum =>
        instances.zip(datum.productIterator).map: (i, v) =>
            i.asInstanceOf[AsSqlExpr[Any]].asSqlExpr(v)
    val sqlValues = SqlQuery.Values(exprList)
    val tableAlias = SqlTableAlias(alias, metaData.columnNames)
    val ast = SqlQuery.Select(
        select = selectItems,
        from = SqlTable.SubQuery(sqlValues, false, Some(tableAlias)) :: Nil
    )
    SelectQuery(table, Nil, ast)

inline def withRecursive[N <: Tuple, WN <: Tuple, V <: Tuple](
    query: Query[NamedTuple[N, V], ?]
)(f: Query[NamedTuple[N, V], ?] => Query[NamedTuple[WN, V], ?])(using
    SelectItem[SubQuery[N, V]],
    QueryContext
): WithRecursive[NamedTuple[N, V]] =
    WithRecursive(query)(f)

inline def delete[T <: Product]: Delete[Table[T]] = Delete[T]

inline def insert[T <: Product]: Insert[Table[T], InsertNew] = Insert[T]

inline def insert[T <: Product](entity: T)(using Mirror.ProductOf[T]): Insert[Table[T], InsertEntity] =
    Insert[T](entity :: Nil)

inline def insert[T <: Product](entities: List[T])(using Mirror.ProductOf[T]): Insert[Table[T], InsertEntity] =
    Insert[T](entities)

inline def save[T <: Product](entity: T)(using Mirror.ProductOf[T]): Save = Save[T](entity)

inline def update[T <: Product]: Update[Table[T], UpdateTable] = Update[T]

inline def update[T <: Product](entity: T, skipNone: Boolean = false)(using
    Mirror.ProductOf[T]
): Update[Table[T], UpdateEntity] =
    Update[T](entity, skipNone)