package sqala.static.dsl

import sqala.ast.expr.*
import sqala.ast.statement.*
import sqala.ast.table.*
import sqala.static.common.*
import sqala.static.macros.*
import sqala.static.statement.query.*

import scala.deriving.Mirror

def queryContext[T](f: QueryContext ?=> T): T =
    given QueryContext = new QueryContext
    f

inline def query[T](using
    p: Mirror.ProductOf[T]
): TableQuery[Table[T]] =
    AsSqlExpr.summonInstances[p.MirroredElemTypes]
    val tableName = TableMacro.tableName[T]
    val metaData = TableMacro.tableMetaData[T]
    val table = Table[T](tableName, metaData)
    // TODO 
    val selectItems = metaData.fieldNames.map: n =>
        SqlSelectItem.Item(SqlExpr.Column(None, n), None)
    val ast = SqlQuery.Select(
        select = selectItems,
        from = SqlTable.IdentTable(tableName, Some(SqlTableAlias(metaData.tableName, Nil))) :: Nil
    )
    TableQuery(Nil, ast)