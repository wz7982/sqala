package sqala.static.dsl.table

import sqala.ast.expr.{SqlExpr, SqlJsonTableColumn, SqlType}
import sqala.ast.table.{SqlTable, SqlTableAlias}
import sqala.static.dsl.*

import scala.NamedTuple.NamedTuple
import scala.compiletime.constValue

case class JsonTable[N <: Tuple, V <: Tuple](
    private[sqala] val __aliasName__ : Option[String],
    private[sqala] val __items__ : V,
    private[sqala] val __sqlTable__ : SqlTable.Json
) extends Selectable:
    type Fields = NamedTuple[N, V]

    inline def selectDynamic(name: String): Any =
        val index = constValue[Index[N, name.type, 0]]
        __items__.toList(index)

object JsonTable:
    def apply[N <: Tuple, V <: Tuple](
        expr: SqlExpr,
        path: SqlExpr,
        alias: Option[String], 
        columns: JsonTableColumns[N, V]
    )(using
        p: AsTableParam[JsonTableColumnFlatten[V]]
    ): JsonTable[JsonTableColumnNameFlatten[N, V], JsonTableColumnFlatten[V]] =
        var index = 0

        def toSqlColumns(columns: List[JsonTableColumn]): List[SqlJsonTableColumn] =
            columns.map:
                case p: JsonTablePathColumn[?] =>
                    index += 1
                    SqlJsonTableColumn.Column(s"c$index", p.`type`, None, Some(p.path), None, None, None, None)
                case o: JsonTableOrdinalColumn =>
                    index += 1
                    SqlJsonTableColumn.Ordinality(s"c$index")
                case e: JsonTableExistsColumn =>
                    index += 1
                    SqlJsonTableColumn.Exists(s"c$index", SqlType.Boolean, Some(e.path), None)
                case n: JsonTableNestedColumns[?, ?] =>
                    SqlJsonTableColumn.Nested(n.path, None, toSqlColumns(n.columns))
                
        val sqlColumns = toSqlColumns(columns.columns)
        new JsonTable(
            alias,
            p.asTableParam(alias, 1),
            SqlTable.Json(
                expr,
                path,
                None,
                Nil,
                sqlColumns,
                None,
                false,
                alias.map(SqlTableAlias(_, Nil)),
                None
            )
        )

case class JsonTableColumns[N <: Tuple, V <: Tuple](private[sqala] columns: List[JsonTableColumn])

sealed trait JsonTableColumn
case class JsonTableNestedColumns[N <: Tuple, V <: Tuple](
    private[sqala] path: SqlExpr, 
    private[sqala] columns: List[JsonTableColumn]
) extends JsonTableColumn
class JsonTableOrdinalColumn extends JsonTableColumn
case class JsonTablePathColumn[T](
    private[sqala] path: SqlExpr, 
    private[sqala] `type`: SqlType
) extends JsonTableColumn
case class JsonTableExistsColumn(private[sqala] path: SqlExpr) extends JsonTableColumn