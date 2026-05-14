package sqala.static.dsl.table

import sqala.ast.expr.{SqlExpr, SqlJsonTableColumn, SqlType}
import sqala.ast.table.{SqlTable, SqlTableAlias}
import sqala.static.dsl.*

import scala.NamedTuple.NamedTuple
import scala.compiletime.constValue

final case class FromJson[N <: Tuple, V <: Tuple, OKS <: Tuple, CL <: Int](
    private[sqala] val __aliasName__ : Option[String],
    private[sqala] val __items__ : V,
    private[sqala] val __sqlTable__ : SqlTable.Json
) extends AnyTable

object FromJson:
    def apply[N <: Tuple, V <: Tuple, OKS <: Tuple, CL <: Int](
        expr: SqlExpr,
        path: SqlExpr,
        alias: Option[String],
        columns: JsonTableColumns[N, V]
    )(using
        p: AsTableParam[JsonTableColumnFlatten[V, CL], CL],
        t: ToTuple[p.R]
    ): FromJson[JsonTableColumnNameFlatten[N, V], t.R, OKS, CL] =
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
        FromJson(
            alias,
            t.toTuple(p.asTableParam(alias, 1)),
            SqlTable.Json(
                false,
                expr,
                path,
                None,
                Nil,
                sqlColumns,
                None,
                alias.map(SqlTableAlias(_, Nil)),
                None
            )
        )

final case class JsonTable[N <: Tuple, V <: Tuple, L <: Int](
    private[sqala] val __aliasName__ : Option[String],
    private[sqala] val __items__ : V,
    private[sqala] val __sqlTable__ : SqlTable.Json
) extends Selectable with AnyTable:
    type Fields = NamedTuple[N, V]

    inline def selectDynamic(name: String): Any =
        val index = constValue[Index[N, name.type, 0]]
        __items__.toList(index)

final case class JsonTableColumns[N <: Tuple, V <: Tuple](private[sqala] val columns: List[JsonTableColumn])

sealed trait JsonTableColumn
final case class JsonTableNestedColumns[N <: Tuple, V <: Tuple](
    private[sqala] val path: SqlExpr,
    private[sqala] val columns: List[JsonTableColumn]
) extends JsonTableColumn
final class JsonTableOrdinalColumn extends JsonTableColumn
final case class JsonTablePathColumn[T](
    private[sqala] val path: SqlExpr,
    private[sqala] val `type`: SqlType
) extends JsonTableColumn
final case class JsonTableExistsColumn(private[sqala] val path: SqlExpr) extends JsonTableColumn