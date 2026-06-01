package sqala.static.dsl.table

import sqala.ast.expr.{SqlExpr, SqlType}
import sqala.ast.table.{SqlTable, SqlTableAlias, SqlJsonColumn}
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
        columns: JsonColumns[N, V]
    )(using
        p: AsTableParam[JsonColumnFlatten[V, CL], CL],
        t: ToTuple[p.R]
    ): FromJson[JsonColumnNameFlatten[N, V], t.R, OKS, CL] =
        var index = 0

        def toSqlColumns(columns: List[JsonColumn]): List[SqlJsonColumn] =
            columns.map:
                case p: JsonPathColumn[?] =>
                    index += 1
                    SqlJsonColumn.Column(s"c$index", p.`type`, None, Some(p.path), None, None, None, None)
                case o: JsonOrdinalColumn =>
                    index += 1
                    SqlJsonColumn.Ordinality(s"c$index")
                case e: JsonExistsColumn =>
                    index += 1
                    SqlJsonColumn.Exists(s"c$index", SqlType.Boolean, Some(e.path), None)
                case n: JsonNestedColumns[?, ?] =>
                    SqlJsonColumn.Nested(n.path, None, toSqlColumns(n.columns))

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

final case class JsonColumns[N <: Tuple, V <: Tuple](private[sqala] val columns: List[JsonColumn])

sealed trait JsonColumn
final case class JsonNestedColumns[N <: Tuple, V <: Tuple](
    private[sqala] val path: SqlExpr,
    private[sqala] val columns: List[JsonColumn]
) extends JsonColumn
final class JsonOrdinalColumn extends JsonColumn
final case class JsonPathColumn[T](
    private[sqala] val path: SqlExpr,
    private[sqala] val `type`: SqlType
) extends JsonColumn
final case class JsonExistsColumn(private[sqala] val path: SqlExpr) extends JsonColumn