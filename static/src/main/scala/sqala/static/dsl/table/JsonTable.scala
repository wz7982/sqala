package sqala.static.dsl.table

import sqala.ast.expr.{SqlExpr, SqlType}
import sqala.ast.table.{SqlJsonColumn, SqlTable, SqlTableAlias}
import sqala.static.dsl.*
import sqala.util.NonEmptyList
import sqala.util.NonEmptyList.toNonEmptyList

import scala.NamedTuple.NamedTuple
import scala.compiletime.constValue

/**
 * A JSON table source, constructed by `jsonTable`.
 */
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

        def toSqlColumns(columns: List[JsonColumn]): NonEmptyList[SqlJsonColumn] =
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
            .toNonEmptyList

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

/**
 * A table reference produced by `from` when a `FromJson` is passed,
 * enabling typed column access via `selectDynamic`.
 */
final case class JsonTable[N <: Tuple, V <: Tuple, L <: Int](
    private[sqala] val __aliasName__ : Option[String],
    private[sqala] val __items__ : V,
    private[sqala] val __sqlTable__ : SqlTable.Json
) extends Selectable with AnyTable:
    /**
     * The structural type declaring available columns as a named tuple.
     * Required by `Selectable`.
     */
    type Fields = NamedTuple[N, V]

    /**
     * Runtime column accessor. Required by `Selectable`.
     */
    inline def selectDynamic(name: String): Any =
        val index = constValue[Index[N, name.type, 0]]
        __items__.toList(index)

/**
 * A list of JSON table column definitions.
 */
final case class JsonColumns[N <: Tuple, V <: Tuple](private[sqala] val columns: List[JsonColumn])

/**
 * A JSON table column definition.
 */
sealed trait JsonColumn

/**
 * A nested path with its own column definitions.
 */
final case class JsonNestedColumns[N <: Tuple, V <: Tuple](
    private[sqala] val path: SqlExpr,
    private[sqala] val columns: List[JsonColumn]
) extends JsonColumn

/**
 * An ordinality column returning the row number.
 */
final class JsonOrdinalColumn extends JsonColumn

/**
 * A column extracting a JSON value at a given path.
 */
final case class JsonPathColumn[T](
    private[sqala] val path: SqlExpr,
    private[sqala] val `type`: SqlType
) extends JsonColumn

/**
 * A column checking existence of a JSON value at a given path.
 */
final case class JsonExistsColumn(private[sqala] val path: SqlExpr) extends JsonColumn