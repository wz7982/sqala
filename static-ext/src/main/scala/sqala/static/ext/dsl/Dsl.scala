package sqala.static.ext.dsl

import sqala.static.dsl.QueryContext
import sqala.static.dsl.AsExpr
import sqala.static.ext.dsl.table.FromFunc
import sqala.static.dsl.Column
import sqala.static.dsl.KindToTuple
import sqala.static.dsl.ExcludeCurrentLevelColumn
import sqala.static.dsl.CanInSimpleClause
import sqala.static.dsl.FlattenUnnest
import sqala.metadata.TableMetaData
import sqala.ir.IrTable
import sqala.util.NonEmptyList
import sqala.static.ext.dsl.table.JsonColumns
import sqala.static.dsl.table.AsTableParam
import sqala.static.ext.dsl.table.JsonColumnFlatten
import sqala.static.dsl.ToTuple
import sqala.static.dsl.CombineKindTuple
import sqala.static.ext.dsl.table.JsonColumnNameFlatten
import sqala.static.ext.dsl.table.FromJson
import sqala.static.ext.dsl.table.JsonOrdinalColumn
import sqala.metadata.AsSqlExpr
import sqala.static.dsl.ValidateArgType
import sqala.static.ext.dsl.table.JsonPathColumn
import sqala.static.dsl.asExpr
import sqala.static.ext.dsl.table.JsonExistsColumn
import scala.NamedTuple.NamedTuple
import sqala.static.ext.dsl.table.JsonNestedColumns

/**
 * Computes the return type of `unnest`.
 */
trait UnnestReturn[T]:
    /**
     * The result type.
     */
    type R

object UnnestReturn:
    type Aux[T, O] = UnnestReturn[T]:
        type R = O

    given flatten[T]: Aux[T, FlattenUnnest[T]] =
        new UnnestReturn[T]:
            type R = FlattenUnnest[T]

/**
 * An unnested table element, used as the row shape from `unnest`.
 */
final case class Unnest[T](x: Option[T])

/**
 * Expands an array into a set of rows. Maps to `UNNEST(expr)`.
 *
 * {{{
 * from(unnest(Array(1, 2, 3))).filter(t => t.x > 1)
 * }}}
 */
def unnest[T, CL <: Int](x: T)(using
    qc: QueryContext[CL],
    a: AsExpr[T, CL],
    r: UnnestReturn[a.R],
    kt: KindToTuple[a.K],
    i: CanInSimpleClause[kt.R, "unnest"],
    e: ExcludeCurrentLevelColumn[kt.R, CL]
): FromFunc[Unnest[r.R], Column, e.R, CL] =
    val metaData = TableMetaData("UNNEST", Nil, None, NonEmptyList("x", Nil), NonEmptyList("x", Nil))
    val table: IrTable.Func =
        IrTable.Func(
            false,
            "UNNEST",
            a.asExpr(x).expr :: Nil,
            false,
            metaData.columns.toList
        )
    FromFunc(table, metaData)

/**
 * An unnested table element with row number, used as the row shape
 * from `unnestWithOrdinal`.
 */
final case class UnnestWithOrdinal[T](x: Option[T], ordinal: Int)

/**
 * Expands an array into a set of rows with ordinal position.
 * Maps to `UNNEST(expr) WITH ORDINALITY`.
 *
 * {{{
 * from(unnestWithOrdinal(Array(1, 2, 3))).filter(t => t.ordinal > 1)
 * }}}
 */
def unnestWithOrdinal[T, CL <: Int](x: T)(using
    qc: QueryContext[CL],
    a: AsExpr[T, CL],
    r: UnnestReturn[a.R],
    kt: KindToTuple[a.K],
    i: CanInSimpleClause[kt.R, "unnest"],
    e: ExcludeCurrentLevelColumn[kt.R, CL]
): FromFunc[UnnestWithOrdinal[r.R], Column, e.R, CL] =
    val metaData = TableMetaData("UNNEST", Nil, None, NonEmptyList("x", "ordinal" :: Nil), NonEmptyList("x", "ordinal" :: Nil))
    val table: IrTable.Func =
        IrTable.Func(
            false,
            "UNNEST",
            a.asExpr(x).expr :: Nil,
            true,
            metaData.columns.toList
        )
    FromFunc(table, metaData)

/**
 * Creates a JSON table. Maps to `JSON_TABLE`. Columns are defined using
 * `pathColumn`, `existsColumn`, `ordinalColumn`, and `nestedColumns`.
 *
 * {{{
 * from(
 *     jsonTable(
 *         """{"a": 1, "obj": {"b": "abc", "c": 2}}""",
 *         "$",
 *         columns(
 *             o = ordinalColumn,
 *             a = pathColumn[Int]("$.a"),
 *             n = nestedColumns("$.obj")(
 *                 b = pathColumn[String]("$.b"),
 *                 c = existsColumn("$.c")
 *             )
 *         )
 *     )
 * )
 * }}}
 */
def jsonTable[E, N <: Tuple, V <: Tuple, CL <: Int](
    expr: E,
    path: String,
    columns: JsonContext ?=> JsonColumns[N, V]
)(using
    qc: QueryContext[CL],
    a: AsExpr[E, CL],
    f: JsonColumnFlatten[V, CL],
    p: AsTableParam[f.R, CL],
    t: ToTuple[p.R],
    kt: KindToTuple[a.K],
    i: CanInSimpleClause[kt.R, "jsonTable"],
    e: ExcludeCurrentLevelColumn[kt.R, CL],
    c: CombineKindTuple[EmptyTuple, e.R]
): FromJson[JsonColumnNameFlatten[N, V], t.R, c.R, CL] =
    given JsonContext = JsonContext()
    FromJson(a.asExpr(expr).expr, path.asExpr.expr, columns)

/**
 * Creates an ordinality column for `jsonTable`. Maps to
 * `FOR ORDINALITY`.
 *
 * {{{
 * ordinalColumn
 * }}}
 */
def ordinalColumn[CL <: Int](using QueryContext[CL], JsonContext): JsonOrdinalColumn =
    JsonOrdinalColumn()

/**
 * Intermediate builder for a path column in `jsonTable`.
 */
final class JsonPathColumnPart[T]:
    /**
     * Creates a path column definition for `jsonTable`. Maps to
     * `path type PATH "$.path"`.
     *
     * {{{
     * pathColumn[Int]("$.a")
     * }}}
     */
    def apply[CL <: Int](path: String)(using
        qc: QueryContext[CL],
        jc: JsonContext,
        as: ValidateArgType[AsSqlExpr, T, "scalar"],
        a: AsSqlExpr[T]
    ): JsonPathColumn[T] =
        JsonPathColumn(path.asExpr.expr, a.sqlType)

/**
 * Creates a path column definition for `jsonTable`. Maps to
 * `type PATH "$.path"`.
 *
 * {{{
 * pathColumn[Int]("$.a")
 * }}}
 */
def pathColumn[T: AsSqlExpr]: JsonPathColumnPart[T] =
    JsonPathColumnPart()

/**
 * Creates an existence check column for `jsonTable`. Maps to
 * `BOOLEAN EXISTS PATH "$.path"`.
 *
 * {{{
 * existsColumn("$.c")
 * }}}
 */
def existsColumn[CL <: Int](path: String)(using
    QueryContext[CL],
    JsonContext,
): JsonExistsColumn =
    JsonExistsColumn(path.asExpr.expr)

/**
 * Defines nested columns at a JSON path for `jsonTable`. Maps to
 * `NESTED PATH "$.path" COLUMNS(...)`.
 *
 * {{{
 * nestedColumns("$.obj")(
 *     b = pathColumn[String]("$.b"),
 *     c = existsColumn("$.c")
 * )
 * }}}
 */
def nestedColumns[N <: Tuple, V <: Tuple, CL <: Int](
    path: String
)(using
    QueryContext[CL],
    JsonContext
)(
    c: JsonContext ?=> NamedTuple[N, V]
): JsonNestedColumns[N, V] =
    JsonNestedColumns(path.asExpr.expr, c.toTuple)

/**
 * Creates a column list for `jsonTable`. Maps to
 * `COLUMNS(...)`.
 *
 * {{{
 * from(
 *     jsonTable(
 *         """{"a": 1, "obj": {"b": "abc", "c": 2}}""",
 *         "$",
 *         columns(
 *             o = ordinalColumn,
 *             a = pathColumn[Int]("$.a"),
 *             n = nestedColumns("$.obj")(
 *                 b = pathColumn[String]("$.b"),
 *                 c = existsColumn("$.c")
 *             )
 *         )
 *     )
 * )
 * }}}
 */
def columns[N <: Tuple, V <: Tuple, CL <: Int](c: JsonContext ?=> NamedTuple[N, V])(using
    QueryContext[CL],
    JsonContext
): JsonColumns[N, V] =
    JsonColumns(c.toTuple)