package sqala.static.ext.dsl.table

import sqala.ast.expr.SqlType
import sqala.ir.IrExpr
import sqala.ir.IrTable
import sqala.static.dsl.table.AnyTable
import sqala.util.NonEmptyList
import sqala.ir.IrJsonColumn
import sqala.static.dsl.Expr
import sqala.static.dsl.Wrap
import sqala.static.dsl.Column
import sqala.static.dsl.table.AsTableParam
import sqala.static.dsl.ToTuple
import sqala.static.dsl.table.AsTable
import sqala.static.dsl.table.MappedTable
import sqala.static.dsl.ExcludeCurrentLevelColumn
import sqala.static.dsl.table.AsLateralTable
import scala.compiletime.ops.int.-

/**
 * A JSON table source, constructed by `jsonTable`.
 */
final case class FromJson[N <: Tuple, V <: Tuple, OKS <: Tuple, CL <: Int](
    private[sqala] val __table__ : IrTable.Json,
    private[sqala] val __items__ : V
) extends AnyTable

object FromJson:
    def apply[N <: Tuple, V <: Tuple, OKS <: Tuple, CL <: Int](
        expr: IrExpr,
        path: IrExpr,
        columns: JsonColumns[N, V]
    )(using
        f: JsonColumnFlatten[V, CL],
        p: AsTableParam[f.R, CL],
        t: ToTuple[p.R]
    ): FromJson[JsonColumnNameFlatten[N, V], t.R, OKS, CL] =
        val irColumns = f.toJsonColumns(columns.columns, 1)
        val table: IrTable.Json = IrTable.Json(
            false,
            expr,
            path,
            None,
            irColumns,
            Nil
        )
        FromJson(
            table,
            t.toTuple(p.asTableParam(table, 1))
        )

    // TODO
    inline given asTable[N <: Tuple, V <: Tuple, TOKS <: Tuple, CL <: Int]: AsTable.Aux[FromJson[N, V, TOKS, CL], CL, MappedTable[N, V, false, CL], TOKS] =
        new AsTable[FromJson[N, V, TOKS, CL], CL]:
            type R = MappedTable[N, V, false, CL]

            type OKS = TOKS

            def asTable(x: FromJson[N, V, TOKS, CL]): (R, IrTable) =
                (MappedTable(x.__table__, x.__items__), x.__table__)

    inline given asLateralTable[N <: Tuple, V <: Tuple, TOKS <: Tuple, CL <: Int](using
        e: ExcludeCurrentLevelColumn[TOKS, CL - 1]
    ): AsLateralTable.Aux[FromJson[N, V, TOKS, CL], CL, MappedTable[N, V, false, CL - 1], e.R] =
        new AsLateralTable[FromJson[N, V, TOKS, CL], CL]:
            type R = MappedTable[N, V, false, CL - 1]

            type OKS = e.R

            def asTable(x: FromJson[N, V, TOKS, CL]): (R, IrTable) =
                val table: IrTable.Json = x.__table__.copy(withLateral = true)
                (MappedTable(table, x.__items__), table)

/**
 * A list of JSON table column definitions.
 */
final case class JsonColumns[N <: Tuple, V <: Tuple](private[sqala] val columns: V)

/**
 * A JSON table column definition.
 */
sealed trait JsonColumn

/**
 * An ordinality column returning the row number.
 */
final class JsonOrdinalColumn extends JsonColumn

/**
 * A column extracting a JSON value at a given path.
 */
final case class JsonPathColumn[T](
    private[sqala] val path: IrExpr,
    private[sqala] val `type`: SqlType
) extends JsonColumn

/**
 * A column checking existence of a JSON value at a given path.
 */
final case class JsonExistsColumn(private[sqala] val path: IrExpr) extends JsonColumn

/**
 * A nested path with its own column definitions.
 */
final case class JsonNestedColumns[N <: Tuple, V <: Tuple](
    private[sqala] val path: IrExpr,
    private[sqala] val columns: V
) extends JsonColumn

/**
 * Flattens nested JSON column types into flat column expressions.
 */
trait JsonColumnFlatten[T, CL <: Int]:
    type R <: Tuple

    def offset(x: T): Int

    def toJsonColumns(x: T, cursor: Int): NonEmptyList[IrJsonColumn]

object JsonColumnFlatten:
    type Aux[T, CL <: Int, O <: Tuple] = JsonColumnFlatten[T, CL]:
        type R = O

    given ordinalColumn[CL <: Int]: Aux[JsonOrdinalColumn, CL, Expr[Int, Column[CL]] *: EmptyTuple] =
        new JsonColumnFlatten[JsonOrdinalColumn, CL]:
            type R = Expr[Int, Column[CL]] *: EmptyTuple

            def offset(x: JsonOrdinalColumn): Int =
                1

            def toJsonColumns(x: JsonOrdinalColumn, cursor: Int): NonEmptyList[IrJsonColumn] =
                NonEmptyList(IrJsonColumn.Ordinality(s"c$cursor"),  Nil)

    given pathColumn[T, CL <: Int]: Aux[JsonPathColumn[T], CL, Expr[Wrap[T, Option], Column[CL]] *: EmptyTuple] =
        new JsonColumnFlatten[JsonPathColumn[T], CL]:
            type R = Expr[Wrap[T, Option], Column[CL]] *: EmptyTuple

            def offset(x: JsonPathColumn[T]): Int =
                1

            def toJsonColumns(x: JsonPathColumn[T], cursor: Int): NonEmptyList[IrJsonColumn] =
                NonEmptyList(IrJsonColumn.Column(s"c$cursor", x.`type`, Some(x.path)), Nil)

    given existsColumn[CL <: Int]: Aux[JsonExistsColumn, CL, Expr[Boolean, Column[CL]] *: EmptyTuple] =
        new JsonColumnFlatten[JsonExistsColumn, CL]:
            type R = Expr[Boolean, Column[CL]] *: EmptyTuple

            def offset(x: JsonExistsColumn): Int =
                1

            def toJsonColumns(x: JsonExistsColumn, cursor: Int): NonEmptyList[IrJsonColumn] =
                NonEmptyList(IrJsonColumn.Exists(s"c$cursor", SqlType.Boolean, Some(x.path)), Nil)

    given nestedColumns[N <: Tuple, V <: Tuple, CL <: Int](using
        f: JsonColumnFlatten[V, CL]
    ): Aux[JsonNestedColumns[N, V], CL, f.R] =
        new JsonColumnFlatten[JsonNestedColumns[N, V], CL]:
            type R = f.R

            def offset(x: JsonNestedColumns[N, V]): Int =
                f.offset(x.columns)

            def toJsonColumns(x: JsonNestedColumns[N, V], cursor: Int): NonEmptyList[IrJsonColumn] =
                NonEmptyList(IrJsonColumn.Nested(x.path, None, f.toJsonColumns(x.columns, cursor)), Nil)

    given tuple[H, T <: Tuple, CL <: Int](using
        fh: JsonColumnFlatten[H, CL],
        ft: JsonColumnFlatten[T, CL]
    ): Aux[H *: T, CL, Tuple.Concat[fh.R, ft.R]] =
        new JsonColumnFlatten[H *: T, CL]:
            type R = Tuple.Concat[fh.R, ft.R]

            def offset(x: H *: T): Int =
                fh.offset(x.head) + ft.offset(x.tail)

            def toJsonColumns(x: H *: T, cursor: Int): NonEmptyList[IrJsonColumn] =
                fh.toJsonColumns(x.head, cursor) ++ ft.toJsonColumns(x.tail, cursor + fh.offset(x.head))

    given tuple1[H, CL <: Int](using
        fh: JsonColumnFlatten[H, CL]
    ): Aux[H *: EmptyTuple, CL, fh.R] =
        new JsonColumnFlatten[H *: EmptyTuple, CL]:
            type R = fh.R

            def offset(x: H *: EmptyTuple): Int =
                fh.offset(x.head)

            def toJsonColumns(x: H *: EmptyTuple, cursor: Int): NonEmptyList[IrJsonColumn] =
                fh.toJsonColumns(x.head, cursor)

/**
 * Flattens nested JSON column names into a flat tuple.
 */
type JsonColumnNameFlatten[N <: Tuple, V <: Tuple] <: Tuple =
    (N, V) match
        case (hn *: tn, hv *: tv) =>
            hv match
                case JsonNestedColumns[n, v] =>
                    Tuple.Concat[JsonColumnNameFlatten[n, v], JsonColumnNameFlatten[tn, tv]]
                case _ =>
                    hn *: JsonColumnNameFlatten[tn, tv]
        case (EmptyTuple, EmptyTuple) => EmptyTuple