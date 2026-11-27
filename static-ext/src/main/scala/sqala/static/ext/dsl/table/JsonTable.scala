package sqala.static.ext.dsl.table

import sqala.ast.expr.{SqlExpr, SqlType}
import sqala.ir.{IrExpr, IrJsonColumn, IrTable}
import sqala.static.dsl.*
import sqala.static.dsl.table.*
import sqala.util.NonEmptyList

import scala.compiletime.ops.int.-

/**
 * A list of JSON table column definitions.
 */
final class JsonColumns[N <: Tuple, V <: Tuple](private[sqala] val columns: V)

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
final class JsonPathColumn[T](
    private[sqala] val path: String,
    private[sqala] val `type`: SqlType
) extends JsonColumn

/**
 * A column checking existence of a JSON value at a given path.
 */
final class JsonExistsColumn(private[sqala] val path: String) extends JsonColumn

/**
 * A nested path with its own column definitions.
 */
final class JsonNestedColumns[N <: Tuple, V <: Tuple](
    private[sqala] val path: String,
    private[sqala] val columns: V
) extends JsonColumn

/**
 * Flattens nested JSON column types into flat column expressions.
 */
trait JsonColumnFlatten[T, CL <: Int]:
     /**
     * The flattened column expression tuple type.
     */
    type R <: Tuple

    /**
     * The number of result columns produced by `x`.
     */
    def offset(x: T): Int

    /**
     * Flattens the nested JSON column definitions of `x` into flat IR
     * columns, assigning aliases starting from `cursor`.
     */
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
                NonEmptyList(
                    IrJsonColumn.Column(
                        s"c$cursor", 
                        x.`type`, 
                        Some(IrExpr.Native(SqlExpr.InlinedLiteral(SqlExpr.StringLiteral(x.path))))
                    ), 
                    Nil
                )

    given existsColumn[CL <: Int]: Aux[JsonExistsColumn, CL, Expr[Boolean, Column[CL]] *: EmptyTuple] =
        new JsonColumnFlatten[JsonExistsColumn, CL]:
            type R = Expr[Boolean, Column[CL]] *: EmptyTuple

            def offset(x: JsonExistsColumn): Int =
                1

            def toJsonColumns(x: JsonExistsColumn, cursor: Int): NonEmptyList[IrJsonColumn] =
                NonEmptyList(
                    IrJsonColumn.Exists(
                        s"c$cursor", 
                        SqlType.Boolean, 
                        Some(IrExpr.Native(SqlExpr.InlinedLiteral(SqlExpr.StringLiteral(x.path))))
                    ), 
                    Nil
                )

    given nestedColumns[N <: Tuple, V <: Tuple, CL <: Int](using
        f: JsonColumnFlatten[V, CL]
    ): Aux[JsonNestedColumns[N, V], CL, f.R] =
        new JsonColumnFlatten[JsonNestedColumns[N, V], CL]:
            type R = f.R

            def offset(x: JsonNestedColumns[N, V]): Int =
                f.offset(x.columns)

            def toJsonColumns(x: JsonNestedColumns[N, V], cursor: Int): NonEmptyList[IrJsonColumn] =
                NonEmptyList(
                    IrJsonColumn.Nested(
                        IrExpr.Native(SqlExpr.InlinedLiteral(SqlExpr.StringLiteral(x.path))), 
                        None, 
                        f.toJsonColumns(x.columns, cursor)
                    ), 
                    Nil
                )

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

/**
 * A JSON table source, constructed by `jsonTable`.
 */
final class FromJson[N <: Tuple, V <: Tuple, OKS <: Tuple, CL <: Int](
    private[sqala] val table: IrTable.Json,
    private[sqala] val items: V
) extends AnyTable

object FromJson:
    def apply[N <: Tuple, V <: Tuple, OKS <: Tuple, CL <: Int](
        expr: IrExpr,
        path: String,
        columns: JsonColumns[N, V]
    )(using
        f: JsonColumnFlatten[V, CL],
        p: AsTableParam[f.R, CL],
        tt: ToTuple[p.R],
    ): FromJson[JsonColumnNameFlatten[N, V], tt.R, OKS, CL] =
        val irColumns = f.toJsonColumns(columns.columns, 1)
        val table: IrTable.Json = IrTable.Json(
            false,
            expr,
            IrExpr.Native(SqlExpr.InlinedLiteral(SqlExpr.StringLiteral(path))),
            None,
            irColumns,
            Nil
        )
        new FromJson(
            table,
            tt.toTuple(p.asTableParam(table, 1))
        )

    given asTable[N <: Tuple, V <: Tuple, TOKS <: Tuple, CL <: Int]: AsTable.Aux[FromJson[N, V, TOKS, CL], CL, MappedTable[N, V, false, CL], TOKS] =
        new AsTable[FromJson[N, V, TOKS, CL], CL]:
            type R = MappedTable[N, V, false, CL]

            type OKS = TOKS

            def asTable(x: FromJson[N, V, TOKS, CL]): (R, IrTable) =
                (new MappedTable[N, V, false, CL](x.items), x.table)

    given asLateralTable[N <: Tuple, V <: Tuple, TOKS <: Tuple, CL <: Int](using
        p: AsTableParam[V, CL - 1],
        e: ExcludeCurrentLevelColumn[TOKS, CL - 1]
    ): AsLateralTable.Aux[FromJson[N, V, TOKS, CL], CL, MappedTable[N, WrapToTuple[p.R], false, CL - 1], e.R] =
        new AsLateralTable[FromJson[N, V, TOKS, CL], CL]:
            type R = MappedTable[N, WrapToTuple[p.R], false, CL - 1]

            type OKS = e.R

            def asTable(x: FromJson[N, V, TOKS, CL]): (R, IrTable) =
                val irTable: IrTable.Json = x.table.copy(withLateral = true)
                (MappedTable[N, V, false, CL - 1](irTable), irTable)