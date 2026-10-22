package sqala.static.dsl.statement.query

import sqala.ast.expr.SqlExpr
import sqala.ast.statement.SqlSelectItem
import sqala.static.dsl.*
import sqala.static.dsl.table.*

import scala.NamedTuple.{AnyNamedTuple, NamedTuple}
import scala.compiletime.ops.int.>
import scala.util.NotGiven
import sqala.ir.IrSelectItem
import sqala.util.NonEmptyList
import sqala.ir.IrExpr
import sqala.metadata.AsSqlExpr
import sqala.static.dsl.statement.query.Query
import scala.quoted.Type
import scala.quoted.Quotes
import scala.compiletime.error
import scala.NamedTuple.AnyNamedTuple

trait AsMapItem[T, CL <: Int]:
    /**
     * The transformed expression type.
     */
    type R

    /**
     * The kind tuple of the mapped expressions.
     */
    type KS <: Tuple

    /**
     * Transforms the expression to the mapped form.
     */
    def transform(x: T): R

    /**
     * The number of columns consumed.
     */
    def offset(x: T): Int

    /**
     * Produces `SELECT` items with auto-incremented column aliases.
     */
    def asSelectItems(x: T, cursor: Int): NonEmptyList[IrSelectItem]

object AsMapItem:
    type Aux[T, CL <: Int, O, OKS <: Tuple] = AsMapItem[T, CL]:
        type R = O

        type KS = OKS

    given expr[T, CL <: Int](using
        a: AsExpr[T, CL],
        as: ValidateArgType[AsSqlExpr, a.R, "scalar"],
        kt: KindToTuple[a.K],
        nt: NotGiven[T <:< Table[?, ?, ?]],
        nmt: NotGiven[T <:< MappedTable[?, ?, ?, ?, ?]]
    ): Aux[T, CL, Expr[a.R, Column[CL]], kt.R] =
        new AsMapItem[T, CL]:
            type R = Expr[a.R, Column[CL]]

            type KS = kt.R

            def transform(x: T): R =
                Expr(a.asExpr(x).asIrExpr)

            def offset(x: T): Int =
                1

            def asSelectItems(x: T, cursor: Int): NonEmptyList[IrSelectItem] =
                NonEmptyList(IrSelectItem(a.asExpr(x).asIrExpr, s"c$cursor"), Nil)

    given table[T, L <: Int, CL <: Int](using
        kt: KindToTuple[Column[L]]
    ): Aux[Table[T, Column, L], CL, Table[T, Column, L], kt.R] =
        new AsMapItem[Table[T, Column, L], CL]:
            type R = Table[T, Column, L]

            type KS = kt.R

            def transform(x: Table[T, Column, L]): R =
                x

            def offset(x: Table[T, Column, L]): Int =
                x.__metaData__.columns.size

            def asSelectItems(x: Table[T, Column, L], cursor: Int): NonEmptyList[IrSelectItem] =
                for
                    (column, index) <- x.__metaData__.columns.zipWithIndex
                yield
                    IrSelectItem(
                        IrExpr.Column(x.__table__, column),
                        s"c${cursor + index}"
                    )

    given mappedTable[N <: Tuple, V <: Tuple, RV <: Tuple, O <: Boolean, L <: Int, CL <: Int](using
        a: AsMap[V, L],
        t: ToTuple[a.R],
        kt: KindToTuple[Column[L]]
    ): Aux[MappedTable[N, V, RV, O, L], CL, MappedTable[N, t.R, RV, O, L], kt.R] =
        new AsMapItem[MappedTable[N, V, RV, O, L], CL]:
            type R = MappedTable[N, t.R, RV, O, L]

            type KS = kt.R

            def transform(x: MappedTable[N, V, RV, O, L]): R =
                MappedTable(
                    x.__table__,
                    t.toTuple(a.transform(x.__items__)),
                    x.__fields__
                )

            def offset(x: MappedTable[N, V, RV, O, L]): Int =
                a.offset(x.__items__)

            def asSelectItems(x: MappedTable[N, V, RV, O, L], cursor: Int): NonEmptyList[IrSelectItem] =
                a.asSelectItems(x.__items__, cursor)

/**
 * Generates `SELECT` items from `map` clause expressions. Lifts
 * values, expressions, subqueries, tuples, and named tuples into
 * column expressions. `CL` is the current query context level.
 */
trait AsMap[T, CL <: Int]:
    /**
     * The transformed expression type.
     */
    type R

    /**
     * The kind tuple of the mapped expressions.
     */
    type KS <: Tuple

    /**
     * Transforms the expression to the mapped form.
     */
    def transform(x: T): R

    /**
     * The number of columns consumed.
     */
    def offset(x: T): Int

    /**
     * Produces `SELECT` items with auto-incremented column aliases.
     */
    def asSelectItems(x: T, cursor: Int): NonEmptyList[IrSelectItem]

object AsMap:
    type Aux[T, CL <: Int, O, OKS <: Tuple] = AsMap[T, CL]:
        type R = O

        type KS = OKS

    given item[T, CL <: Int](using
        a: AsMapItem[T, CL]
    ): Aux[T, CL, a.R, a.KS] =
        new AsMap[T, CL]:
            type R = a.R

            type KS = a.KS

            def transform(x: T): R =
                a.transform(x)

            def offset(x: T): Int =
                a.offset(x)

            def asSelectItems(x: T, cursor: Int): NonEmptyList[IrSelectItem] =
                a.asSelectItems(x, cursor)

    given tuple[H, T <: Tuple, CL <: Int](using
        ah: AsMapItem[H, CL],
        at: AsMap[T, CL],
        tt: ToTuple[at.R],
        c: CombineKindTuple[ah.KS, at.KS]
    ): Aux[H *: T, CL, ah.R *: tt.R, c.R] =
        new AsMap[H *: T, CL]:
            type R = ah.R *: tt.R

            type KS = c.R

            def transform(x: H *: T): R =
                ah.transform(x.head) *: tt.toTuple(at.transform(x.tail))

            def offset(x: H *: T): Int =
                ah.offset(x.head) + at.offset(x.tail)

            def asSelectItems(x: H *: T, cursor: Int): NonEmptyList[IrSelectItem] =
                ah.asSelectItems(x.head, cursor) ++ at.asSelectItems(x.tail, cursor + ah.offset(x.head))

    given tuple1[H, CL <: Int](using
        ah: AsMapItem[H, CL],
        c: CombineKindTuple[ah.KS, EmptyTuple]
    ): Aux[H *: EmptyTuple, CL, ah.R *: EmptyTuple, c.R] =
        new AsMap[H *: EmptyTuple, CL]:
            type R = ah.R *: EmptyTuple

            type KS = c.R

            def transform(x: H *: EmptyTuple): R =
                ah.transform(x.head) *: EmptyTuple

            def offset(x: H *: EmptyTuple): Int =
                ah.offset(x.head)

            def asSelectItems(x: H *: EmptyTuple, cursor: Int): NonEmptyList[IrSelectItem] =
                ah.asSelectItems(x.head, cursor)

    given namedTuple[N <: Tuple, V <: Tuple, CL <: Int](using
        a: AsMap[V, CL],
        t: ToTuple[a.R]
    ): Aux[NamedTuple[N, V], CL, NamedTuple[N, t.R], a.KS] =
        new AsMap[NamedTuple[N, V], CL]:
            type R = NamedTuple[N, t.R]

            type KS = a.KS

            def transform(x: NamedTuple[N, V]): R =
                t.toTuple(a.transform(x.toTuple))

            def offset(x: NamedTuple[N, V]): Int =
                a.offset(x.toTuple)

            def asSelectItems(x: NamedTuple[N, V], cursor: Int): NonEmptyList[IrSelectItem] =
                a.asSelectItems(x.toTuple, cursor)