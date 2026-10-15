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

/**
 * Generates `SELECT` items from table sources. Each table type
 * produces column expressions with aliases (`c1`, `c2`, ...),
 * tracking the cursor offset for multi-table projection.
 */
trait AsSelect[T]:
    /**
     * The transformed table type.
     */
    type R

    /**
     * Transforms the table to the `SELECT`-ready form.
     */
    def transform(x: T): R

    /**
     * The number of columns consumed by this table.
     */
    def offset(x: T): Int

    /**
     * Produces `SELECT` items with auto-incremented column aliases.
     */
    def asSelectItems(x: T, cursor: Int): NonEmptyList[IrSelectItem]

object AsSelect:
    type Aux[T, O] = AsSelect[T]:
        type R = O

    given table[T, L <: Int]: Aux[Table[T, Column, L], Table[T, Column, L]] =
        new AsSelect[Table[T, Column, L]]:
            type R = Table[T, Column, L]

            def transform(x: Table[T, Column, L]): R =
                x

            def offset(x: Table[T, Column, L]): Int =
                x.__metaData__.fields.size

            def asSelectItems(x: Table[T, Column, L], cursor: Int): NonEmptyList[IrSelectItem] =
                for
                    (column, index) <- x.__metaData__.columns.zipWithIndex
                yield
                    IrSelectItem(
                        IrExpr.Column(x.__table__, column),
                        s"c${cursor + index}"
                    )

    // TODO
    // given recursiveTable[N <: Tuple, V <: Tuple, L <: Int](using
    //     a: AsMap[V, L],
    //     t: ToTuple[a.R]
    // ): Aux[RecursiveTable[N, V, L], RecursiveTable[N, t.R, L]] =
    //     new AsSelect[RecursiveTable[N, V, L]]:
    //         type R = RecursiveTable[N, t.R, L]

    //         def transform(x: RecursiveTable[N, V, L]): R =
    //             RecursiveTable(
    //                 x.__aliasName__,
    //                 t.toTuple(a.transform(x.__items__)),
    //                 x.__sqlTable__
    //             )

    //         def offset(x: RecursiveTable[N, V, L]): Int =
    //             a.offset(x.__items__)

    //         def asSelectItems(x: RecursiveTable[N, V, L], cursor: Int): List[IrSelectItem] =
    //             a.asSelectItems(x.__items__, cursor)

    given mappedTable[N <: Tuple, V <: Tuple, L <: Int](using
        a: AsMap[V, L],
        t: ToTuple[a.R]
    ): Aux[MappedTable[N, V, L], MappedTable[N, t.R, L]] =
        new AsSelect[MappedTable[N, V, L]]:
            type R = MappedTable[N, t.R, L]

            def transform(x: MappedTable[N, V, L]): R =
                MappedTable(
                    x.__table__,
                    t.toTuple(a.transform(x.__items__)),
                    x.__fields__
                )

            def offset(x: MappedTable[N, V, L]): Int =
                a.offset(x.__items__)

            def asSelectItems(x: MappedTable[N, V, L], cursor: Int): NonEmptyList[IrSelectItem] =
                a.asSelectItems(x.__items__, cursor)

    given tuple[H, T <: Tuple](using
        ah: AsSelect[H],
        at: AsSelect[T],
        tt: ToTuple[at.R]
    ): Aux[H *: T, ah.R *: tt.R] =
        new AsSelect[H *: T]:
            type R = ah.R *: tt.R

            def transform(x: H *: T): R =
                ah.transform(x.head) *: tt.toTuple(at.transform(x.tail))

            def offset(x: H *: T): Int =
                ah.offset(x.head) + at.offset(x.tail)

            def asSelectItems(x: H *: T, cursor: Int): NonEmptyList[IrSelectItem] =
                ah.asSelectItems(x.head, cursor) ++ at.asSelectItems(x.tail, cursor + ah.offset(x.head))

    given tuple1[TH](using
        ah: AsSelect[TH]
    ): Aux[TH *: EmptyTuple, ah.R *: EmptyTuple] =
        new AsSelect[TH *: EmptyTuple]:
            type R = ah.R *: EmptyTuple

            def transform(x: TH *: EmptyTuple): R =
                ah.transform(x.head) *: EmptyTuple

            def offset(x: TH *: EmptyTuple): Int =
                ah.offset(x.head)

            def asSelectItems(x: TH *: EmptyTuple, cursor: Int): NonEmptyList[IrSelectItem] =
                ah.asSelectItems(x.head, cursor)

    inline given fallback[T](using
        NotGiven[AsSelect[T]]
    ): Aux[T, T] =
        ${ errorMacro[T] }

    def errorMacro[T: Type](using q: Quotes): Nothing =
        import q.reflect.*

        val t = Type.show[T]

        report.errorAndAbort(s"Cannot convert type $t for the SQL SELECT list.")

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

    given expr[T: AsSqlExpr, K <: ExprKind, E <: Expr[T, K], CL <: Int](using
        kt: KindToTuple[K]
    ): Aux[E, CL, Expr[T, Column[CL]], kt.R] =
        new AsMap[E, CL]:
            type R = Expr[T, Column[CL]]

            type KS = kt.R

            def transform(x: E): R =
                Expr(x.asIrExpr)

            def offset(x: E): Int =
                1

            def asSelectItems(x: E, cursor: Int): NonEmptyList[IrSelectItem] =
                NonEmptyList(IrSelectItem(x.asIrExpr, s"c$cursor"), Nil)

    given value[T: AsSqlExpr as a, CL <: Int]: Aux[T, CL, Expr[T, Column[CL]], Value *: EmptyTuple] =
        new AsMap[T, CL]:
            type R = Expr[T, Column[CL]]

            type KS = Value *: EmptyTuple

            def transform(x: T): R =
                Expr(IrExpr.Native(a.asSqlExpr(x)))

            def offset(x: T): Int =
                1

            def asSelectItems(x: T, cursor: Int): NonEmptyList[IrSelectItem] =
                NonEmptyList(IrSelectItem(transform(x).asIrExpr, s"c$cursor"), Nil)

    given scalarQuery[T, OKS <: Tuple, L <: Int, S <: QuerySize, Q <: Query[T, OKS, L, S], CL <: Int](using
        a: AsExpr[T, CL],
        vc: ValidateScalarSubqueryColumnSize[a.R],
        to: ToOption[Expr[a.R, Column[CL]]],
        ao: AsExpr[to.R, CL],
        vs: ValidateSubqueryExprSize[S],
        vss: ValidateSubqueryScope[L, CL]
    ): Aux[Q, CL, to.R, OKS] =
        new AsMap[Q, CL]:
            type R = to.R

            type KS = OKS

            def transform(x: Q): R =
                to.toOption(Expr(IrExpr.Subquery(x.tree)))

            def offset(x: Q): Int =
                1

            def asSelectItems(x: Q, cursor: Int): NonEmptyList[IrSelectItem] =
                NonEmptyList(IrSelectItem(ao.asExpr(transform(x)).asIrExpr, s"c$cursor"), Nil)

    inline given window[T, KS <: Tuple, CL <: Int]: Aux[WindowPart[T, KS], CL, WindowPart[T, KS], KS] =
        error("Window function requires an OVER clause.")

    given table[T, L <: Int, CL <: Int](using
        kt: KindToTuple[Column[L]]
    ): Aux[Table[T, Column, L], CL, Table[T, Column, L], kt.R] =
        new AsMap[Table[T, Column, L], CL]:
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

    given mappedTable[N <: Tuple, V <: Tuple, L <: Int, CL <: Int](using
        a: AsMap[V, L],
        t: ToTuple[a.R],
        kt: KindToTuple[Column[L]]
    ): Aux[MappedTable[N, V, L], CL, MappedTable[N, t.R, L], kt.R] =
        new AsMap[MappedTable[N, V, L], CL]:
            type R = MappedTable[N, t.R, L]

            type KS = kt.R

            def transform(x: MappedTable[N, V, L]): R =
                MappedTable(
                    x.__table__,
                    t.toTuple(a.transform(x.__items__)),
                    x.__fields__
                )

            def offset(x: MappedTable[N, V, L]): Int =
                a.offset(x.__items__)

            def asSelectItems(x: MappedTable[N, V, L], cursor: Int): NonEmptyList[IrSelectItem] =
                a.asSelectItems(x.__items__, cursor)

    given tuple[H, T <: Tuple, CL <: Int](using
        ah: AsMap[H, CL],
        at: AsMap[T, CL],
        tt: ToTuple[at.R],
        c: CombineKindTuple[ah.KS, at.KS],
        nt: NotGiven[H <:< Tuple],
        nnt: NotGiven[H <:< AnyNamedTuple]
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
        ah: AsMap[H, CL],
        c: CombineKindTuple[ah.KS, EmptyTuple],
        nt: NotGiven[H <:< Tuple],
        nnt: NotGiven[H <:< AnyNamedTuple]
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

    inline given fallback[T, CL <: Int](using
        NotGiven[AsMap[T, CL]]
    ): Aux[T, CL, T, EmptyTuple] =
        ${ errorMacro[T] }

    def errorMacro[T: Type](using q: Quotes): Nothing =
        import q.reflect.*

        val t = Type.show[T]

        report.errorAndAbort(s"Cannot convert type $t for the SQL SELECT list.")