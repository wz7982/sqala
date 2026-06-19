package sqala.static.dsl.statement.query

import sqala.ast.expr.SqlExpr
import sqala.ast.statement.SqlSelectItem
import sqala.metadata.AsSqlExpr
import sqala.static.dsl.*
import sqala.static.dsl.table.*

import scala.NamedTuple.NamedTuple
import scala.compiletime.ops.int.>

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
    def asSelectItems(x: T, cursor: Int): List[SqlSelectItem.Expr]

object AsSelect:
    type Aux[T, O] = AsSelect[T]:
        type R = O

    given table[T, L <: Int]: Aux[Table[T, Column, L], Table[T, Column, L]] =
        new AsSelect[Table[T, Column, L]]:
            type R = Table[T, Column, L]

            def transform(x: Table[T, Column, L]): R =
                x

            def offset(x: Table[T, Column, L]): Int =
                x.__metaData__.fieldNames.size

            def asSelectItems(x: Table[T, Column, L], cursor: Int): List[SqlSelectItem.Expr] =
                for
                    (column, index) <- x.__metaData__.columnNames.zipWithIndex
                yield
                    SqlSelectItem.Expr(
                        SqlExpr.Column(x.__aliasName__, column), Some(s"c${cursor + index}")
                    )

    given jsonTable[N <: Tuple, V <: Tuple, L <: Int](using
        a: AsMap[V, L],
        t: ToTuple[a.R]
    ): Aux[JsonTable[N, V, L], JsonTable[N, t.R, L]] =
        new AsSelect[JsonTable[N, V, L]]:
            type R = JsonTable[N, t.R, L]

            def transform(x: JsonTable[N, V, L]): R =
                JsonTable(
                    x.__aliasName__,
                    t.toTuple(a.transform(x.__items__)),
                    x.__sqlTable__
                )

            def offset(x: JsonTable[N, V, L]): Int =
                a.offset(x.__items__)

            def asSelectItems(x: JsonTable[N, V, L], cursor: Int): List[SqlSelectItem.Expr] =
                a.asSelectItems(x.__items__, cursor)

    given funcTable[T, L <: Int]: Aux[FuncTable[T, Column, L], FuncTable[T, Column, L]] =
        new AsSelect[FuncTable[T, Column, L]]:
            type R = FuncTable[T, Column, L]

            def transform(x: FuncTable[T, Column, L]): R =
                x

            def offset(x: FuncTable[T, Column, L]): Int =
                x.__fieldNames__.size

            def asSelectItems(x: FuncTable[T, Column, L], cursor: Int): List[SqlSelectItem.Expr] =
                for
                    (column, index) <- x.__columnNames__.zipWithIndex
                yield
                    SqlSelectItem.Expr(
                        SqlExpr.Column(x.__aliasName__, column), Some(s"c${cursor + index}")
                    )

    given excludedTable[N <: Tuple, V <: Tuple, L <: Int](using
        a: AsMap[V, L],
        t: ToTuple[a.R]
    ): Aux[ExcludedTable[N, V, L], ExcludedTable[N, t.R, L]] =
        new AsSelect[ExcludedTable[N, V, L]]:
            type R = ExcludedTable[N, t.R, L]

            def transform(x: ExcludedTable[N, V, L]): R =
                ExcludedTable(
                    x.__aliasName__,
                    t.toTuple(a.transform(x.__items__.asInstanceOf[V])),
                    x.__sqlTable__
                )

            def offset(x: ExcludedTable[N, V, L]): Int =
                a.offset(x.__items__.asInstanceOf[V])

            def asSelectItems(x: ExcludedTable[N, V, L], cursor: Int): List[SqlSelectItem.Expr] =
                a.asSelectItems(x.__items__.asInstanceOf[V], cursor)

    given graphTable[N <: Tuple, V <: Tuple, L <: Int](using
        a: AsMap[V, L],
        t: ToTuple[a.R]
    ): Aux[GraphTable[N, V, L], GraphTable[N, t.R, L]] =
        new AsSelect[GraphTable[N, V, L]]:
            type R = GraphTable[N, t.R, L]

            def transform(x: GraphTable[N, V, L]): R =
                GraphTable(
                    x.__aliasName__,
                    t.toTuple(a.transform(x.__items__)),
                    x.__sqlTable__
                )

            def offset(x: GraphTable[N, V, L]): Int =
                a.offset(x.__items__)

            def asSelectItems(x: GraphTable[N, V, L], cursor: Int): List[SqlSelectItem.Expr] =
                a.asSelectItems(x.__items__, cursor)

    given recursiveTable[N <: Tuple, V <: Tuple, L <: Int](using
        a: AsMap[V, L],
        t: ToTuple[a.R]
    ): Aux[RecursiveTable[N, V, L], RecursiveTable[N, t.R, L]] =
        new AsSelect[RecursiveTable[N, V, L]]:
            type R = RecursiveTable[N, t.R, L]

            def transform(x: RecursiveTable[N, V, L]): R =
                RecursiveTable(
                    x.__aliasName__,
                    t.toTuple(a.transform(x.__items__)),
                    x.__sqlTable__
                )

            def offset(x: RecursiveTable[N, V, L]): Int =
                a.offset(x.__items__)

            def asSelectItems(x: RecursiveTable[N, V, L], cursor: Int): List[SqlSelectItem.Expr] =
                a.asSelectItems(x.__items__, cursor)

    given recognizeTable[N <: Tuple, V <: Tuple, L <: Int](using
        a: AsMap[V, L],
        t: ToTuple[a.R]
    ): Aux[RecognizeTable[N, V, L], RecognizeTable[N, t.R, L]] =
        new AsSelect[RecognizeTable[N, V, L]]:
            type R = RecognizeTable[N, t.R, L]

            def transform(x: RecognizeTable[N, V, L]): R =
                RecognizeTable(
                    x.__aliasName__,
                    t.toTuple(a.transform(x.__items__)),
                    x.__sqlTable__
                )

            def offset(x: RecognizeTable[N, V, L]): Int =
                a.offset(x.__items__)

            def asSelectItems(x: RecognizeTable[N, V, L], cursor: Int): List[SqlSelectItem.Expr] =
                a.asSelectItems(x.__items__, cursor)

    given subqueryTable[N <: Tuple, V <: Tuple, L <: Int](using
        a: AsMap[V, L],
        t: ToTuple[a.R]
    ): Aux[SubqueryTable[N, V, L], SubqueryTable[N, t.R, L]] =
        new AsSelect[SubqueryTable[N, V, L]]:
            type R = SubqueryTable[N, t.R, L]

            def transform(x: SubqueryTable[N, V, L]): R =
                SubqueryTable(
                    x.__aliasName__,
                    t.toTuple(a.transform(x.__items__)),
                    x.__sqlTable__
                )

            def offset(x: SubqueryTable[N, V, L]): Int =
                a.offset(x.__items__)

            def asSelectItems(x: SubqueryTable[N, V, L], cursor: Int): List[SqlSelectItem.Expr] =
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

            def asSelectItems(x: H *: T, cursor: Int): List[SqlSelectItem.Expr] =
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

            def asSelectItems(x: TH *: EmptyTuple, cursor: Int): List[SqlSelectItem.Expr] =
                ah.asSelectItems(x.head, cursor)

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
    def asSelectItems(x: T, cursor: Int): List[SqlSelectItem.Expr]

object AsMap:
    type Aux[T, CL <: Int, O, OKS <: Tuple] = AsMap[T, CL]:
        type R = O

        type KS = OKS

    given expr[T: AsSqlExpr, K <: ExprKind, CL <: Int](using
        kt: KindToTuple[K]
    ): Aux[Expr[T, K], CL, Expr[T, Column[CL]], kt.R] =
        new AsMap[Expr[T, K], CL]:
            type R = Expr[T, Column[CL]]

            type KS = kt.R

            def transform(x: Expr[T, K]): R =
                Expr(x.asSqlExpr)

            def offset(x: Expr[T, K]): Int =
                1

            def asSelectItems(x: Expr[T, K], cursor: Int): List[SqlSelectItem.Expr] =
                SqlSelectItem.Expr(x.asSqlExpr, Some(s"c$cursor")) :: Nil

    given value[T: AsSqlExpr as a, CL <: Int]: Aux[T, CL, Expr[T, Column[CL]], Value *: EmptyTuple] =
        new AsMap[T, CL]:
            type R = Expr[T, Column[CL]]

            type KS = Value *: EmptyTuple

            def transform(x: T): R =
                Expr(a.asSqlExpr(x))

            def offset(x: T): Int =
                1

            def asSelectItems(x: T, cursor: Int): List[SqlSelectItem.Expr] =
                SqlSelectItem.Expr(transform(x).asSqlExpr, Some(s"c$cursor")) :: Nil

    given scalarQuery[T, OKS <: Tuple, L <: Int, Q <: Query[T, OKS, L, OneRow], CL <: Int](using
        a: AsExpr[T, CL],
        as: AsSqlExpr[a.R],
        refl: L > CL =:= true
    ): Aux[Q, CL, Expr[a.R, Column[CL]], OKS] =
        new AsMap[Q, CL]:
            type R = Expr[a.R, Column[CL]]

            type KS = OKS

            def transform(x: Q): R =
                Expr(SqlExpr.Subquery(x.tree))

            def offset(x: Q): Int =
                1

            def asSelectItems(x: Q, cursor: Int): List[SqlSelectItem.Expr] =
                SqlSelectItem.Expr(transform(x).asSqlExpr, Some(s"c$cursor")) :: Nil

    given tuple[H, T <: Tuple, CL <: Int](using
        ah: AsMap[H, CL],
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

            def asSelectItems(x: H *: T, cursor: Int): List[SqlSelectItem.Expr] =
                ah.asSelectItems(x.head, cursor) ++ at.asSelectItems(x.tail, cursor + ah.offset(x.head))

    given tuple1[H, CL <: Int](using
        ah: AsMap[H, CL],
        c: CombineKindTuple[ah.KS, EmptyTuple]
    ): Aux[H *: EmptyTuple, CL, ah.R *: EmptyTuple, c.R] =
        new AsMap[H *: EmptyTuple, CL]:
            type R = ah.R *: EmptyTuple

            type KS = c.R

            def transform(x: H *: EmptyTuple): R =
                ah.transform(x.head) *: EmptyTuple

            def offset(x: H *: EmptyTuple): Int =
                ah.offset(x.head)

            def asSelectItems(x: H *: EmptyTuple, cursor: Int): List[SqlSelectItem.Expr] =
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

            def asSelectItems(x: NamedTuple[N, V], cursor: Int): List[SqlSelectItem.Expr] =
                a.asSelectItems(x.toTuple, cursor)