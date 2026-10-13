package sqala.static.dsl

import scala.util.NotGiven
import scala.compiletime.ops.boolean.{&&, ||}
import scala.compiletime.ops.int.{>, <}
import scala.compiletime.error
import sqala.metadata.SqlBoolean
import sqala.metadata.AsSqlExpr
import scala.quoted.Type
import scala.quoted.Quotes
import sqala.ir.IrExpr
import sqala.ast.expr.SqlBinaryOperator
import sqala.metadata.SqlNumber
import sqala.metadata.SqlDateTime
import sqala.metadata.SqlInterval
import sqala.metadata.SqlTime

/**
 * The `ExprKind` system tags each expression with its SQL semantic
 * category. This enables compile-time validation of where an
 * expression can legally appear — for example, aggregate functions
 * are rejected in `filter` clause, and ungrouped columns are rejected
 * after `groupBy`.
 *
 * The validation rules are encoded as type classes with boolean
 * results, enforced through `given` resolution at compile time.
 */
sealed trait ExprKind

/**
 * A literal value with no table or column context.
 */
final class Value extends ExprKind

/**
 * A column reference, tagged with the query context level `L`.
 */
final class Column[L <: Int] extends ExprKind

/**
 * An aggregate function call. `KS` is the kind tuple of expressions
 * inside the aggregation.
 */
final class Agg[KS <: Tuple] extends ExprKind

/**
 * A window function call. `KS` is the kind tuple of expressions
 * inside the window specification.
 */
final class Window[KS <: Tuple] extends ExprKind

/**
 * An expression inside a `groupBy` clause. `KS` is the kind tuple
 * of the grouped expressions.
 */
final class Grouped[KS <: Tuple] extends ExprKind

/**
 * A column that has not been grouped and cannot appear after
 * `groupBy` without being inside an aggregate function. `L` is the
 * query context level.
 */
final class UngroupedColumn[L <: Int] extends ExprKind

/**
 * A composite of multiple expression kinds. `KS` is the combined kind
 * tuple.
 */
final class Composite[KS <: Tuple] extends ExprKind

/**
 * The cardinality of a subquery result.
 */
sealed trait QuerySize

/**
 * The subquery returns at most one row (scalar subquery).
 */
final class OneRow extends QuerySize

/**
 * The subquery may return more than one row.
 */
final class ManyRows extends QuerySize

/**
 * Transforms an expression's semantic kind to a target kind
 * without changing the underlying value type.
 */
trait TransformExprKind[T, K <: ExprKind]:
    /**
     * The transformed type.
     */
    type R

    /**
     * Transforms the expression to the target kind.
     */
    def transform(x: T): R

object TransformExprKind:
    type Aux[T, K <: ExprKind, O] = TransformExprKind[T, K]:
        type R = O

    given expr[T, EK <: ExprKind, K <: ExprKind]: Aux[Expr[T, EK], K, Expr[T, K]] =
        new TransformExprKind[Expr[T, EK], K]:
            type R = Expr[T, K]

            def transform(x: Expr[T, EK]): R =
                Expr(x.asIrExpr)

    given tuple[H, T <: Tuple, K <: ExprKind](using
        h: TransformExprKind[H, K],
        t: TransformExprKind[T, K],
        tt: ToTuple[t.R]
    ): Aux[H *: T, K, h.R *: tt.R] =
        new TransformExprKind[H *: T, K]:
            type R = h.R *: tt.R

            def transform(x: H *: T): R =
                h.transform(x.head) *: tt.toTuple(t.transform(x.tail))

    given tuple1[H, K <: ExprKind](using
        h: TransformExprKind[H, K]
    ): Aux[H *: EmptyTuple, K, h.R *: EmptyTuple] =
        new TransformExprKind[H *: EmptyTuple, K]:
            type R = h.R *: EmptyTuple

            def transform(x: H *: EmptyTuple): R =
                h.transform(x.head) *: EmptyTuple

/**
 * Converts an expression kind to its tuple representation.
 */
trait KindToTuple[K <: ExprKind]:
    /**
     * The tuple representation of the kind.
     */
    type R <: Tuple

object KindToTuple:
    type Aux[K <: ExprKind, O <: Tuple] = KindToTuple[K]:
        type R = O

    given composite[KS <: Tuple]: Aux[Composite[KS], TupleDistinct[KS]] =
        new KindToTuple[Composite[KS]]:
            type R = TupleDistinct[KS]

    given other[K <: ExprKind](using NotGiven[K <:< Composite[?]]): Aux[K, K *: EmptyTuple] =
        new KindToTuple[K]:
            type R = K *: EmptyTuple

/**
 * Combines two expression kinds into one.
 */
trait CombineKind[A <: ExprKind, B <: ExprKind]:
    /**
     * The combined kind.
     */
    type R <: ExprKind

object CombineKind:
    type Aux[A <: ExprKind, B <: ExprKind, O <: ExprKind] = CombineKind[A, B]:
        type R = O

    given [A <: ExprKind, B <: ExprKind](using
        ta: KindToTuple[A],
        tb: KindToTuple[B],
        c: CanCombineKindTuple[Tuple.Concat[ta.R, tb.R]]
    ): Aux[A, B, Composite[TupleDistinct[Tuple.Concat[ta.R, tb.R]]]] =
        new CombineKind[A, B]:
            type R = Composite[TupleDistinct[Tuple.Concat[ta.R, tb.R]]]

/**
 * Combines two kind tuples into one.
 */
trait CombineKindTuple[A <: Tuple, B <: Tuple]:
    /**
     * The combined kind tuple.
     */
    type R <: Tuple

object CombineKindTuple:
    type Aux[A <: Tuple, B <: Tuple, O <: Tuple] = CombineKindTuple[A, B]:
        type R = O

    given [A <: Tuple, B <: Tuple](using
        c: CanCombineKindTuple[Tuple.Concat[A, B]]
    ): Aux[A, B, TupleDistinct[Tuple.Concat[A, B]]] =
        new CombineKindTuple[A, B]:
            type R = TupleDistinct[Tuple.Concat[A, B]]

/**
 * Determines whether two expression kinds can be
 * combined.
 */
trait CanCombineKind[A <: ExprKind, B <: ExprKind]

object CanCombineKind:
    given ungrouped[L <: Int]: CanCombineKind[UngroupedColumn[L], UngroupedColumn[L]]()

    given ungroupedAndValue[L <: Int]: CanCombineKind[UngroupedColumn[L], Value]()

    given valueAndUngrouped[L <: Int]: CanCombineKind[Value, UngroupedColumn[L]]()

    given other[A <: ExprKind, B <: ExprKind](using
        NotGiven[A <:< UngroupedColumn[?]],
        NotGiven[B <:< UngroupedColumn[?]]
    ): CanCombineKind[A, B]()

/**
 * Determines whether all elements of a kind tuple can be
 * combined.
 */
trait CanCombineKindTuple[KS <: Tuple]

object CanCombineKindTuple:
    given tuple[A <: ExprKind, B <: ExprKind, T <: Tuple](using
        CanCombineKind[A, B],
        CanCombineKindTuple[B *: T]
    ): CanCombineKindTuple[A *: B *: T]()

    given tuple1[A <: ExprKind]: CanCombineKindTuple[A *: EmptyTuple]()

    given emptyTuple: CanCombineKindTuple[EmptyTuple]()

/**
 * Marks non-grouped fields as `UngroupedColumn` after a `groupBy`
 * clause. `CL` is the current query context level.
 */
trait ToUngrouped[T, CL <: Int]:
    /**
     * The transformed type.
     */
    type R

    /**
     * Marks the value as ungrouped.
     */
    def toUngrouped(x: T): R

object ToUngrouped:
    type Aux[T, CL <: Int, O] = ToUngrouped[T, CL]:
        type R = O

    given expr[T, EK <: ExprKind, E <: Expr[T, EK], CL <: Int]: Aux[E, CL, Expr[T, UngroupedColumn[CL]]] =
        new ToUngrouped[E, CL]:
            type R = Expr[T, UngroupedColumn[CL]]

            def toUngrouped(x: E): R =
                Expr(x.asIrExpr)

    given tuple[H, T <: Tuple, CL <: Int](using
        h: ToUngrouped[H, CL],
        t: ToUngrouped[T, CL],
        tt: ToTuple[t.R]
    ): Aux[H *: T, CL, h.R *: tt.R] =
        new ToUngrouped[H *: T, CL]:
            type R = h.R *: tt.R

            def toUngrouped(x: H *: T): R =
                h.toUngrouped(x.head) *: tt.toTuple(t.toUngrouped(x.tail))

    given tuple1[H, CL <: Int](using
        h: ToUngrouped[H, CL]
    ): Aux[H *: EmptyTuple, CL, h.R *: EmptyTuple] =
        new ToUngrouped[H *: EmptyTuple, CL]:
            type R = h.R *: EmptyTuple

            def toUngrouped(x: H *: EmptyTuple): R =
                h.toUngrouped(x.head) *: EmptyTuple

/**
 * Removes columns at the current context level (`CL`) from a kind
 * tuple. Used for scope validation in lateral and correlated
 * subqueries.
 */
trait ExcludeCurrentLevelColumn[KS <: Tuple, CL <: Int]:
    /**
     * The filtered kind tuple.
     */
    type R <: Tuple

object ExcludeCurrentLevelColumn:
    type Aux[KS <: Tuple, CL <: Int, O <: Tuple] = ExcludeCurrentLevelColumn[KS, CL]:
        type R = O

    given valueHeadTuple[T <: Tuple, CL <: Int](using
        t: ExcludeCurrentLevelColumn[T, CL]
    ): Aux[Value *: T, CL, t.R] =
        new ExcludeCurrentLevelColumn[Value *: T, CL]:
            type R = t.R

    given columnHeadTuple[HL <: Int, T <: Tuple, CL <: Int](using
        t: ExcludeCurrentLevelColumn[T, CL],
        refl: HL =:= CL
    ): Aux[Column[HL] *: T, CL, t.R] =
        new ExcludeCurrentLevelColumn[Column[HL] *: T, CL]:
            type R = t.R

    given outerColumnHeadTuple[HL <: Int, T <: Tuple, CL <: Int](using
        t: ExcludeCurrentLevelColumn[T, CL],
        refl: CL > HL =:= true
    ): Aux[Column[HL] *: T, CL, Column[HL] *: t.R] =
        new ExcludeCurrentLevelColumn[Column[HL] *: T, CL]:
            type R = Column[HL] *: t.R

    given aggHeadTuple[HS <: Tuple, T <: Tuple, CL <: Int](using
        h: ExcludeCurrentLevelColumn[HS, CL],
        t: ExcludeCurrentLevelColumn[T, CL],
        refl: h.R =:= EmptyTuple
    ): Aux[Agg[HS] *: T, CL, t.R] =
        new ExcludeCurrentLevelColumn[Agg[HS] *: T, CL]:
            type R = t.R

    given aggOuterHeadTuple[HS <: Tuple, T <: Tuple, CL <: Int](using
        h: ExcludeCurrentLevelColumn[HS, CL],
        t: ExcludeCurrentLevelColumn[T, CL],
        refl: NotGiven[h.R =:= EmptyTuple]
    ): Aux[Agg[HS] *: T, CL, Agg[h.R] *: t.R] =
        new ExcludeCurrentLevelColumn[Agg[HS] *: T, CL]:
            type R = Agg[h.R] *: t.R

    given windowHeadTuple[HS <: Tuple, T <: Tuple, CL <: Int](using
        h: ExcludeCurrentLevelColumn[HS, CL],
        t: ExcludeCurrentLevelColumn[T, CL],
        refl: h.R =:= EmptyTuple
    ): Aux[Window[HS] *: T, CL, t.R] =
        new ExcludeCurrentLevelColumn[Window[HS] *: T, CL]:
            type R = t.R

    given windowOuterHeadTuple[HS <: Tuple, T <: Tuple, CL <: Int](using
        h: ExcludeCurrentLevelColumn[HS, CL],
        t: ExcludeCurrentLevelColumn[T, CL],
        refl: NotGiven[h.R =:= EmptyTuple]
    ): Aux[Window[HS] *: T, CL, Window[h.R] *: t.R] =
        new ExcludeCurrentLevelColumn[Window[HS] *: T, CL]:
            type R = Window[h.R] *: t.R

    given groupedHeadTuple[HS <: Tuple, T <: Tuple, CL <: Int](using
        h: ExcludeCurrentLevelColumn[HS, CL],
        t: ExcludeCurrentLevelColumn[T, CL],
        refl: h.R =:= EmptyTuple
    ): Aux[Grouped[HS] *: T, CL, t.R] =
        new ExcludeCurrentLevelColumn[Grouped[HS] *: T, CL]:
            type R = t.R

    given groupedOuterHeadTuple[HS <: Tuple, T <: Tuple, CL <: Int](using
        h: ExcludeCurrentLevelColumn[HS, CL],
        t: ExcludeCurrentLevelColumn[T, CL],
        refl: NotGiven[h.R =:= EmptyTuple]
    ): Aux[Grouped[HS] *: T, CL, Grouped[h.R] *: t.R] =
        new ExcludeCurrentLevelColumn[Grouped[HS] *: T, CL]:
            type R = Grouped[h.R] *: t.R

    given ungroupedColumnHeadTuple[HL <: Int, T <: Tuple, CL <: Int](using
        t: ExcludeCurrentLevelColumn[T, CL],
        refl: HL =:= CL
    ): Aux[UngroupedColumn[HL] *: T, CL, t.R] =
        new ExcludeCurrentLevelColumn[UngroupedColumn[HL] *: T, CL]:
            type R = t.R

    given outerUngroupedColumnHeadTuple[HL <: Int, T <: Tuple, CL <: Int](using
        t: ExcludeCurrentLevelColumn[T, CL],
        refl: CL > HL =:= true
    ): Aux[UngroupedColumn[HL] *: T, CL, UngroupedColumn[HL] *: t.R] =
        new ExcludeCurrentLevelColumn[UngroupedColumn[HL] *: T, CL]:
            type R = UngroupedColumn[HL] *: t.R

    given emptyTuple[CL <: Int]: Aux[EmptyTuple, CL, EmptyTuple] =
        new ExcludeCurrentLevelColumn[EmptyTuple, CL]:
            type R = EmptyTuple

/**
 * Keeps only columns at the current context level (`CL`) in a kind
 * tuple. Used to verify that a correlated subquery correctly
 * references columns from the outer query.
 */
trait IncludeCurrentLevelColumn[KS <: Tuple, CL <: Int]:
    /**
     * The filtered kind tuple.
     */
    type R <: Tuple

object IncludeCurrentLevelColumn:
    type Aux[KS <: Tuple, CL <: Int, O <: Tuple] = IncludeCurrentLevelColumn[KS, CL]:
        type R = O

    given valueHeadTuple[T <: Tuple, CL <: Int](using
        t: IncludeCurrentLevelColumn[T, CL]
    ): Aux[Value *: T, CL, t.R] =
        new IncludeCurrentLevelColumn[Value *: T, CL]:
            type R = t.R

    given columnHeadTuple[HL <: Int, T <: Tuple, CL <: Int](using
        t: IncludeCurrentLevelColumn[T, CL],
        refl: HL =:= CL
    ): Aux[Column[HL] *: T, CL, Column[HL] *: t.R] =
        new IncludeCurrentLevelColumn[Column[HL] *: T, CL]:
            type R = Column[HL] *: t.R

    given outerColumnHeadTuple[HL <: Int, T <: Tuple, CL <: Int](using
        t: IncludeCurrentLevelColumn[T, CL],
        refl: CL > HL =:= true
    ): Aux[Column[HL] *: T, CL, t.R] =
        new IncludeCurrentLevelColumn[Column[HL] *: T, CL]:
            type R = t.R

    given aggHeadTuple[HS <: Tuple, T <: Tuple, CL <: Int](using
        h: IncludeCurrentLevelColumn[HS, CL],
        t: IncludeCurrentLevelColumn[T, CL],
        refl: h.R =:= EmptyTuple
    ): Aux[Agg[HS] *: T, CL, t.R] =
        new IncludeCurrentLevelColumn[Agg[HS] *: T, CL]:
            type R = t.R

    given aggOuterHeadTuple[HS <: Tuple, T <: Tuple, CL <: Int](using
        h: IncludeCurrentLevelColumn[HS, CL],
        t: IncludeCurrentLevelColumn[T, CL],
        refl: NotGiven[h.R =:= EmptyTuple]
    ): Aux[Agg[HS] *: T, CL, Agg[h.R] *: t.R] =
        new IncludeCurrentLevelColumn[Agg[HS] *: T, CL]:
            type R = Agg[h.R] *: t.R

    given windowHeadTuple[HS <: Tuple, T <: Tuple, CL <: Int](using
        h: IncludeCurrentLevelColumn[HS, CL],
        t: IncludeCurrentLevelColumn[T, CL],
        refl: h.R =:= EmptyTuple
    ): Aux[Window[HS] *: T, CL, t.R] =
        new IncludeCurrentLevelColumn[Window[HS] *: T, CL]:
            type R = t.R

    given windowOuterHeadTuple[HS <: Tuple, T <: Tuple, CL <: Int](using
        h: IncludeCurrentLevelColumn[HS, CL],
        t: IncludeCurrentLevelColumn[T, CL],
        refl: NotGiven[h.R =:= EmptyTuple]
    ): Aux[Window[HS] *: T, CL, Window[h.R] *: t.R] =
        new IncludeCurrentLevelColumn[Window[HS] *: T, CL]:
            type R = Window[h.R] *: t.R

    given groupedHeadTuple[HS <: Tuple, T <: Tuple, CL <: Int](using
        h: IncludeCurrentLevelColumn[HS, CL],
        t: IncludeCurrentLevelColumn[T, CL],
        refl: h.R =:= EmptyTuple
    ): Aux[Grouped[HS] *: T, CL, t.R] =
        new IncludeCurrentLevelColumn[Grouped[HS] *: T, CL]:
            type R = t.R

    given groupedOuterHeadTuple[HS <: Tuple, T <: Tuple, CL <: Int](using
        h: IncludeCurrentLevelColumn[HS, CL],
        t: IncludeCurrentLevelColumn[T, CL],
        refl: NotGiven[h.R =:= EmptyTuple]
    ): Aux[Grouped[HS] *: T, CL, Grouped[h.R] *: t.R] =
        new IncludeCurrentLevelColumn[Grouped[HS] *: T, CL]:
            type R = Grouped[h.R] *: t.R

    given ungroupedColumnHeadTuple[HL <: Int, T <: Tuple, CL <: Int](using
        t: IncludeCurrentLevelColumn[T, CL],
        refl: HL =:= CL
    ): Aux[UngroupedColumn[HL] *: T, CL, UngroupedColumn[HL] *: t.R] =
        new IncludeCurrentLevelColumn[UngroupedColumn[HL] *: T, CL]:
            type R = UngroupedColumn[HL] *: t.R

    given outerUngroupedColumnHeadTuple[HL <: Int, T <: Tuple, CL <: Int](using
        t: IncludeCurrentLevelColumn[T, CL],
        refl: CL > HL =:= true
    ): Aux[UngroupedColumn[HL] *: T, CL, t.R] =
        new IncludeCurrentLevelColumn[UngroupedColumn[HL] *: T, CL]:
            type R = t.R

    given emptyTuple[CL <: Int]: Aux[EmptyTuple, CL, EmptyTuple] =
        new IncludeCurrentLevelColumn[EmptyTuple, CL]:
            type R = EmptyTuple

/**
 * Tests whether an expression kind is a specific kind. `K` is the
 * kind to test, `TK` is the target kind.
 */
trait IsKind[K <: ExprKind, TK <: ExprKind]:
    /**
     * `true` if `K` is `TK`.
     */
    type R <: Boolean

object IsKind:
    type Aux[K <: ExprKind, TK <: ExprKind, O <: Boolean] = IsKind[K, TK]:
        type R = O

    given kind[K <: ExprKind, TK <: ExprKind](using K <:< TK): Aux[K, TK, true] =
        new IsKind[K, TK]:
            type R = true

    given other[K <: ExprKind, TK <: ExprKind](using NotGiven[K <:< TK]): Aux[K, TK, false] =
        new IsKind[K, TK]:
            type R = false

/**
 * Tests whether a kind tuple contains at least one instance of a
 * specific kind. `KS` is the kind tuple, `TK` is the target kind.
 */
trait HasKind[KS <: Tuple, TK <: ExprKind]:
    /**
     * `true` if any element matches `TK`.
     */
    type R <: Boolean

object HasKind:
    type Aux[KS <: Tuple, TK <: ExprKind, O <: Boolean] = HasKind[KS, TK]:
        type R = O

    given windowHeadTuple[KS <: Tuple, T <: Tuple, TK <: ExprKind](using
        h: HasKind[KS, TK],
        t: HasKind[T, TK],
        refl: NotGiven[TK <:< Window[?]]
    ): Aux[Window[KS] *: T, TK, h.R || t.R] =
        new HasKind[Window[KS] *: T, TK]:
            type R = h.R || t.R

    given tuple[H <: ExprKind, T <: Tuple, TK <: ExprKind](using
        h: IsKind[H, TK],
        t: HasKind[T, TK]
    ): Aux[H *: T, TK, h.R || t.R] =
        new HasKind[H *: T, TK]:
            type R = h.R || t.R

    given emptyTuple[TK <: ExprKind]: Aux[EmptyTuple, TK, false] =
        new HasKind[EmptyTuple, TK]:
            type R = false

/**
 * Tests whether all elements in a kind tuple are of a specific kind.
 * `KS` is the kind tuple, `TK` is the target kind.
 */
trait AllIsKind[KS <: Tuple, TK <: ExprKind]:
    /**
     * `true` if every element matches `TK`.
     */
    type R <: Boolean

object AllIsKind:
    type Aux[KS <: Tuple, TK <: ExprKind, O <: Boolean] = AllIsKind[KS, TK]:
        type R = O

    given tuple[H <: ExprKind, T <: Tuple, TK <: ExprKind](using
        h: IsKind[H, TK],
        t: AllIsKind[T, TK]
    ): Aux[H *: T, TK, h.R && t.R] =
        new AllIsKind[H *: T, TK]:
            type R = h.R && t.R

    given tuple1[H <: ExprKind, TK <: ExprKind](using
        h: IsKind[H, TK]
    ): Aux[H *: EmptyTuple, TK, h.R] =
        new AllIsKind[H *: EmptyTuple, TK]:
            type R = h.R

/**
 * Validates that a kind tuple contains aggregate functions nested.
 */
trait AggNested[KS <: Tuple]:
    /**
     * `true` if the kind tuple contains aggregate functions nested.
     */
    type R <: Boolean

object AggNested:
    type Aux[KS <: Tuple, O <: Boolean] = AggNested[KS]:
        type R = O

    given valueHeadTuple[T <: Tuple](using
        t: AggNested[T]
    ): Aux[Value *: T, t.R] =
        new AggNested[Value *: T]:
            type R = t.R

    given columnHeadTuple[L <: Int, T <: Tuple](using
        t: AggNested[T]
    ): Aux[Column[L] *: T, t.R] =
        new AggNested[Column[L] *: T]:
            type R = t.R

    given aggHeadTuple[KS <: Tuple, T <: Tuple](using
        h: HasKind[KS, Agg[?]],
        t: AggNested[T]
    ): Aux[Agg[KS] *: T, h.R || t.R] =
        new AggNested[Agg[KS] *: T]:
            type R = h.R || t.R

    given windowHeadTuple[KS <: Tuple, T <: Tuple](using
        h: AggNested[KS],
        t: AggNested[T]
    ): Aux[Window[KS] *: T, h.R || t.R] =
        new AggNested[Window[KS] *: T]:
            type R = h.R || t.R

    given groupedHeadTuple[KS <: Tuple, T <: Tuple](using
        h: AggNested[KS],
        t: AggNested[T]
    ): Aux[Grouped[KS] *: T, h.R || t.R] =
        new AggNested[Grouped[KS] *: T]:
            type R = h.R || t.R

    given ungroupedColumnHeadTuple[L <: Int, T <: Tuple](using
        t: AggNested[T]
    ): Aux[UngroupedColumn[L] *: T, t.R] =
        new AggNested[UngroupedColumn[L] *: T]:
            type R = t.R

    given compositeHeadTuple[KS <: Tuple, T <: Tuple](using
        h: AggNested[KS],
        t: AggNested[T]
    ): Aux[Composite[KS] *: T, h.R || t.R] =
        new AggNested[Composite[KS] *: T]:
            type R = h.R || t.R

    given emptyTuple: Aux[EmptyTuple, false] =
        new AggNested[EmptyTuple]:
            type R = false

/**
 * Validates that a kind tuple contains no aggregate functions nested.
 */
trait AggNotNested[KS <: Tuple]

object AggNotNested:
    given validate[KS <: Tuple](using
        an: AggNested[KS],
        ra: an.R =:= false
    ): AggNotNested[KS]()

    inline given fallback[KS <: Tuple](using
        NotGiven[AggNotNested[KS]]
    ): AggNotNested[KS] =
        error("Aggregate function calls cannot be nested.")

/**
 * Validates that a kind tuple contains no column references
 * that would create variable capture issues.
 */
trait IsNotVariable[KS <: Tuple]

object IsNotVariable:
    given validate[KS <: Tuple](using
        hc: HasKind[KS, Column[?]],
        nc: hc.R =:= false,
        ha: HasKind[KS, Agg[?]],
        na: ha.R =:= false,
        hw: HasKind[KS, Window[?]],
        nw: hw.R =:= false,
        hg: HasKind[KS, Grouped[?]],
        ng: hg.R =:= false,
        hu: HasKind[KS, UngroupedColumn[?]],
        nu: hu.R =:= false
    ): IsNotVariable[KS]()

    inline given fallback[KS <: Tuple](using
        NotGiven[IsNotVariable[KS]]
    ): IsNotVariable[KS] =
        error("Argument must not contain variables.")

/**
 * Validates that a kind tuple can legally appear inside an
 * aggregate function.
 */
trait CanInAgg[KS <: Tuple]

object CanInAgg:
    given validate[KS <: Tuple](using
        hw: HasKind[KS, Window[?]],
        nw: hw.R =:= false
    ): CanInAgg[KS]()

    inline given fallback[KS <: Tuple](using
        NotGiven[CanInAgg[KS]]
    ): CanInAgg[KS] =
        error("Aggregate function calls cannot contain window function calls.")

/**
 * Validates that a kind tuple contains no window functions nested.
 */
trait WindowNotNested[KS <: Tuple]

object WindowNotNested:
    given validate[KS <: Tuple](using
        hw: HasKind[KS, Window[?]],
        nw: hw.R =:= false
    ): WindowNotNested[KS]()

    inline given fallback[KS <: Tuple](using
        NotGiven[WindowNotNested[KS]]
    ): WindowNotNested[KS] =
        error("Window function calls cannot be nested.")

// TODO 测一下window嵌套的错能不能正常报出来
/**
 * Validates that a kind tuple can appear inside a window
 * specification.
 */
trait CanInWindow[KS <: Tuple]

object CanInWindow:
    given validate[KS <: Tuple](using
        wn: WindowNotNested[KS],
        hu: HasKind[KS, UngroupedColumn[?]],
        nu: hu.R =:= false
    ): CanInWindow[KS]()

    inline given fallback[KS <: Tuple](using
        NotGiven[CanInWindow[KS]]
    ): CanInWindow[KS] =
        error("Window function calls cannot contain ungrouped column references.")

// TODO fallback
/**
 * Determines whether a type can be used as a `rows` or `groups`
 * frame bound.
 */
trait CanInRowsOrGroupsFrame[T]

object CanInRowsOrGroupsFrame:
    given int: CanInRowsOrGroupsFrame[Int]()

    given long: CanInRowsOrGroupsFrame[Long]()

    given nothing: CanInRowsOrGroupsFrame[Nothing]()

/**
 * Determines whether `S` and `T` can be used together in a `range`
 * frame (`S` is the sort key type, `T` is the bound type).
 */
trait CanInRangeFrame[S, T]

object CanInRangeFrame:
    given number[S: SqlNumber, N: SqlNumber]: CanInRangeFrame[S, N]()

    given dateTime[T: SqlDateTime, I: SqlInterval]: CanInRangeFrame[T, I]()

    given time[T: SqlTime, I: SqlInterval]: CanInRangeFrame[T, I]()

    given nothing[S]: CanInRangeFrame[S, Nothing]()

// TODO 注释
trait ValidateFrame[SS <: Int, SN <: String, ES <: Int, EN <: String]

object ValidateFrame:
    given validate[SS <: Int, SN <: String, ES <: Int, EN <: String](using 
        SS < ES =:= true
    ): ValidateFrame[SS, SN, ES, EN]()

    inline given fallback[SS <: Int, SN <: String, ES <: Int, EN <: String](using 
        SS < ES =:= false
    ): ValidateFrame[SS, SN, ES, EN] =
        ${ errorMacro[SN, EN] }

    def errorMacro[S: Type, E: Type](using q: Quotes): Nothing =
        import q.reflect.*

        val start = Type.show[S].replace("\"", "")
        val end = Type.show[E].replace("\"", "")

        report.errorAndAbort(s"Frame starting from $start cannot end at $end.")

// TODO 注释
trait CanCallOver[T]:
    type R

    type KS <: Tuple

    def asIrExpr(x: T): IrExpr

object CanCallOver:
    type Aux[T, O, OKS <: Tuple] = CanCallOver[T]:
        type R = O

        type KS = OKS

    given agg[T, EKS <: Tuple]: Aux[AggExpr[T, EKS], T, EKS] =
        new CanCallOver[AggExpr[T, EKS]]:
            type R = T

            type KS = EKS

            def asIrExpr(x: AggExpr[T, EKS]): IrExpr =
                x.expr

    given window[T, EKS <: Tuple]: Aux[WindowPart[T, EKS], T, EKS] =
        new CanCallOver[WindowPart[T, EKS]]:
            type R = T

            type KS = EKS

            def asIrExpr(x: WindowPart[T, EKS]): IrExpr =
                x.expr

    inline given fallback[T](using
        NotGiven[CanCallOver[T]]
    ): Aux[T, Int, EmptyTuple] =
        error("Over specified, but this expression is not a window function nor an aggregate function.")

/**
 * 注释
 */
trait CallAggFilter[T, FKS <: Tuple]:
    type R

    def call(x: T, filter: IrExpr): R

object CallAggFilter:
    type Aux[T, FKS <: Tuple, O] = CallAggFilter[T, FKS]:
        type R = O

    given agg[T, KS <: Tuple, FKS <: Tuple](using
        c: CombineKindTuple[KS, FKS]
    ): Aux[AggExpr[T, KS], FKS, AggExpr[T, c.R]] =
        new CallAggFilter[AggExpr[T, KS], FKS]:
            type R = AggExpr[T, c.R]

            def call(x: AggExpr[T, KS], filter: IrExpr): AggExpr[T, c.R] =
                def addFilter(originalFilter: Option[IrExpr], filter: IrExpr): Option[IrExpr] =
                    originalFilter.map(IrExpr.Binary(_, SqlBinaryOperator.And, filter)).orElse(Some(filter))

                val expr: IrExpr.CountAsteriskFunc | IrExpr.ListAggFunc | IrExpr.GeneralFunc | IrExpr.JsonObjectAggFunc | IrExpr.JsonArrayAggFunc =
                    x.expr match
                        case c: IrExpr.CountAsteriskFunc =>
                            c.copy(addFilter(c.filter, filter))
                        case g: IrExpr.GeneralFunc =>
                            g.copy(filter = addFilter(g.filter, filter))
                        case l: IrExpr.ListAggFunc =>
                            l.copy(filter = addFilter(l.filter, filter))
                        case j: IrExpr.JsonArrayAggFunc =>
                            j.copy(filter = addFilter(j.filter, filter))
                        case j: IrExpr.JsonObjectAggFunc =>
                            j.copy(filter = addFilter(j.filter, filter))
                AggExpr(expr)

    inline given fallback[T, FKS <: Tuple](using
        NotGiven[CallAggFilter[T, FKS]]
    ): Aux[T, FKS, T] =
        error("Filter specified, but this expression is not an aggregate function.")

// TODO 后面的报错还要细化
/**
 * Validates that a kind tuple contains no aggregate or window
 * functions, allowing it to appear in a simple clause.
 */
trait CanInSimpleClause[KS <: Tuple]

object CanInSimpleClause:
    given validate[KS <: Tuple](using
        ha: HasKind[KS, Agg[?]],
        na: ha.R =:= false,
        hw: HasKind[KS, Window[?]],
        nw: hw.R =:= false,
        hu: HasKind[KS, UngroupedColumn[?]],
        nu: hu.R =:= false
    ): CanInSimpleClause[KS]()

/**
 * Validates that a kind tuple contains no aggregate functions, allowing it to appear in a filter clause.
 */
trait FilterNotAllowedAgg[KS <: Tuple]

object FilterNotAllowedAgg:
    given validate[KS <: Tuple](using
        ha: HasKind[KS, Agg[?]],
        na: ha.R =:= false
    ): FilterNotAllowedAgg[KS]()

    inline given fallback[KS <: Tuple](using
        NotGiven[FilterNotAllowedAgg[KS]]
    ): FilterNotAllowedAgg[KS] =
        error("Aggregate functions are not allowed in filter/on.")

/**
 * Validates that a kind tuple contains no window functions, allowing it to appear in a filter clause.
 */
trait FilterNotAllowedWindow[KS <: Tuple]

object FilterNotAllowedWindow:
    given validate[KS <: Tuple](using
        hw: HasKind[KS, Window[?]],
        nw: hw.R =:= false
    ): FilterNotAllowedWindow[KS]()

    inline given fallback[KS <: Tuple](using
        NotGiven[FilterNotAllowedWindow[KS]]
    ): FilterNotAllowedWindow[KS] =
        error("Window functions are not allowed in filter/on.")

/**
 * Validates that a kind tuple contains no ungrouped column references, allowing it to appear in a filter clause.
 */
trait FilterNotAllowedUngroupedColumn[KS <: Tuple]

object FilterNotAllowedUngroupedColumn:
    given validate[KS <: Tuple](using
        hu: HasKind[KS, UngroupedColumn[?]],
        nu: hu.R =:= false
    ): FilterNotAllowedUngroupedColumn[KS]()

    inline given fallback[KS <: Tuple](using
        NotGiven[FilterNotAllowedUngroupedColumn[KS]]
    ): FilterNotAllowedUngroupedColumn[KS] =
        error("Ungrouped column references are not allowed in filter/on.")

/**
 * Validates that a kind tuple can appear in a `filter` clause.
 */
trait CanInFilter[T, KS <: Tuple]

object CanInFilter:
    given validate[T, KS <: Tuple](using
        FilterNotAllowedAgg[KS],
        FilterNotAllowedWindow[KS],
        FilterNotAllowedUngroupedColumn[KS],
        SqlBoolean[T]
    ): CanInFilter[T, KS]()

    inline given fallback[T, KS <: Tuple](using
        NotGiven[CanInFilter[T, KS]]
    ): CanInFilter[T, KS] =
        error("Argument of filter/on must be type Boolean.")

/**
 * Validates that a kind tuple can appear in a `map` clause.
 */
trait CanInMap[KS <: Tuple]:
    /**
     * The resulting query size.
     */
    type R <: QuerySize

object CanInMap:
    type Aux[KS <: Tuple, O <: QuerySize] = CanInMap[KS]:
        type R = O

    given oneRow[KS <: Tuple](using
        ha: HasKind[KS, Agg[?]],
        ia: ha.R =:= true,
        hc: HasKind[KS, Column[?]],
        nc: hc.R =:= false,
        hu: HasKind[KS, UngroupedColumn[?]],
        nu: hu.R =:= false
    ): Aux[KS, OneRow] =
        new CanInMap[KS]:
            type R = OneRow

    given manyRows[KS <: Tuple](using
        ha: HasKind[KS, Agg[?]],
        ia: ha.R =:= false,
        hu: HasKind[KS, UngroupedColumn[?]],
        nu: hu.R =:= false
    ): Aux[KS, ManyRows] =
        new CanInMap[KS]:
            type R = ManyRows

/**
 * Validates that a kind tuple can appear in a `map` clause
 * after `groupBy`.
 */
trait CanInGroupedMap[KS <: Tuple]

object CanInGroupedMap:
    given validate[KS <: Tuple](using
        hc: HasKind[KS, Column[?]],
        nc: hc.R =:= false,
        hu: HasKind[KS, UngroupedColumn[?]],
        nu: hu.R =:= false
    ): CanInGroupedMap[KS]()

/**
 * Validates that a kind tuple can appear in a `sortBy` clause.
 * `S` is the query size of the result.
 */
trait CanInSort[KS <: Tuple, S <: QuerySize]

object CanInSort:
    given oneRow[KS <: Tuple](using
        hc: HasKind[KS, Column[?]],
        nc: hc.R =:= false,
        hu: HasKind[KS, UngroupedColumn[?]],
        nu: hu.R =:= false
    ): CanInSort[KS, OneRow]()

    given manyRows[KS <: Tuple](using
        ha: HasKind[KS, Agg[?]],
        ia: ha.R =:= false,
        hu: HasKind[KS, UngroupedColumn[?]],
        nu: hu.R =:= false
    ): CanInSort[KS, ManyRows]()

/**
 * Validates that a kind tuple can appear in a `sortBy` clause
 * after `groupBy`.
 */
trait CanInGroupedSort[KS <: Tuple]

object CanInGroupedSort:
    given validate[KS <: Tuple](using
        hc: HasKind[KS, Column[?]],
        nc: hc.R =:= false,
        hu: HasKind[KS, UngroupedColumn[?]],
        nu: hu.R =:= false
    ): CanInGroupedSort[KS]()

/**
 * Validates that a kind tuple can appear in a `groupBy`
 * clause.
 */
trait CanInGroup[KS <: Tuple]

object CanInGroup:
    given validate[KS <: Tuple](using
        ha: HasKind[KS, Agg[?]],
        na: ha.R =:= false,
        hw: HasKind[KS, Window[?]],
        nw: hw.R =:= false,
        hu: HasKind[KS, UngroupedColumn[?]],
        nu: hu.R =:= false
    ): CanInGroup[KS]()

/**
 * Validates that a kind tuple can appear in a `having` clause.
 */
trait CanInHaving[KS <: Tuple]

object CanInHaving:
    given validate[KS <: Tuple](using
        hc: HasKind[KS, Column[?]],
        nc: hc.R =:= false,
        hw: HasKind[KS, Window[?]],
        nw: hw.R =:= false,
        hu: HasKind[KS, UngroupedColumn[?]],
        nu: hu.R =:= false
    ): CanInHaving[KS]()

/**
 * Validates that a kind tuple can appear in a `define` clause
 * of `matchRecognize`. `L` is the query context level.
 */
trait CanInRecognizeDefine[KS <: Tuple, CL <: Int]

object CanInRecognizeDefine:
    given validate[KS <: Tuple, CL <: Int](using
        hc: HasKind[KS, Column[?]],
        nc: hc.R =:= false,
        hw: HasKind[KS, Window[?]],
        nw: hw.R =:= false,
        hu: HasKind[KS, UngroupedColumn[?]],
        nu: hu.R =:= false,
        oks: ExcludeCurrentLevelColumn[KS, CL],
        e: oks.R =:= EmptyTuple
    ): CanInRecognizeDefine[KS, CL]()

/**
 * Validates that a subquery used as an expression returns most one row.
 */
trait ValidateSubqueryExprSize[S <: QuerySize]

object ValidateSubqueryExprSize:
    given oneRow: ValidateSubqueryExprSize[OneRow]()

    inline given manyRows: ValidateSubqueryExprSize[ManyRows] =
        error("More than one row returned by a subquery used as an expression.")

/**
 * Validates that a scalar subquery returns one column.
 */
trait ValidateScalarSubqueryColumnSize[T]

object ValidateScalarSubqueryColumnSize:
    given validate[T: AsSqlExpr]: ValidateScalarSubqueryColumnSize[T]()

    inline given fallback[T](using
        NotGiven[ValidateScalarSubqueryColumnSize[T]]
    ): ValidateScalarSubqueryColumnSize[T] =
        error("Subquery must return only one column.")

// TODO 注释和错误文案
trait ValidateSubqueryScope[L <: Int, CL <: Int]

object ValidateSubqueryScope:
    given validate[L <: Int, CL <: Int](using
        L > CL =:= true
    ): ValidateSubqueryScope[L, CL]()

    inline given fallback[L <: Int, CL <: Int](using
        L > CL =:= false
    ): ValidateSubqueryScope[L, CL] =
        error("Subqueries are not allowed to be called across scopes.")

/**
 * Determines the query size after a `take` clause.
 */
trait TakeSize[T]:
    /**
     * The resulting query size.
     */
    type R <: QuerySize

object TakeSize:
    type Aux[T, O <: QuerySize] = TakeSize[T]:
        type R = O

    given zeroRow: Aux[0, OneRow] =
        new TakeSize[0]:
            type R = OneRow

    given oneRow: Aux[1, OneRow] =
        new TakeSize[1]:
            type R = OneRow

    given manyRows[T](using
        NotGiven[T =:= 0],
        NotGiven[T =:= 1]
    ): Aux[T, ManyRows] =
        new TakeSize[T]:
            type R = ManyRows

/**
 * TODO
 */
trait ValidateArgType[F[_], T]

object ValidateArgType:
    given validate[F[_], T](using F[T]): ValidateArgType[F, T]()

    inline given fallback[F[_], T, OP <: String](using NotGiven[F[T]]): ValidateArgType[F, T] =
        error("The parameter type is incorrect.")