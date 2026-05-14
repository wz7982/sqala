package sqala.static.dsl

import scala.util.NotGiven
import scala.compiletime.ops.boolean.{&&, ||}
import scala.compiletime.ops.int.>

sealed trait ExprKind
final class Value extends ExprKind
final class Column[L <: Int] extends ExprKind
final class Agg[KS <: Tuple] extends ExprKind
final class Window[KS <: Tuple] extends ExprKind
final class Grouped[KS <: Tuple] extends ExprKind
final class UngroupedColumn[L <: Int] extends ExprKind
final class Composite[KS <: Tuple] extends ExprKind

sealed trait QuerySize
final class OneRow extends QuerySize
final class ManyRows extends QuerySize

trait TransformExprKind[T, K <: ExprKind]:
    type R

    def transform(x: T): R

object TransformExprKind:
    type Aux[T, K <: ExprKind, O] = TransformExprKind[T, K]:
        type R = O

    given expr[T, EK <: ExprKind, K <: ExprKind]: Aux[Expr[T, EK], K, Expr[T, K]] =
        new TransformExprKind[Expr[T, EK], K]:
            type R = Expr[T, K]

            def transform(x: Expr[T, EK]): R =
                Expr(x.asSqlExpr)

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

trait KindToTuple[K <: ExprKind]:
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

trait CombineKind[A <: ExprKind, B <: ExprKind]:
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

trait CombineKindTuple[A <: Tuple, B <: Tuple]:
    type R <: Tuple

object CombineKindTuple:
    type Aux[A <: Tuple, B <: Tuple, O <: Tuple] = CombineKindTuple[A, B]:
        type R = O

    given [A <: Tuple, B <: Tuple](using
        c: CanCombineKindTuple[Tuple.Concat[A, B]]
    ): Aux[A, B, TupleDistinct[Tuple.Concat[A, B]]] =
        new CombineKindTuple[A, B]:
            type R = TupleDistinct[Tuple.Concat[A, B]]

trait CanCombineKind[A <: ExprKind, B <: ExprKind]

object CanCombineKind:
    given ungrouped[L <: Int]: CanCombineKind[UngroupedColumn[L], UngroupedColumn[L]]()

    given ungroupedAndValue[L <: Int]: CanCombineKind[UngroupedColumn[L], Value]()

    given valueAndUngrouped[L <: Int]: CanCombineKind[Value, UngroupedColumn[L]]()

    given other[A <: ExprKind, B <: ExprKind](using
        NotGiven[A <:< UngroupedColumn[?]],
        NotGiven[B <:< UngroupedColumn[?]]
    ): CanCombineKind[A, B]()

trait CanCombineKindTuple[KS <: Tuple]

object CanCombineKindTuple:
    given tuple[A <: ExprKind, B <: ExprKind, T <: Tuple](using
        CanCombineKind[A, B],
        CanCombineKindTuple[B *: T]
    ): CanCombineKindTuple[A *: B *: T]()

    given tuple1[A <: ExprKind]: CanCombineKindTuple[A *: EmptyTuple]()

    given emptyTuple: CanCombineKindTuple[EmptyTuple]()

trait ToUngrouped[T, CL <: Int]:
    type R

    def toUngrouped(x: T): R

object ToUngrouped:
    type Aux[T, CL <: Int, O] = ToUngrouped[T, CL]:
        type R = O

    given expr[T, EK <: ExprKind, CL <: Int]: Aux[Expr[T, EK], CL, Expr[T, UngroupedColumn[CL]]] =
        new ToUngrouped[Expr[T, EK], CL]:
            type R = Expr[T, UngroupedColumn[CL]]

            def toUngrouped(x: Expr[T, EK]): R =
                Expr(x.asSqlExpr)

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

trait ExcludeCurrentLevelColumn[KS <: Tuple, L <: Int]:
    type R <: Tuple

object ExcludeCurrentLevelColumn:
    type Aux[KS <: Tuple, L <: Int, O <: Tuple] = ExcludeCurrentLevelColumn[KS, L]:
        type R = O

    given valueHeadTuple[T <: Tuple, L <: Int](using
        t: ExcludeCurrentLevelColumn[T, L]
    ): Aux[Value *: T, L, t.R] =
        new ExcludeCurrentLevelColumn[Value *: T, L]:
            type R = t.R

    given columnHeadTuple[HL <: Int, T <: Tuple, L <: Int](using
        t: ExcludeCurrentLevelColumn[T, L],
        refl: HL =:= L
    ): Aux[Column[HL] *: T, L, t.R] =
        new ExcludeCurrentLevelColumn[Column[HL] *: T, L]:
            type R = t.R

    given outerColumnHeadTuple[HL <: Int, T <: Tuple, L <: Int](using
        t: ExcludeCurrentLevelColumn[T, L],
        refl: L > HL =:= true
    ): Aux[Column[HL] *: T, L, Column[HL] *: t.R] =
        new ExcludeCurrentLevelColumn[Column[HL] *: T, L]:
            type R = Column[HL] *: t.R

    given aggHeadTuple[HS <: Tuple, T <: Tuple, L <: Int](using
        h: ExcludeCurrentLevelColumn[HS, L],
        t: ExcludeCurrentLevelColumn[T, L],
        refl: h.R =:= EmptyTuple
    ): Aux[Agg[HS] *: T, L, t.R] =
        new ExcludeCurrentLevelColumn[Agg[HS] *: T, L]:
            type R = t.R

    given aggOuterHeadTuple[HS <: Tuple, T <: Tuple, L <: Int](using
        h: ExcludeCurrentLevelColumn[HS, L],
        t: ExcludeCurrentLevelColumn[T, L],
        refl: NotGiven[h.R =:= EmptyTuple]
    ): Aux[Agg[HS] *: T, L, Agg[h.R] *: t.R] =
        new ExcludeCurrentLevelColumn[Agg[HS] *: T, L]:
            type R = Agg[h.R] *: t.R

    given windowHeadTuple[HS <: Tuple, T <: Tuple, L <: Int](using
        h: ExcludeCurrentLevelColumn[HS, L],
        t: ExcludeCurrentLevelColumn[T, L],
        refl: h.R =:= EmptyTuple
    ): Aux[Window[HS] *: T, L, t.R] =
        new ExcludeCurrentLevelColumn[Window[HS] *: T, L]:
            type R = t.R

    given windowOuterHeadTuple[HS <: Tuple, T <: Tuple, L <: Int](using
        h: ExcludeCurrentLevelColumn[HS, L],
        t: ExcludeCurrentLevelColumn[T, L],
        refl: NotGiven[h.R =:= EmptyTuple]
    ): Aux[Window[HS] *: T, L, Window[h.R] *: t.R] =
        new ExcludeCurrentLevelColumn[Window[HS] *: T, L]:
            type R = Window[h.R] *: t.R

    given groupedHeadTuple[HS <: Tuple, T <: Tuple, L <: Int](using
        h: ExcludeCurrentLevelColumn[HS, L],
        t: ExcludeCurrentLevelColumn[T, L],
        refl: h.R =:= EmptyTuple
    ): Aux[Grouped[HS] *: T, L, t.R] =
        new ExcludeCurrentLevelColumn[Grouped[HS] *: T, L]:
            type R = t.R

    given groupedOuterHeadTuple[HS <: Tuple, T <: Tuple, L <: Int](using
        h: ExcludeCurrentLevelColumn[HS, L],
        t: ExcludeCurrentLevelColumn[T, L],
        refl: NotGiven[h.R =:= EmptyTuple]
    ): Aux[Grouped[HS] *: T, L, Grouped[h.R] *: t.R] =
        new ExcludeCurrentLevelColumn[Grouped[HS] *: T, L]:
            type R = Grouped[h.R] *: t.R

    given ungroupedColumnHeadTuple[HL <: Int, T <: Tuple, L <: Int](using
        t: ExcludeCurrentLevelColumn[T, L],
        refl: HL =:= L
    ): Aux[UngroupedColumn[HL] *: T, L, t.R] =
        new ExcludeCurrentLevelColumn[UngroupedColumn[HL] *: T, L]:
            type R = t.R

    given outerUngroupedColumnHeadTuple[HL <: Int, T <: Tuple, L <: Int](using
        t: ExcludeCurrentLevelColumn[T, L],
        refl: L > HL =:= true
    ): Aux[UngroupedColumn[HL] *: T, L, UngroupedColumn[HL] *: t.R] =
        new ExcludeCurrentLevelColumn[UngroupedColumn[HL] *: T, L]:
            type R = UngroupedColumn[HL] *: t.R

    given emptyTuple[L <: Int]: Aux[EmptyTuple, L, EmptyTuple] =
        new ExcludeCurrentLevelColumn[EmptyTuple, L]:
            type R = EmptyTuple

trait IncludeCurrentLevelColumn[KS <: Tuple, L <: Int]:
    type R <: Tuple

object IncludeCurrentLevelColumn:
    type Aux[KS <: Tuple, L <: Int, O <: Tuple] = IncludeCurrentLevelColumn[KS, L]:
        type R = O

    given valueHeadTuple[T <: Tuple, L <: Int](using
        t: IncludeCurrentLevelColumn[T, L]
    ): Aux[Value *: T, L, t.R] =
        new IncludeCurrentLevelColumn[Value *: T, L]:
            type R = t.R

    given columnHeadTuple[HL <: Int, T <: Tuple, L <: Int](using
        t: IncludeCurrentLevelColumn[T, L],
        refl: HL =:= L
    ): Aux[Column[HL] *: T, L, Column[HL] *: t.R] =
        new IncludeCurrentLevelColumn[Column[HL] *: T, L]:
            type R = Column[HL] *: t.R

    given outerColumnHeadTuple[HL <: Int, T <: Tuple, L <: Int](using
        t: IncludeCurrentLevelColumn[T, L],
        refl: L > HL =:= true
    ): Aux[Column[HL] *: T, L, t.R] =
        new IncludeCurrentLevelColumn[Column[HL] *: T, L]:
            type R = t.R

    given aggHeadTuple[HS <: Tuple, T <: Tuple, L <: Int](using
        h: IncludeCurrentLevelColumn[HS, L],
        t: IncludeCurrentLevelColumn[T, L],
        refl: h.R =:= EmptyTuple
    ): Aux[Agg[HS] *: T, L, t.R] =
        new IncludeCurrentLevelColumn[Agg[HS] *: T, L]:
            type R = t.R

    given aggOuterHeadTuple[HS <: Tuple, T <: Tuple, L <: Int](using
        h: IncludeCurrentLevelColumn[HS, L],
        t: IncludeCurrentLevelColumn[T, L],
        refl: NotGiven[h.R =:= EmptyTuple]
    ): Aux[Agg[HS] *: T, L, Agg[h.R] *: t.R] =
        new IncludeCurrentLevelColumn[Agg[HS] *: T, L]:
            type R = Agg[h.R] *: t.R

    given windowHeadTuple[HS <: Tuple, T <: Tuple, L <: Int](using
        h: IncludeCurrentLevelColumn[HS, L],
        t: IncludeCurrentLevelColumn[T, L],
        refl: h.R =:= EmptyTuple
    ): Aux[Window[HS] *: T, L, t.R] =
        new IncludeCurrentLevelColumn[Window[HS] *: T, L]:
            type R = t.R

    given windowOuterHeadTuple[HS <: Tuple, T <: Tuple, L <: Int](using
        h: IncludeCurrentLevelColumn[HS, L],
        t: IncludeCurrentLevelColumn[T, L],
        refl: NotGiven[h.R =:= EmptyTuple]
    ): Aux[Window[HS] *: T, L, Window[h.R] *: t.R] =
        new IncludeCurrentLevelColumn[Window[HS] *: T, L]:
            type R = Window[h.R] *: t.R

    given groupedHeadTuple[HS <: Tuple, T <: Tuple, L <: Int](using
        h: IncludeCurrentLevelColumn[HS, L],
        t: IncludeCurrentLevelColumn[T, L],
        refl: h.R =:= EmptyTuple
    ): Aux[Grouped[HS] *: T, L, t.R] =
        new IncludeCurrentLevelColumn[Grouped[HS] *: T, L]:
            type R = t.R

    given groupedOuterHeadTuple[HS <: Tuple, T <: Tuple, L <: Int](using
        h: IncludeCurrentLevelColumn[HS, L],
        t: IncludeCurrentLevelColumn[T, L],
        refl: NotGiven[h.R =:= EmptyTuple]
    ): Aux[Grouped[HS] *: T, L, Grouped[h.R] *: t.R] =
        new IncludeCurrentLevelColumn[Grouped[HS] *: T, L]:
            type R = Grouped[h.R] *: t.R

    given ungroupedColumnHeadTuple[HL <: Int, T <: Tuple, L <: Int](using
        t: IncludeCurrentLevelColumn[T, L],
        refl: HL =:= L
    ): Aux[UngroupedColumn[HL] *: T, L, UngroupedColumn[HL] *: t.R] =
        new IncludeCurrentLevelColumn[UngroupedColumn[HL] *: T, L]:
            type R = UngroupedColumn[HL] *: t.R

    given outerUngroupedColumnHeadTuple[HL <: Int, T <: Tuple, L <: Int](using
        t: IncludeCurrentLevelColumn[T, L],
        refl: L > HL =:= true
    ): Aux[UngroupedColumn[HL] *: T, L, t.R] =
        new IncludeCurrentLevelColumn[UngroupedColumn[HL] *: T, L]:
            type R = t.R

    given emptyTuple[L <: Int]: Aux[EmptyTuple, L, EmptyTuple] =
        new IncludeCurrentLevelColumn[EmptyTuple, L]:
            type R = EmptyTuple

trait IsKind[K <: ExprKind, TK <: ExprKind]:
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

trait HasKind[KS <: Tuple, TK <: ExprKind]:
    type R <: Boolean

object HasKind:
    type Aux[KS <: Tuple, TK <: ExprKind, O <: Boolean] = HasKind[KS, TK]:
        type R = O

    given windowHeadTuple[KS <: Tuple, T <: Tuple, TK <: ExprKind](using
        h: HasKind[KS, TK],
        t: HasKind[T, TK]
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

trait AllIsKind[KS <: Tuple, TK <: ExprKind]:
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

trait CanInAgg[KS <: Tuple]

object CanInAgg:
    given validate[KS <: Tuple](using
        ha: HasKind[KS, Agg[?]],
        na: ha.R =:= false,
        hw: HasKind[KS, Window[?]],
        nw: hw.R =:= false
    ): CanInAgg[KS]()

trait CanCallOver[K <: ExprKind]

object CanCallOver:
    given agg[K <: Agg[?]]: CanCallOver[K]()

trait CanInWindow[KS <: Tuple]

object CanInWindow:
    given validate[KS <: Tuple](using
        hw: HasKind[KS, Window[?]],
        nw: hw.R =:= false,
        hu: HasKind[KS, UngroupedColumn[?]],
        nu: hu.R =:= false
    ): CanInWindow[KS]()

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

trait CanInFilter[KS <: Tuple]

object CanInFilter:
    given validate[KS <: Tuple](using
        ha: HasKind[KS, Agg[?]],
        na: ha.R =:= false,
        hw: HasKind[KS, Window[?]],
        nw: hw.R =:= false,
        hu: HasKind[KS, UngroupedColumn[?]],
        nu: hu.R =:= false
    ): CanInFilter[KS]()

trait CanInMap[KS <: Tuple]:
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

trait CanInGroupedMap[KS <: Tuple]

object CanInGroupedMap:
    given validate[KS <: Tuple](using
        hc: HasKind[KS, Column[?]],
        nc: hc.R =:= false,
        hu: HasKind[KS, UngroupedColumn[?]],
        nu: hu.R =:= false
    ): CanInGroupedMap[KS]()

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

trait CanInGroupedSort[KS <: Tuple]

object CanInGroupedSort:
    given validate[KS <: Tuple](using
        hc: HasKind[KS, Column[?]],
        nc: hc.R =:= false,
        hu: HasKind[KS, UngroupedColumn[?]],
        nu: hu.R =:= false
    ): CanInGroupedSort[KS]()

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

trait CanInRecognizeDefine[KS <: Tuple, L <: Int]

object CanInRecognizeDefine:
    given validate[KS <: Tuple, L <: Int](using
        hc: HasKind[KS, Column[?]],
        nc: hc.R =:= false,
        hw: HasKind[KS, Window[?]],
        nw: hw.R =:= false,
        hu: HasKind[KS, UngroupedColumn[?]],
        nu: hu.R =:= false,
        oks: ExcludeCurrentLevelColumn[KS, L],
        e: oks.R =:= EmptyTuple
    ): CanInRecognizeDefine[KS, L]()

trait TakeSize[T]:
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