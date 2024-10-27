package sqala.dsl

import scala.NamedTuple.NamedTuple
import scala.annotation.implicitNotFound
import scala.compiletime.ops.boolean.&&

sealed trait ExprKind

case class ValueKind() extends ExprKind

case class CommonKind() extends ExprKind

case class ColumnKind() extends ExprKind

case class AggKind() extends ExprKind

case class AggOperationKind() extends ExprKind

case class WindowKind() extends ExprKind

case class DistinctKind() extends ExprKind

case class GroupKind() extends ExprKind

type SimpleKind = ColumnKind | CommonKind | ValueKind

type CompositeKind = CommonKind | AggOperationKind | WindowKind

type SortKind = ColumnKind | CommonKind | WindowKind

type FuncKind = CommonKind | AggKind | AggOperationKind | WindowKind

type ResultKind[L <: ExprKind, R <: ExprKind] <: CompositeKind = (L, R) match
    case (WindowKind, r) => WindowKind
    case (l, WindowKind) => WindowKind
    case (AggKind | AggOperationKind | GroupKind, r) => AggOperationKind
    case (l, AggKind | AggOperationKind | GroupKind) => AggOperationKind
    case (l, r) => CommonKind

trait TransformKind[T, TK <: ExprKind]:
    type R

    def tansform(x: T): R

object TransformKind:
    type Aux[T, TK <: ExprKind, O] = TransformKind[T, TK]:
        type R = O

    given transformExpr[T, K <: ExprKind, TK <: ExprKind]: Aux[Expr[T, K], TK, Expr[T, TK]] =
        new TransformKind[Expr[T, K], TK]:
            type R = Expr[T, TK]

            def tansform(x: Expr[T, K]): R = x.asInstanceOf[R]

    given transformTuple[H, T <: Tuple, TK <: ExprKind](using h: TransformKind[H, TK], t: TransformKind[T, TK], tt: ToTuple[t.R]): Aux[H *: T, TK, h.R *: tt.R] =
        new TransformKind[H *: T, TK]:
            type R = h.R *: tt.R

            def tansform(x: H *: T): R =
                h.tansform(x.head) *: tt.toTuple(t.tansform(x.tail))

    given transformTuple1[H, TK <: ExprKind](using h: TransformKind[H, TK]): Aux[H *: EmptyTuple, TK, h.R *: EmptyTuple] =
        new TransformKind[H *: EmptyTuple, TK]:
            type R = h.R *: EmptyTuple

            def tansform(x: H *: EmptyTuple): R =
                h.tansform(x.head) *: EmptyTuple

    given transformNamedTuple[N <: Tuple, V <: Tuple, TK <: ExprKind](using t: TransformKind[V, TK], tt: ToTuple[t.R]): Aux[NamedTuple[N, V], TK, NamedTuple[N, tt.R]] =
        new TransformKind[NamedTuple[N, V], TK]:
            type R = NamedTuple[N, tt.R]

            def tansform(x: NamedTuple[N, V]): R =
                NamedTuple(tt.toTuple(t.tansform(x.toTuple)))

trait TransformKindIfNot[T, TK <: ExprKind, NK <: ExprKind]:
    type R

    def tansform(x: T): R

object TransformKindIfNot:
    type Aux[T, TK <: ExprKind, NK <: ExprKind, O] = TransformKindIfNot[T, TK, NK]:
        type R = O

    given transformExpr[T, K <: ExprKind, TK <: ExprKind, NK <: ExprKind]: Aux[Expr[T, K], TK, NK, Expr[T, TK]] =
        new TransformKindIfNot[Expr[T, K], TK, NK]:
            type R = Expr[T, TK]

            def tansform(x: Expr[T, K]): R = x.asInstanceOf[R]

    given transformExprSkip[T, K <: ExprKind, TK <: ExprKind]: Aux[Expr[T, K], TK, K, Expr[T, K]] =
        new TransformKindIfNot[Expr[T, K], TK, K]:
            type R = Expr[T, K]

            def tansform(x: Expr[T, K]): R = x

    given transformTuple[H, T <: Tuple, TK <: ExprKind, NK <: ExprKind](using h: TransformKindIfNot[H, TK, NK], t: TransformKindIfNot[T, TK, NK], tt: ToTuple[t.R]): Aux[H *: T, TK, NK, h.R *: tt.R] =
        new TransformKindIfNot[H *: T, TK, NK]:
            type R = h.R *: tt.R

            def tansform(x: H *: T): R =
                h.tansform(x.head) *: tt.toTuple(t.tansform(x.tail))

    given transformTuple1[H, TK <: ExprKind, NK <: ExprKind](using h: TransformKindIfNot[H, TK, NK]): Aux[H *: EmptyTuple, TK, NK, h.R *: EmptyTuple] =
        new TransformKindIfNot[H *: EmptyTuple, TK, NK]:
            type R = h.R *: EmptyTuple

            def tansform(x: H *: EmptyTuple): R =
                h.tansform(x.head) *: EmptyTuple

    given transformNamedTuple[N <: Tuple, V <: Tuple, TK <: ExprKind, NK <: ExprKind](using t: TransformKindIfNot[V, TK, NK], tt: ToTuple[t.R]): Aux[NamedTuple[N, V], TK, NK, NamedTuple[N, tt.R]] =
        new TransformKindIfNot[NamedTuple[N, V], TK, NK]:
            type R = NamedTuple[N, tt.R]

            def tansform(x: NamedTuple[N, V]): R =
                NamedTuple(tt.toTuple(t.tansform(x.toTuple)))

trait HasAgg[T]:
    type R <: Boolean

object HasAgg:
    type Aux[T, O <: Boolean] = HasAgg[T]:
        type R = O

    given notAgg[T, K <: CommonKind | ColumnKind | WindowKind]: Aux[Expr[T, K], false] =
        new HasAgg[Expr[T, K]]:
            type R = false

    given hasAgg[T, K <: AggKind | AggOperationKind | ValueKind]: Aux[Expr[T, K], true] =
        new HasAgg[Expr[T, K]]:
            type R = true

    given tupleHasAgg[H, T <: Tuple](using ch: HasAgg[H], ct: HasAgg[T]): Aux[H *: T, ch.R && ct.R] =
        new HasAgg[H *: T]:
            type R = ch.R && ct.R

    given tuple1HasAgg[H](using ch: HasAgg[H]): Aux[H *: EmptyTuple, ch.R] =
        new HasAgg[H *: EmptyTuple]:
            type R = ch.R

    given namedTupleHasAgg[N <: Tuple, V <: Tuple](using h: HasAgg[V]): Aux[NamedTuple[N, V], h.R] =
        new HasAgg[NamedTuple[N, V]]:
            type R = h.R

trait IsAggOrGroup[T]:
    type R <: Boolean

object IsAggOrGroup:
    type Aux[T, O <: Boolean] = IsAggOrGroup[T]:
        type R = O

    given notAgg[T, K <: CommonKind | ColumnKind | WindowKind]: Aux[Expr[T, K], false] =
        new IsAggOrGroup[Expr[T, K]]:
            type R = false

    given hasAgg[T, K <: AggKind | AggOperationKind | ValueKind | GroupKind]: Aux[Expr[T, K], true] =
        new IsAggOrGroup[Expr[T, K]]:
            type R = true

    given tupleHasAgg[H, T <: Tuple](using ch: IsAggOrGroup[H], ct: IsAggOrGroup[T]): Aux[H *: T, ch.R && ct.R] =
        new IsAggOrGroup[H *: T]:
            type R = ch.R && ct.R

    given tuple1HasAgg[H](using ch: IsAggOrGroup[H]): Aux[H *: EmptyTuple, ch.R] =
        new IsAggOrGroup[H *: EmptyTuple]:
            type R = ch.R

    given namedTupleHasAgg[N <: Tuple, V <: Tuple](using i: IsAggOrGroup[V]): Aux[NamedTuple[N, V], i.R] =
        new IsAggOrGroup[NamedTuple[N, V]]:
            type R = i.R

trait NotAgg[T]:
    type R <: Boolean

object NotAgg:
    type Aux[T, O <: Boolean] = NotAgg[T]:
        type R = O

    given notAgg[T, K <: CommonKind | ColumnKind | WindowKind | ValueKind]: Aux[Expr[T, K], true] =
        new NotAgg[Expr[T, K]]:
            type R = true

    given hasAgg[T, K <: AggKind | AggOperationKind]: Aux[Expr[T, K], false] =
        new NotAgg[Expr[T, K]]:
            type R = false

    given tupleNotAgg[H, T <: Tuple](using nh: NotAgg[H], nt: NotAgg[T]): Aux[H *: T, nh.R && nt.R] =
        new NotAgg[H *: T]:
            type R = nh.R && nt.R

    given tuple1NotAgg[H](using nh: NotAgg[H]): Aux[H *: EmptyTuple, nh.R] =
        new NotAgg[H *: EmptyTuple]:
            type R = nh.R

    given namedTupleNotAgg[N <: Tuple, V <: Tuple](using n: NotAgg[V]): Aux[NamedTuple[N, V], n.R] =
        new NotAgg[NamedTuple[N, V]]:
            type R = n.R

trait NotWindow[T]:
    type R <: Boolean

object NotWindow:
    type Aux[T, O <: Boolean] = NotWindow[T]:
        type R = O

    given notWindow[T, K <: CommonKind | ColumnKind | AggKind | AggOperationKind | ValueKind]: Aux[Expr[T, K], true] =
        new NotWindow[Expr[T, K]]:
            type R = true

    given hasWindow[T, K <: WindowKind]: Aux[Expr[T, K], false] =
        new NotWindow[Expr[T, K]]:
            type R = false

    given tupleNotWindow[H, T <: Tuple](using nh: NotWindow[H], nt: NotWindow[T]): Aux[H *: T, nh.R && nt.R] =
        new NotWindow[H *: T]:
            type R = nh.R && nt.R

    given tuple1NotWindow[H](using nh: NotWindow[H]): Aux[H *: EmptyTuple, nh.R] =
        new NotWindow[H *: EmptyTuple]:
            type R = nh.R

    given namedTupleNotWindow[N <: Tuple, V <: Tuple](using n: NotWindow[V]): Aux[NamedTuple[N, V], n.R] =
        new NotWindow[NamedTuple[N, V]]:
            type R = n.R

trait NotValue[T]:
    type R <: Boolean

object NotValue:
    type Aux[T, O <: Boolean] = NotValue[T]:
        type R = O

    given notValue[T, K <: CommonKind | ColumnKind | AggKind | AggOperationKind | WindowKind]: Aux[Expr[T, K], true] =
        new NotValue[Expr[T, K]]:
            type R = true

    given hasValue[T, K <: ValueKind]: Aux[Expr[T, K], false] =
        new NotValue[Expr[T, K]]:
            type R = false

    given tupleNotValue[H, T <: Tuple](using nh: NotValue[H], nt: NotValue[T]): Aux[H *: T, nh.R && nt.R] =
        new NotValue[H *: T]:
            type R = nh.R && nt.R

    given tuple1NotValue[H](using nh: NotValue[H]): Aux[H *: EmptyTuple, nh.R] =
        new NotValue[H *: EmptyTuple]:
            type R = nh.R

    given namedTupleNotValue[N <: Tuple, V <: Tuple](using n: NotValue[V]): Aux[NamedTuple[N, V], n.R] =
        new NotValue[NamedTuple[N, V]]:
            type R = n.R

@implicitNotFound("Column must appear in the GROUP BY clause or be used in an aggregate function.")
trait CheckMapKind[IsAgg <: Boolean, NotAgg <: Boolean]

object CheckMapKind:
    given checkTrueTrue: CheckMapKind[true, true]()

    given checkTrueFalse: CheckMapKind[true, false]()

    given checkFalseTrue: CheckMapKind[false, true]()

@implicitNotFound("Column must appear in the GROUP BY clause or be used in an aggregate function.")
trait CheckGroupMapKind[IsAggOrGroup <: Boolean]

object CheckGroupMapKind:
    given check: CheckGroupMapKind[true]()

@implicitNotFound("Aggregate functions or window functions or constants are not allowed in GROUP BY.")
trait CheckGroupByKind[NotAgg <: Boolean, NotWindow <: Boolean, NotValue <: Boolean]

object CheckGroupByKind:
    given check: CheckGroupByKind[true, true, true]()