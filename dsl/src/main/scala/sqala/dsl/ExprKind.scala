package sqala.dsl

import scala.NamedTuple.NamedTuple
import scala.annotation.{implicitNotFound, nowarn}
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

type CastKind[E <: Expr[?, ?]] <: CompositeKind = E match
    case Expr[_, k] => ResultKind[k, ValueKind]

trait TransformKind[T, TK <: ExprKind]:
    type R

    def tansform(x: T): R

object TransformKind:
    @nowarn("msg=New anonymous class definition will be duplicated at each inline site")
    transparent inline given transformExpr[T, K <: ExprKind, TK <: ExprKind]: TransformKind[Expr[T, K], TK] =
        new TransformKind[Expr[T, K], TK]:
            type R = Expr[T, TK]

            def tansform(x: Expr[T, K]): R = x.asInstanceOf[R]

    @nowarn("msg=New anonymous class definition will be duplicated at each inline site")
    transparent inline given transformTuple[H, T <: Tuple, TK <: ExprKind](using h: TransformKind[H, TK], t: TransformKind[T, TK]): TransformKind[H *: T, TK] =
        new TransformKind[H *: T, TK]:
            type R = h.R *: ToTuple[t.R]

            def tansform(x: H *: T): R =
                val head = h.tansform(x.head)
                val tail = t.tansform(x.tail) match
                    case x: Tuple => x
                (head *: tail).asInstanceOf[R]

    @nowarn("msg=New anonymous class definition will be duplicated at each inline site")
    transparent inline given transformTuple1[H, TK <: ExprKind](using h: TransformKind[H, TK]): TransformKind[H *: EmptyTuple, TK] =
        new TransformKind[H *: EmptyTuple, TK]:
            type R = h.R *: EmptyTuple

            def tansform(x: H *: EmptyTuple): R =
                h.tansform(x.head) *: EmptyTuple

    @nowarn("msg=New anonymous class definition will be duplicated at each inline site")
    transparent inline given transformNamedTuple[N <: Tuple, V <: Tuple, TK <: ExprKind](using t: TransformKind[V, TK]): TransformKind[NamedTuple[N, V], TK] =
        new TransformKind[NamedTuple[N, V], TK]:
            type R = NamedTuple[N, ToTuple[t.R]]

            def tansform(x: NamedTuple[N, V]): R =
                val v = t.tansform(x.toTuple).asInstanceOf[ToTuple[t.R]]
                NamedTuple(v)

trait TransformKindIfNot[T, TK <: ExprKind, NK <: ExprKind]:
    type R

    def tansform(x: T): R

object TransformKindIfNot:
    @nowarn("msg=New anonymous class definition will be duplicated at each inline site")
    transparent inline given transformExpr[T, K <: ExprKind, TK <: ExprKind, NK <: ExprKind]: TransformKindIfNot[Expr[T, K], TK, NK] =
        new TransformKindIfNot[Expr[T, K], TK, NK]:
            type R = Expr[T, TK]

            def tansform(x: Expr[T, K]): R = x.asInstanceOf[R]

    @nowarn("msg=New anonymous class definition will be duplicated at each inline site")
    transparent inline given transformExprSkip[T, K <: ExprKind, TK <: ExprKind]: TransformKindIfNot[Expr[T, K], TK, K] =
        new TransformKindIfNot[Expr[T, K], TK, K]:
            type R = Expr[T, K]

            def tansform(x: Expr[T, K]): R = x

    @nowarn("msg=New anonymous class definition will be duplicated at each inline site")
    transparent inline given transformTuple[H, T <: Tuple, TK <: ExprKind, NK <: ExprKind](using h: TransformKindIfNot[H, TK, NK], t: TransformKindIfNot[T, TK, NK]): TransformKindIfNot[H *: T, TK, NK] =
        new TransformKindIfNot[H *: T, TK, NK]:
            type R = h.R *: ToTuple[t.R]

            def tansform(x: H *: T): R =
                val head = h.tansform(x.head)
                val tail = t.tansform(x.tail) match
                    case x: Tuple => x
                (head *: tail).asInstanceOf[R]

    @nowarn("msg=New anonymous class definition will be duplicated at each inline site")
    transparent inline given transformTuple1[H, TK <: ExprKind, NK <: ExprKind](using h: TransformKindIfNot[H, TK, NK]): TransformKindIfNot[H *: EmptyTuple, TK, NK] =
        new TransformKindIfNot[H *: EmptyTuple, TK, NK]:
            type R = h.R *: EmptyTuple

            def tansform(x: H *: EmptyTuple): R =
                h.tansform(x.head) *: EmptyTuple

    @nowarn("msg=New anonymous class definition will be duplicated at each inline site")
    transparent inline given transformNamedTuple[N <: Tuple, V <: Tuple, TK <: ExprKind, NK <: ExprKind](using t: TransformKindIfNot[V, TK, NK]): TransformKindIfNot[NamedTuple[N, V], TK, NK] =
        new TransformKindIfNot[NamedTuple[N, V], TK, NK]:
            type R = NamedTuple[N, ToTuple[t.R]]

            def tansform(x: NamedTuple[N, V]): R =
                val v = t.tansform(x.toTuple).asInstanceOf[ToTuple[t.R]]
                NamedTuple(v)

trait HasAgg[T]:
    type R <: Boolean

object HasAgg:
    @nowarn("msg=New anonymous class definition will be duplicated at each inline site")
    transparent inline given notAgg[T, K <: CommonKind | ColumnKind | WindowKind]: HasAgg[Expr[T, K]] =
        new HasAgg[Expr[T, K]]:
            type R = false

    @nowarn("msg=New anonymous class definition will be duplicated at each inline site")
    transparent inline given hasAgg[T, K <: AggKind | AggOperationKind | ValueKind]: HasAgg[Expr[T, K]] =
        new HasAgg[Expr[T, K]]:
            type R = true

    @nowarn("msg=New anonymous class definition will be duplicated at each inline site")
    transparent inline given tupleHasAgg[H, T <: Tuple](using ch: HasAgg[H], ct: HasAgg[T]): HasAgg[H *: T] =
        new HasAgg[H *: T]:
            type R = ch.R && ct.R

    @nowarn("msg=New anonymous class definition will be duplicated at each inline site")
    transparent inline given tuple1HasAgg[H](using ch: HasAgg[H]): HasAgg[H *: EmptyTuple] =
        new HasAgg[H *: EmptyTuple]:
            type R = ch.R

    @nowarn("msg=New anonymous class definition will be duplicated at each inline site")
    transparent inline given namedTupleHasAgg[N <: Tuple, V <: Tuple](using h: HasAgg[V]): HasAgg[NamedTuple[N, V]] =
        new HasAgg[NamedTuple[N, V]]:
            type R = h.R

trait IsAggOrGroup[T]:
    type R <: Boolean

object IsAggOrGroup:
    @nowarn("msg=New anonymous class definition will be duplicated at each inline site")
    transparent inline given notAgg[T, K <: CommonKind | ColumnKind | WindowKind]: IsAggOrGroup[Expr[T, K]] =
        new IsAggOrGroup[Expr[T, K]]:
            type R = false

    @nowarn("msg=New anonymous class definition will be duplicated at each inline site")
    transparent inline given hasAgg[T, K <: AggKind | AggOperationKind | ValueKind | GroupKind]: IsAggOrGroup[Expr[T, K]] =
        new IsAggOrGroup[Expr[T, K]]:
            type R = true

    @nowarn("msg=New anonymous class definition will be duplicated at each inline site")
    transparent inline given tupleHasAgg[H, T <: Tuple](using ch: IsAggOrGroup[H], ct: IsAggOrGroup[T]): IsAggOrGroup[H *: T] =
        new IsAggOrGroup[H *: T]:
            type R = ch.R && ct.R

    @nowarn("msg=New anonymous class definition will be duplicated at each inline site")
    transparent inline given tuple1HasAgg[H](using ch: IsAggOrGroup[H]): IsAggOrGroup[H *: EmptyTuple] =
        new IsAggOrGroup[H *: EmptyTuple]:
            type R = ch.R

    @nowarn("msg=New anonymous class definition will be duplicated at each inline site")
    transparent inline given namedTupleHasAgg[N <: Tuple, V <: Tuple](using i: IsAggOrGroup[V]): IsAggOrGroup[NamedTuple[N, V]] =
        new IsAggOrGroup[NamedTuple[N, V]]:
            type R = i.R

trait NotAgg[T]:
    type R <: Boolean

object NotAgg:
    @nowarn("msg=New anonymous class definition will be duplicated at each inline site")
    transparent inline given notAgg[T, K <: CommonKind | ColumnKind | WindowKind | ValueKind]: NotAgg[Expr[T, K]] =
        new NotAgg[Expr[T, K]]:
            type R = true

    @nowarn("msg=New anonymous class definition will be duplicated at each inline site")
    transparent inline given hasAgg[T, K <: AggKind | AggOperationKind]: NotAgg[Expr[T, K]] =
        new NotAgg[Expr[T, K]]:
            type R = false

    @nowarn("msg=New anonymous class definition will be duplicated at each inline site")
    transparent inline given tupleNotAgg[H, T <: Tuple](using ch: NotAgg[H], ct: NotAgg[T]): NotAgg[H *: T] =
        new NotAgg[H *: T]:
            type R = ch.R && ct.R

    @nowarn("msg=New anonymous class definition will be duplicated at each inline site")
    transparent inline given tuple1NotAgg[H](using ch: NotAgg[H]): NotAgg[H *: EmptyTuple] =
        new NotAgg[H *: EmptyTuple]:
            type R = ch.R

    @nowarn("msg=New anonymous class definition will be duplicated at each inline site")
    transparent inline given namedTupleNotAgg[N <: Tuple, V <: Tuple](using n: NotAgg[V]): NotAgg[NamedTuple[N, V]] =
        new NotAgg[NamedTuple[N, V]]:
            type R = n.R

trait NotWindow[T]:
    type R <: Boolean

object NotWindow:
    @nowarn("msg=New anonymous class definition will be duplicated at each inline site")
    transparent inline given notWindow[T, K <: CommonKind | ColumnKind | AggKind | AggOperationKind | ValueKind]: NotWindow[Expr[T, K]] =
        new NotWindow[Expr[T, K]]:
            type R = true

    @nowarn("msg=New anonymous class definition will be duplicated at each inline site")
    transparent inline given hasWindow[T, K <: WindowKind]: NotWindow[Expr[T, K]] =
        new NotWindow[Expr[T, K]]:
            type R = false

    @nowarn("msg=New anonymous class definition will be duplicated at each inline site")
    transparent inline given tupleNotWindow[H, T <: Tuple](using ch: NotWindow[H], ct: NotWindow[T]): NotWindow[H *: T] =
        new NotWindow[H *: T]:
            type R = ch.R && ct.R

    @nowarn("msg=New anonymous class definition will be duplicated at each inline site")
    transparent inline given tuple1NotWindow[H](using ch: NotWindow[H]): NotWindow[H *: EmptyTuple] =
        new NotWindow[H *: EmptyTuple]:
            type R = ch.R

    @nowarn("msg=New anonymous class definition will be duplicated at each inline site")
    transparent inline given namedTupleNotWindow[N <: Tuple, V <: Tuple](using n: NotWindow[V]): NotWindow[NamedTuple[N, V]] =
        new NotWindow[NamedTuple[N, V]]:
            type R = n.R

trait NotValue[T]:
    type R <: Boolean

object NotValue:
    @nowarn("msg=New anonymous class definition will be duplicated at each inline site")
    transparent inline given notValue[T, K <: CommonKind | ColumnKind | AggKind | AggOperationKind | WindowKind]: NotValue[Expr[T, K]] =
        new NotValue[Expr[T, K]]:
            type R = true

    @nowarn("msg=New anonymous class definition will be duplicated at each inline site")
    transparent inline given hasValue[T, K <: ValueKind]: NotValue[Expr[T, K]] =
        new NotValue[Expr[T, K]]:
            type R = false

    @nowarn("msg=New anonymous class definition will be duplicated at each inline site")
    transparent inline given tupleNotValue[H, T <: Tuple](using ch: NotValue[H], ct: NotValue[T]): NotValue[H *: T] =
        new NotValue[H *: T]:
            type R = ch.R && ct.R

    @nowarn("msg=New anonymous class definition will be duplicated at each inline site")
    transparent inline given tuple1NotValue[H](using ch: NotValue[H]): NotValue[H *: EmptyTuple] =
        new NotValue[H *: EmptyTuple]:
            type R = ch.R

    @nowarn("msg=New anonymous class definition will be duplicated at each inline site")
    transparent inline given namedTupleNotValue[N <: Tuple, V <: Tuple](using n: NotValue[V]): NotValue[NamedTuple[N, V]] =
        new NotValue[NamedTuple[N, V]]:
            type R = n.R

@implicitNotFound("Column must appear in the GROUP BY clause or be used in an aggregate function")
trait CheckMapKind[IsAgg <: Boolean, NotAgg <: Boolean]

object CheckMapKind:
    given checkTrueTrue: CheckMapKind[true, true]()

    given checkTrueFalse: CheckMapKind[true, false]()

    given checkFalseTrue: CheckMapKind[false, true]()

@implicitNotFound("Column must appear in the GROUP BY clause or be used in an aggregate function")
trait CheckGroupMapKind[IsAggOrGroup <: Boolean]

object CheckGroupMapKind:
    given check: CheckGroupMapKind[true]()

@implicitNotFound("Aggregate functions or window functions or constants are not allowed in GROUP BY")
trait CheckGroupByKind[NotAgg <: Boolean, NotWindow <: Boolean, NotValue <: Boolean]

object CheckGroupByKind:
    given check: CheckGroupByKind[true, true, true]()