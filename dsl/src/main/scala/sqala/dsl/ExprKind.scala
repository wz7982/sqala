package sqala.dsl

import scala.compiletime.ops.boolean.&&
import scala.annotation.implicitNotFound

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
    transparent inline given transformExpr[T, K <: ExprKind, TK <: ExprKind]: TransformKind[Expr[T, K], TK] =
        new TransformKind[Expr[T, K], TK]:
            type R = Expr[T, TK]

            def tansform(x: Expr[T, K]): R = x.asInstanceOf[R]

    transparent inline given transformTuple[H, T <: Tuple, TK <: ExprKind](using h: TransformKind[H, TK], t: TransformKind[T, TK]): TransformKind[H *: T, TK] =
        new TransformKind[H *: T, TK]:
            type R = h.R *: ToTuple[t.R]

            def tansform(x: H *: T): R =
                val head = h.tansform(x.head)
                val tail = t.tansform(x.tail) match
                    case x: Tuple => x
                (head *: tail).asInstanceOf[R]

    transparent inline given transformEmptyTuple[TK <: ExprKind, NK <: ExprKind]: TransformKind[EmptyTuple, TK] =
        new TransformKind[EmptyTuple, TK]:
            type R = EmptyTuple

            def tansform(x: EmptyTuple): R = x

trait TransformKindIfNot[T, TK <: ExprKind, NK <: ExprKind]:
    type R

    def tansform(x: T): R

object TransformKindIfNot:
    transparent inline given transformExpr[T, K <: ExprKind, TK <: ExprKind, NK <: ExprKind]: TransformKindIfNot[Expr[T, K], TK, NK] =
        new TransformKindIfNot[Expr[T, K], TK, NK]:
            type R = Expr[T, TK]

            def tansform(x: Expr[T, K]): R = x.asInstanceOf[R]

    transparent inline given transformExprSkip[T, K <: ExprKind, TK <: ExprKind]: TransformKindIfNot[Expr[T, K], TK, K] =
        new TransformKindIfNot[Expr[T, K], TK, K]:
            type R = Expr[T, K]

            def tansform(x: Expr[T, K]): R = x

    transparent inline given transformTuple[H, T <: Tuple, TK <: ExprKind, NK <: ExprKind](using h: TransformKindIfNot[H, TK, NK], t: TransformKindIfNot[T, TK, NK]): TransformKindIfNot[H *: T, TK, NK] =
        new TransformKindIfNot[H *: T, TK, NK]:
            type R = h.R *: ToTuple[t.R]

            def tansform(x: H *: T): R =
                val head = h.tansform(x.head)
                val tail = t.tansform(x.tail) match
                    case x: Tuple => x
                (head *: tail).asInstanceOf[R]

    transparent inline given transformEmptyTuple[TK <: ExprKind, NK <: ExprKind]: TransformKindIfNot[EmptyTuple, TK, NK] =
        new TransformKindIfNot[EmptyTuple, TK, NK]:
            type R = EmptyTuple

            def tansform(x: EmptyTuple): R = x

trait HasAgg[T]:
    type R <: Boolean

object HasAgg:
    transparent inline given notAgg[T, K <: CommonKind | ColumnKind | WindowKind]: HasAgg[Expr[T, K]] =
        new HasAgg[Expr[T, K]]:
            type R = false

    transparent inline given hasAgg[T, K <: AggKind | AggOperationKind | ValueKind]: HasAgg[Expr[T, K]] =
        new HasAgg[Expr[T, K]]:
            type R = true

    transparent inline given tupleHasAgg[H, T <: Tuple](using ch: HasAgg[H], ct: HasAgg[T]): HasAgg[H *: T] =
        new HasAgg[H *: T]:
            type R = ch.R && ct.R

    transparent inline given emptyTupleHasAgg: HasAgg[EmptyTuple] =
        new HasAgg[EmptyTuple]:
            type R = true

trait IsAggOrGroup[T]:
    type R <: Boolean

object IsAggOrGroup:
    transparent inline given notAgg[T, K <: CommonKind | ColumnKind | WindowKind]: IsAggOrGroup[Expr[T, K]] =
        new IsAggOrGroup[Expr[T, K]]:
            type R = false

    transparent inline given hasAgg[T, K <: AggKind | AggOperationKind | ValueKind | GroupKind]: IsAggOrGroup[Expr[T, K]] =
        new IsAggOrGroup[Expr[T, K]]:
            type R = true

    transparent inline given tupleHasAgg[H, T <: Tuple](using ch: IsAggOrGroup[H], ct: IsAggOrGroup[T]): IsAggOrGroup[H *: T] =
        new IsAggOrGroup[H *: T]:
            type R = ch.R && ct.R

    transparent inline given emptyTupleHasAgg: IsAggOrGroup[EmptyTuple] =
        new IsAggOrGroup[EmptyTuple]:
            type R = true

trait NotAgg[T]:
    type R <: Boolean

object NotAgg:
    transparent inline given notAgg[T, K <: CommonKind | ColumnKind | WindowKind | ValueKind]: NotAgg[Expr[T, K]] =
        new NotAgg[Expr[T, K]]:
            type R = true

    transparent inline given hasAgg[T, K <: AggKind | AggOperationKind]: NotAgg[Expr[T, K]] =
        new NotAgg[Expr[T, K]]:
            type R = false

    transparent inline given tupleNotAgg[H, T <: Tuple](using ch: NotAgg[H], ct: NotAgg[T]): NotAgg[H *: T] =
        new NotAgg[H *: T]:
            type R = ch.R && ct.R

    transparent inline given emptyTupleNotAgg: NotAgg[EmptyTuple] =
        new NotAgg[EmptyTuple]:
            type R = true

trait NotWindow[T]:
    type R <: Boolean

object NotWindow:
    transparent inline given notWindow[T, K <: CommonKind | ColumnKind | AggKind | AggOperationKind | ValueKind]: NotWindow[Expr[T, K]] =
        new NotWindow[Expr[T, K]]:
            type R = true

    transparent inline given hasWindow[T, K <: WindowKind]: NotWindow[Expr[T, K]] =
        new NotWindow[Expr[T, K]]:
            type R = false

    transparent inline given tupleNotWindow[H, T <: Tuple](using ch: NotWindow[H], ct: NotWindow[T]): NotWindow[H *: T] =
        new NotWindow[H *: T]:
            type R = ch.R && ct.R

    transparent inline given emptyTupleNotWindow: NotWindow[EmptyTuple] =
        new NotWindow[EmptyTuple]:
            type R = true

trait NotValue[T]:
    type R <: Boolean

object NotValue:
    transparent inline given notValue[T, K <: CommonKind | ColumnKind | AggKind | AggOperationKind | WindowKind]: NotValue[Expr[T, K]] =
        new NotValue[Expr[T, K]]:
            type R = true

    transparent inline given hasValue[T, K <: ValueKind]: NotValue[Expr[T, K]] =
        new NotValue[Expr[T, K]]:
            type R = false

    transparent inline given tupleNotValue[H, T <: Tuple](using ch: NotValue[H], ct: NotValue[T]): NotValue[H *: T] =
        new NotValue[H *: T]:
            type R = ch.R && ct.R

    transparent inline given emptyTupleNotValue: NotValue[EmptyTuple] =
        new NotValue[EmptyTuple]:
            type R = true

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