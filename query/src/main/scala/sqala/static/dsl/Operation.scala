package sqala.static.dsl

import sqala.common.*

import java.time.LocalDateTime
import scala.NamedTuple.NamedTuple
import scala.annotation.implicitNotFound
import scala.compiletime.{erasedValue, summonInline}
import scala.compiletime.ops.boolean.*
import scala.deriving.Mirror
import scala.util.NotGiven

@implicitNotFound("Types ${A} and ${B} be cannot compared.")
trait Compare[A, B]

object Compare:
    given idCompare[A: AsSqlExpr]: Compare[A, A]()

    given nothingCompare: Compare[Nothing, Nothing]()

    given valueAndNothingCompare[A](using
        NotGiven[A =:= Nothing]
    ): Compare[A, Nothing]()

    given nothingAndValueCompare[A](using
        NotGiven[A =:= Nothing]
    ): Compare[Nothing, A]()

    given numericCompare[A: Number, B: Number](using
        NotGiven[A =:= B]
    ): Compare[A, B]()

    given timeCompare[A: DateTime, B: DateTime](using
        NotGiven[A =:= B]
    ): Compare[A, B]()

    given timeAndStringCompare[A: DateTime, B <: String]: Compare[A, B]()

    given stringAndTimeCompare[A <: String, B: DateTime]: Compare[A, B]()

    given tupleCompare[LH, LT <: Tuple, RH, RT <: Tuple](using
        Compare[Unwrap[LH, Option], Unwrap[RH, Option]],
        Compare[LT, RT]
    ): Compare[LH *: LT, RH *: RT]()

    given tuple1Compare[LH, RH](using
        Compare[LH, RH]
    ): Compare[LH *: EmptyTuple, RH *: EmptyTuple]()

@implicitNotFound("Types ${A} and ${B} cannot be subtract.")
trait Minus[A, B, Nullable <: Boolean]:
    type R

object Minus:
    type Aux[A, B, N <: Boolean, O] = Minus[A, B, N]:
        type R = O

    given leftNothingResult[A: Numeric, N <: Boolean](using NotGiven[A =:= Nothing]): Aux[Nothing, A, N, Option[A]] =
        new Minus[Nothing, A, N]:
            type R = Option[A]

    given rightNothingResult[A: Numeric, N <: Boolean](using NotGiven[A =:= Nothing]): Aux[A, Nothing, N, Option[A]] =
        new Minus[A, Nothing, N]:
            type R = Option[A]

    given numberMinusNumber[A: Number, B: Number, N <: Boolean]: Aux[A, B, N, NumericResult[A, B, N]] =
        new Minus[A, B, N]:
            type R = NumericResult[A, B, N]

    given timeMinusTime[A: DateTime, B: DateTime, N <: Boolean]: Aux[A, B, N, Interval] =
        new Minus[A, B, N]:
            type R = Interval

@implicitNotFound("Types ${A} and ${B} cannot be returned as results.")
trait Return[A, B, Nullable <: Boolean]:
    type R

object Return:
    type Aux[A, B, N <: Boolean, O] = Return[A, B, N]:
        type R = O

    given result[A: AsSqlExpr]: Aux[A, A, false, A] =
        new Return[A, A, false]:
            type R = A

    given optionResult[A: AsSqlExpr](using NotGiven[A =:= Nothing]): Aux[A, A, true, Option[A]] =
        new Return[A, A, true]:
            type R = Option[A]

    given leftNothingResult[A, N <: Boolean](using NotGiven[A =:= Nothing]): Aux[Nothing, A, N, Option[A]] =
        new Return[Nothing, A, N]:
            type R = Option[A]

    given rightNothingResult[A, N <: Boolean](using NotGiven[A =:= Nothing]): Aux[A, Nothing, N, Option[A]] =
        new Return[A, Nothing, N]:
            type R = Option[A]

    given numericResult[A: Number, B: Number, N <: Boolean](using
        NotGiven[A =:= B]
    ): Aux[A, B, N, NumericResult[A, B, N]] =
        new Return[A, B, N]:
            type R = NumericResult[A, B, N]

    given timeResult[A: DateTime, B: DateTime](using
        NotGiven[A =:= B]
    ): Aux[A, B, false, LocalDateTime] =
        new Return[A, B, false]:
            type R = LocalDateTime

    given timeOptionResult[A: DateTime, B: DateTime](using
        NotGiven[A =:= B]
    ): Aux[A, B, true, Option[LocalDateTime]] =
        new Return[A, B, true]:
            type R = Option[LocalDateTime]

    given timeAndStringResult[A: DateTime]: Aux[A, String, false, LocalDateTime] =
        new Return[A, String, false]:
            type R = LocalDateTime

    given timeAndStringOptionResult[A: DateTime]: Aux[A, String, true, Option[LocalDateTime]] =
        new Return[A, String, true]:
            type R = Option[LocalDateTime]

    given stringAndTimeResult[A: DateTime]: Aux[String, A, false, LocalDateTime] =
        new Return[String, A, false]:
            type R = LocalDateTime

    given stringAndTimeOptionResult[A: DateTime]: Aux[String, A, true, Option[LocalDateTime]] =
        new Return[String, A, true]:
            type R = Option[LocalDateTime]

@implicitNotFound("Types ${A} and ${B} cannot be UNION.")
trait Union[A, B]:
    type R

    def unionQueryItems(x: A): R

object Union:
    type Aux[A, B, O] = Union[A, B]:
        type R = O

    given union[A, B](using
        r: Return[Unwrap[A, Option], Unwrap[B, Option], IsOption[A] || IsOption[B]]
    ): Aux[Expr[A], Expr[B], Expr[r.R]] =
        new Union[Expr[A], Expr[B]]:
            type R = Expr[r.R]

            def unionQueryItems(x: Expr[A]): R =
                Expr.Ref(x.asSqlExpr)

    given tableUnion[A, B](using
        ma: Mirror.ProductOf[A],
        mb: Mirror.ProductOf[B],
        refl: ma.MirroredElemTypes =:= mb.MirroredElemTypes
    ): Aux[Table[A], Table[B], Table[A]] =
        new Union[Table[A], Table[B]]:
            type R = Table[A]

            def unionQueryItems(x: Table[A]): R = x

    given tupleUnion[LH, LT <: Tuple, RH, RT <: Tuple](using
        h: Union[LH, RH],
        t: Union[LT, RT],
        tt: ToTuple[t.R]
    ): Aux[LH *: LT, RH *: RT, h.R *: tt.R] =
        new Union[LH *: LT, RH *: RT]:
            type R = h.R *: tt.R

            def unionQueryItems(x: LH *: LT): R =
                h.unionQueryItems(x.head) *: tt.toTuple(t.unionQueryItems(x.tail))

    given tuple1Union[LH, RH](using
        h: Union[LH, RH]
    ): Aux[LH *: EmptyTuple, RH *: EmptyTuple, h.R *: EmptyTuple] =
        new Union[LH *: EmptyTuple, RH *: EmptyTuple]:
            type R = h.R *: EmptyTuple

            def unionQueryItems(x: LH *: EmptyTuple): R =
                h.unionQueryItems(x.head) *: EmptyTuple

    given namedTupleUnion[LN <: Tuple, LV <: Tuple, RN <: Tuple, RV <: Tuple](using
        u: Union[LV, RV],
        t: ToTuple[u.R]
    ): Aux[NamedTuple[LN, LV], NamedTuple[RN, RV], NamedTuple[LN, t.R]] =
        new Union[NamedTuple[LN, LV], NamedTuple[RN, RV]]:
            type R = NamedTuple[LN, t.R]

            def unionQueryItems(x: NamedTuple[LN, LV]): R =
                NamedTuple(t.toTuple(u.unionQueryItems(x.toTuple)))

    given namedTupleUnionTuple[LN <: Tuple, LV <: Tuple, RV <: Tuple](using
        u: Union[LV, RV],
        t: ToTuple[u.R]
    ): Aux[NamedTuple[LN, LV], RV, NamedTuple[LN, t.R]] =
        new Union[NamedTuple[LN, LV], RV]:
            type R = NamedTuple[LN, t.R]

            def unionQueryItems(x: NamedTuple[LN, LV]): R =
                NamedTuple(t.toTuple(u.unionQueryItems(x.toTuple)))