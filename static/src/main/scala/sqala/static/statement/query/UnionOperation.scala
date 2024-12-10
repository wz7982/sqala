package sqala.static.statement.query

import sqala.static.common.*

import java.time.LocalDateTime
import scala.NamedTuple.NamedTuple
import scala.annotation.implicitNotFound
import scala.deriving.Mirror
import scala.util.NotGiven

@implicitNotFound("Types ${A} and ${B} cannot be returned as results.")
trait ResultOperation[A, B, Nullable <: Boolean]:
    type R

object ResultOperation:
    type Aux[A, B, N <: Boolean, O] = ResultOperation[A, B, N]:
        type R = O

    given result[A: AsSqlExpr]: Aux[A, A, false, A] =
        new ResultOperation[A, A, false]:
            type R = A

    given optionResult[A: AsSqlExpr](using NotGiven[A =:= Nothing]): Aux[A, A, true, Option[A]] =
        new ResultOperation[A, A, true]:
            type R = Option[A]

    given numericResult[A: Number, B: Number, N <: Boolean](using 
        NotGiven[A =:= B]
    ): Aux[A, B, N, NumericResult[A, B, N]] =
        new ResultOperation[A, B, N]:
            type R = NumericResult[A, B, N]

    given timeResult[A: DateTime, B: DateTime](using
        NotGiven[A =:= B]
    ): Aux[A, B, false, LocalDateTime] =
        new ResultOperation[A, B, false]:
            type R = LocalDateTime

    given optionTimeResult[A: DateTime, B: DateTime](using
        NotGiven[A =:= B]
    ): Aux[A, B, true, Option[LocalDateTime]] =
        new ResultOperation[A, B, true]:
            type R = Option[LocalDateTime]

@implicitNotFound("Types ${A} and ${B} cannot be UNION.")
trait UnionOperation[A, B]:
    type R

object UnionOperation:
    type Aux[A, B, O] = UnionOperation[A, B]:
        type R = O

    given exprUnion[A: AsSqlExpr, B: AsSqlExpr](using
        r: ResultOperation[Unwrap[A, Option], Unwrap[B, Option], HasOption[A, B]]
    ): Aux[A, B, r.R] =
        new UnionOperation[A, B]:
            type R = r.R

    given tableUnion[A, B](using
        a: Mirror.ProductOf[A],
        b: Mirror.ProductOf[B],
        refl: a.MirroredElemTypes =:= b.MirroredElemTypes
    ): Aux[Table[A], Table[B], Table[A]] =
        new UnionOperation[Table[A], Table[B]]:
            type R = Table[A]

    given tupleUnion[LH, LT <: Tuple, RH, RT <: Tuple](using
        h: UnionOperation[LH, RH],
        t: UnionOperation[LT, RT],
        tt: ToTuple[t.R]
    ): Aux[LH *: LT, RH *: RT, h.R *: tt.R] =
        new UnionOperation[LH *: LT, RH *: RT]:
            type R = h.R *: tt.R

    given tuple1Union[LH, RH](using
        h: UnionOperation[LH, RH]
    ): Aux[LH *: EmptyTuple, RH *: EmptyTuple, h.R *: EmptyTuple] =
        new UnionOperation[LH *: EmptyTuple, RH *: EmptyTuple]:
            type R = h.R *: EmptyTuple

    given namedTupleUnion[LN <: Tuple, LV <: Tuple, RN <: Tuple, RV <: Tuple](using
        v: UnionOperation[LV, RV],
        t: ToTuple[v.R]
    ): Aux[NamedTuple[LN, LV], NamedTuple[RN, RV], NamedTuple[LN, t.R]] =
        new UnionOperation[NamedTuple[LN, LV], NamedTuple[RN, RV]]:
            type R = NamedTuple[LN, t.R]

    given namedTupleUnionTuple[LN <: Tuple, LV <: Tuple, R <: Tuple](using
        v: UnionOperation[LV, R],
        t: ToTuple[v.R]
    ): Aux[NamedTuple[LN, LV], R, NamedTuple[LN, t.R]] =
        new UnionOperation[NamedTuple[LN, LV], R]:
            type R = NamedTuple[LN, t.R]