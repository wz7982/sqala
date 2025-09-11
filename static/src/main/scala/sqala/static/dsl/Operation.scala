package sqala.static.dsl

import sqala.ast.expr.SqlExpr
import sqala.static.metadata.*

import scala.NamedTuple.NamedTuple
import scala.annotation.implicitNotFound
import scala.compiletime.ops.boolean.*
import scala.util.NotGiven

@implicitNotFound("Types ${A} and ${B} be cannot compared.")
trait Compare[A, B]

object Compare:
    given idCompare[A: AsSqlExpr]: Compare[A, A]()

    given numericCompare[A: SqlNumber, B: SqlNumber](using
        NotGiven[A =:= B]
    ): Compare[A, B]()

    given timeCompare[A: SqlDateTime, B: SqlDateTime](using
        NotGiven[A =:= B]
    ): Compare[A, B]()

    given dateTimeAndStringCompare[A: SqlDateTime, B: SqlString]: Compare[A, B]()

    given stringAndDateTimeCompare[A <: String, B: SqlDateTime]: Compare[A, B]()

    given timeAndStringCompare[A: SqlTime, B: SqlString]: Compare[A, B]()

    given stringAndTimeCompare[A <: String, B: SqlTime]: Compare[A, B]()

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

    given numberMinus[A: SqlNumber, B: SqlNumber, N <: Boolean]: Aux[A, B, N, NumericResult[A, B, N]] =
        new Minus[A, B, N]:
            type R = NumericResult[A, B, N]

    given dateTimeMinus[A: SqlDateTime, B: SqlDateTime, N <: Boolean]: Aux[A, B, N, Interval] =
        new Minus[A, B, N]:
            type R = Interval

    given timeMinus[A: SqlTime, B: SqlTime, N <: Boolean]: Aux[A, B, N, Interval] =
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

    given numericResult[A: SqlNumber, B: SqlNumber, N <: Boolean](using
        NotGiven[A =:= B]
    ): Aux[A, B, N, NumericResult[A, B, N]] =
        new Return[A, B, N]:
            type R = NumericResult[A, B, N]

    given dateTimeResult[A: SqlDateTime, B: SqlDateTime](using
        NotGiven[A =:= B]
    ): Aux[A, B, false, DateTimeResult[A, B, false]] =
        new Return[A, B, false]:
            type R = DateTimeResult[A, B, false]

    given dateTimeOptionResult[A: SqlDateTime, B: SqlDateTime](using
        NotGiven[A =:= B]
    ): Aux[A, B, true, DateTimeResult[A, B, true]] =
        new Return[A, B, true]:
            type R = DateTimeResult[A, B, true]

    given timeResult[A: SqlTime, B: SqlTime](using
        NotGiven[A =:= B]
    ): Aux[A, B, false, TimeResult[A, B, false]] =
        new Return[A, B, false]:
            type R = TimeResult[A, B, false]

    given timeOptionResult[A: SqlTime, B: SqlTime](using
        NotGiven[A =:= B]
    ): Aux[A, B, true, TimeResult[A, B, true]] =
        new Return[A, B, true]:
            type R = TimeResult[A, B, true]

    given dateTimeAndStringResult[A: SqlDateTime, B: SqlString]: Aux[A, B, false, DateTimeResult[A, B, false]] =
        new Return[A, B, false]:
            type R = DateTimeResult[A, B, false]

    given dateTimeAndStringOptionResult[A: SqlDateTime, B: SqlString]: Aux[A, B, true, DateTimeResult[A, B, true]] =
        new Return[A, B, true]:
            type R = DateTimeResult[A, B, true]

    given stringAndDateTimeResult[A: SqlString, B: SqlDateTime]: Aux[A, B, false, DateTimeResult[A, B, false]] =
        new Return[A, B, false]:
            type R = DateTimeResult[A, B, false]

    given stringAndDateTimeOptionResult[A: SqlString, B: SqlDateTime]: Aux[A, B, true, DateTimeResult[A, B, true]] =
        new Return[A, B, true]:
            type R = DateTimeResult[A, B, true]

    given timeAndStringResult[A: SqlTime, B: SqlString]: Aux[A, B, false, TimeResult[A, B, false]] =
        new Return[A, B, false]:
            type R = TimeResult[A, B, false]

    given timeAndStringOptionResult[A: SqlTime, B: SqlString]: Aux[A, B, true, TimeResult[A, B, true]] =
        new Return[A, B, true]:
            type R = TimeResult[A, B, true]

    given stringAndTimeResult[A: SqlString, B: SqlTime]: Aux[A, B, false, TimeResult[A, B, false]] =
        new Return[A, B, false]:
            type R = TimeResult[A, B, false]

    given stringAndTimeOptionResult[A: SqlString, B: SqlTime]: Aux[A, B, true, TimeResult[A, B, true]] =
        new Return[A, B, true]:
            type R = TimeResult[A, B, true]

// TODO 支持Table[T]和子查询
@implicitNotFound("Types ${A} and ${B} cannot be UNION.")
trait Union[A, B]:
    type R

    def offset: Int

    def unionQueryItems(x: A, cursor: Int): R

object Union:
    type Aux[A, B, O] = Union[A, B]:
        type R = O
 
    given union[A, B](using
        r: Return[Unwrap[A, Option], Unwrap[B, Option], IsOption[A] || IsOption[B]]
    ): Aux[Expr[A], Expr[B], Expr[r.R]] =
        new Union[Expr[A], Expr[B]]:
            type R = Expr[r.R]

            def offset: Int = 1

            def unionQueryItems(x: Expr[A], cursor: Int): R =
                Expr(SqlExpr.Column(None, s"c$cursor"))

    given tupleUnion[LH, LT <: Tuple, RH, RT <: Tuple](using
        h: Union[LH, RH],
        t: Union[LT, RT],
        tt: ToTuple[t.R]
    ): Aux[LH *: LT, RH *: RT, h.R *: tt.R] =
        new Union[LH *: LT, RH *: RT]:
            type R = h.R *: tt.R

            def offset: Int = h.offset + t.offset

            def unionQueryItems(x: LH *: LT, cursor: Int): R =
                h.unionQueryItems(x.head, cursor) *: 
                    tt.toTuple(t.unionQueryItems(x.tail, cursor + h.offset))

    given tuple1Union[LH, RH](using
        h: Union[LH, RH]
    ): Aux[LH *: EmptyTuple, RH *: EmptyTuple, h.R *: EmptyTuple] =
        new Union[LH *: EmptyTuple, RH *: EmptyTuple]:
            type R = h.R *: EmptyTuple

            def offset: Int = h.offset

            def unionQueryItems(x: LH *: EmptyTuple, cursor: Int): R =
                h.unionQueryItems(x.head, cursor) *: EmptyTuple

    given namedTupleUnion[LN <: Tuple, LV <: Tuple, RN <: Tuple, RV <: Tuple](using
        u: Union[LV, RV],
        t: ToTuple[u.R]
    ): Aux[NamedTuple[LN, LV], NamedTuple[RN, RV], NamedTuple[LN, t.R]] =
        new Union[NamedTuple[LN, LV], NamedTuple[RN, RV]]:
            type R = NamedTuple[LN, t.R]

            def offset: Int = u.offset

            def unionQueryItems(x: NamedTuple[LN, LV], cursor: Int): R =
                NamedTuple(t.toTuple(u.unionQueryItems(x.toTuple, cursor)))

    given namedTupleUnionTuple[LN <: Tuple, LV <: Tuple, RV <: Tuple](using
        u: Union[LV, RV],
        t: ToTuple[u.R]
    ): Aux[NamedTuple[LN, LV], RV, NamedTuple[LN, t.R]] =
        new Union[NamedTuple[LN, LV], RV]:
            type R = NamedTuple[LN, t.R]

            def offset: Int = u.offset

            def unionQueryItems(x: NamedTuple[LN, LV], cursor: Int): R =
                NamedTuple(t.toTuple(u.unionQueryItems(x.toTuple, cursor)))