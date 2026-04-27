package sqala.static.dsl

import sqala.ast.expr.{SqlBinaryOperator, SqlExpr}
import sqala.static.metadata.*

import java.time.OffsetDateTime
import scala.NamedTuple.NamedTuple
import scala.compiletime.ops.boolean.||
import scala.util.NotGiven

trait Compare[A, B]

object Compare:
    given idCompare[A: AsSqlExpr]: Compare[A, A]()

    given numberCompare[A: SqlNumber, B: SqlNumber](using
        NotGiven[A =:= B]
    ): Compare[A, B]()

    given timeCompare[A: SqlDateTime, B: SqlDateTime](using
        NotGiven[A =:= B]
    ): Compare[A, B]()

    given dateTimeAndStringCompare[A: SqlDateTime, B: SqlString]: Compare[A, B]()

    given stringAndDateTimeCompare[A: SqlString, B: SqlDateTime]: Compare[A, B]()

    given timeAndStringCompare[A: SqlTime, B: SqlString]: Compare[A, B]()

    given stringAndTimeCompare[A: SqlString, B: SqlTime]: Compare[A, B]()

    given arrayCompare[A, B](using
        aa: AsExpr[A],
        ab: AsExpr[B],
        c: Compare[Unwrap[aa.R, Option], Unwrap[ab.R, Option]]
    ): Compare[Array[A], Array[B]]()

    given tupleCompare[LH, LT <: Tuple, RH, RT <: Tuple](using
        al: AsExpr[LH],
        ar: AsExpr[RH],
        ch: Compare[Unwrap[al.R, Option], Unwrap[ar.R, Option]],
        ct: Compare[LT, RT]
    ): Compare[LH *: LT, RH *: RT]()

    given tuple1Compare[LH, RH](using
        al: AsExpr[LH],
        ar: AsExpr[RH],
        c: Compare[Unwrap[al.R, Option], Unwrap[ar.R, Option]]
    ): Compare[LH *: EmptyTuple, RH *: EmptyTuple]()

trait Plus[A, B, Nullable <: Boolean]:
    type R

    def plus(x: SqlExpr, y: SqlExpr): SqlExpr

object Plus:
    type Aux[A, B, N <: Boolean, O] = Plus[A, B, N]:
        type R = O

    given numberPlus[A: SqlNumber, B: SqlNumber, N <: Boolean]: Aux[A, B, N, NumericResult[A, B, N]] =
        new Plus[A, B, N]:
            type R = NumericResult[A, B, N]

            def plus(x: SqlExpr, y: SqlExpr): SqlExpr =
                SqlExpr.Binary(x, SqlBinaryOperator.Plus, y)

    given dateTimePlusInterval[A: SqlDateTime, B: SqlInterval, N <: Boolean]: Aux[A, B, N, WrapIf[OffsetDateTime, N, Option]] =
        new Plus[A, B, N]:
            type R = WrapIf[OffsetDateTime, N, Option]

            def plus(x: SqlExpr, y: SqlExpr): SqlExpr =
                SqlExpr.Binary(x, SqlBinaryOperator.Plus, y)

    given stringPlus[A: SqlString, B: SqlString, N <: Boolean]: Aux[A, B, N, WrapIf[String, N, Option]] =
        new Plus[A, B, N]:
            type R = WrapIf[String, N, Option]

            def plus(x: SqlExpr, y: SqlExpr): SqlExpr =
                SqlExpr.Binary(x, SqlBinaryOperator.Concat, y)

trait Minus[A, B, Nullable <: Boolean]:
    type R

object Minus:
    type Aux[A, B, N <: Boolean, O] = Minus[A, B, N]:
        type R = O

    given numberMinus[A: SqlNumber, B: SqlNumber, N <: Boolean]: Aux[A, B, N, NumericResult[A, B, N]] =
        new Minus[A, B, N]:
            type R = NumericResult[A, B, N]

    given dateTimeMinus[A: SqlDateTime, B: SqlDateTime, N <: Boolean]: Aux[A, B, N, WrapIf[Interval, N, Option]] =
        new Minus[A, B, N]:
            type R = WrapIf[Interval, N, Option]

    given timeMinus[A: SqlTime, B: SqlTime, N <: Boolean]: Aux[A, B, N, WrapIf[Interval, N, Option]] =
        new Minus[A, B, N]:
            type R = WrapIf[Interval, N, Option]

    given dateTimeMinusInterval[A: SqlDateTime, B: SqlInterval, N <: Boolean]: Aux[A, B, N, WrapIf[OffsetDateTime, N, Option]] =
        new Minus[A, B, N]:
            type R = WrapIf[OffsetDateTime, N, Option]

trait Relation[A, B, N <: Boolean]:
    type R

object Relation:
    type Aux[A, B, N <: Boolean, O] = Relation[A, B, N]:
        type R = O

    given relation[A, B, N <: Boolean](using Compare[A, B]): Aux[A, B, N, WrapIf[Boolean, N, Option]] =
        new Relation[A, B, N]:
            type R = WrapIf[Boolean, N, Option]

trait CanInRowsOrGroupsFrame[T]

object CanInRowsOrGroupsFrame:
    given int: CanInRowsOrGroupsFrame[Int]()

    given long: CanInRowsOrGroupsFrame[Long]()

    given nothing: CanInRowsOrGroupsFrame[Nothing]()

trait CanInRangeFrame[S, T]

object CanInRangeFrame:
    given number[S: SqlNumber, N: SqlNumber](using Unwrap[S, Option] =:= Unwrap[N, Option]): CanInRangeFrame[S, N]()

    given time[T: SqlDateTime, I: SqlInterval]: CanInRangeFrame[T, I]()

    given nothing[S]: CanInRangeFrame[S, Nothing]()

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

    given arrayResult[A, B](using
        r: Return[Unwrap[A, Option], Unwrap[B, Option], IsOption[A] || IsOption[B]]
    ): Aux[Array[A], Array[B], false, Array[r.R]] =
        new Return[Array[A], Array[B], false]:
            type R = Array[r.R]

    given arrayOptionResult[A, B](using
        r: Return[Unwrap[A, Option], Unwrap[B, Option], IsOption[A] || IsOption[B]]
    ): Aux[Array[A], Array[B], true, Option[Array[r.R]]] =
        new Return[Array[A], Array[B], true]:
            type R = Option[Array[r.R]]

trait Union[A, B]:
    type R

    def offset: Int

    def unionQueryItems(x: A, cursor: Int): R

object Union:
    type Aux[A, B, O] = Union[A, B]:
        type R = O

    given union[A, AK <: ExprKind, B, BK <: ExprKind](using
        r: Return[Unwrap[A, Option], Unwrap[B, Option], IsOption[A] || IsOption[B]]
    ): Aux[Expr[A, AK], Expr[B, BK], Expr[r.R, Column]] =
        new Union[Expr[A, AK], Expr[B, BK]]:
            type R = Expr[r.R, Column]

            def offset: Int = 1

            def unionQueryItems(x: Expr[A, AK], cursor: Int): R =
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