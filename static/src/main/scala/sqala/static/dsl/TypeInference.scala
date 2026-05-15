package sqala.static.dsl

import sqala.ast.expr.{SqlBinaryOperator, SqlExpr}
import sqala.metadata.*

import java.time.{OffsetDateTime, OffsetTime}
import scala.NamedTuple.NamedTuple
import scala.compiletime.ops.boolean.||

trait Compare[A, B]

object Compare:
    given number[A: SqlNumber, B: SqlNumber]: Compare[A, B]()

    given dateTime[A: SqlDateTime, B: SqlDateTime]: Compare[A, B]()

    given time[A: SqlTime, B: SqlTime]: Compare[A, B]()

    given string[A: SqlString, B: SqlString]: Compare[A, B]()

    given boolean[A: SqlBoolean, B: SqlBoolean]: Compare[A, B]()

    given json[A: SqlJson, B: SqlJson]: Compare[A, B]()

    given geometry[A: SqlGeometry, B: SqlGeometry]: Compare[A, B]()

    given interval[A: SqlInterval, B: SqlInterval]: Compare[A, B]()

    given dateTimeAndString[A: SqlDateTime, B: SqlString]: Compare[A, B]()

    given stringAndDateTime[A: SqlString, B: SqlDateTime]: Compare[A, B]()

    given timeAndString[A: SqlTime, B: SqlString]: Compare[A, B]()

    given stringAndTime[A: SqlString, B: SqlTime]: Compare[A, B]()

    given array[A, B](using
        Compare[A, B]
    ): Compare[Array[A], Array[B]]()

    given tuple[AH, AT <: Tuple, BH, BT <: Tuple](using
        Compare[AH, BH],
        Compare[AT, BT]
    ): Compare[AH *: AT, BH *: BT]()

    given tuple1[AH, BH](using
        Compare[AH, BH]
    ): Compare[AH *: EmptyTuple, BH *: EmptyTuple]()

trait Plus[A, B, N <: Boolean]:
    type R

    def plus(x: SqlExpr, y: SqlExpr): SqlExpr

object Plus:
    type Aux[A, B, N <: Boolean, O] = Plus[A, B, N]:
        type R = O

    given number[A: SqlNumber, B: SqlNumber, N <: Boolean]: Aux[A, B, N, NumericResult[A, B, N]] =
        new Plus[A, B, N]:
            type R = NumericResult[A, B, N]

            def plus(x: SqlExpr, y: SqlExpr): SqlExpr =
                SqlExpr.Binary(x, SqlBinaryOperator.Plus, y)

    given dateTimeAndInterval[A: SqlDateTime, B: SqlInterval, N <: Boolean]: Aux[A, B, N, WrapIf[OffsetDateTime, N, Option]] =
        new Plus[A, B, N]:
            type R = WrapIf[OffsetDateTime, N, Option]

            def plus(x: SqlExpr, y: SqlExpr): SqlExpr =
                SqlExpr.Binary(x, SqlBinaryOperator.Plus, y)

    given timeAndInterval[A: SqlTime, B: SqlInterval, N <: Boolean]: Aux[A, B, N, WrapIf[OffsetTime, N, Option]] =
        new Plus[A, B, N]:
            type R = WrapIf[OffsetTime, N, Option]

            def plus(x: SqlExpr, y: SqlExpr): SqlExpr =
                SqlExpr.Binary(x, SqlBinaryOperator.Plus, y)

    given string[A: SqlString, B: SqlString, N <: Boolean]: Aux[A, B, N, WrapIf[String, N, Option]] =
        new Plus[A, B, N]:
            type R = WrapIf[String, N, Option]

            def plus(x: SqlExpr, y: SqlExpr): SqlExpr =
                SqlExpr.Binary(x, SqlBinaryOperator.Concat, y)

trait Minus[A, B, N <: Boolean]:
    type R

object Minus:
    type Aux[A, B, N <: Boolean, O] = Minus[A, B, N]:
        type R = O

    given number[A: SqlNumber, B: SqlNumber, N <: Boolean]: Aux[A, B, N, NumericResult[A, B, N]] =
        new Minus[A, B, N]:
            type R = NumericResult[A, B, N]

    given dateTime[A: SqlDateTime, B: SqlDateTime, N <: Boolean]: Aux[A, B, N, WrapIf[Interval, N, Option]] =
        new Minus[A, B, N]:
            type R = WrapIf[Interval, N, Option]

    given time[A: SqlTime, B: SqlTime, N <: Boolean]: Aux[A, B, N, WrapIf[Interval, N, Option]] =
        new Minus[A, B, N]:
            type R = WrapIf[Interval, N, Option]

    given dateTimeAndInterval[A: SqlDateTime, B: SqlInterval, N <: Boolean]: Aux[A, B, N, WrapIf[OffsetDateTime, N, Option]] =
        new Minus[A, B, N]:
            type R = WrapIf[OffsetDateTime, N, Option]

    given timeAndInterval[A: SqlTime, B: SqlInterval, N <: Boolean]: Aux[A, B, N, WrapIf[OffsetTime, N, Option]] =
        new Minus[A, B, N]:
            type R = WrapIf[OffsetTime, N, Option]

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
    given number[S: SqlNumber, N: SqlNumber]: CanInRangeFrame[S, N]()

    given dateTime[T: SqlDateTime, I: SqlInterval]: CanInRangeFrame[T, I]()

    given time[T: SqlTime, I: SqlInterval]: CanInRangeFrame[T, I]()

    given nothing[S]: CanInRangeFrame[S, Nothing]()

trait UnnestReturn[T]:
    type R

object UnnestReturn:
    type Aux[T, O] = UnnestReturn[T]:
        type R = O

    given flatten[T]: Aux[T, FlattenUnnest[T]] =
        new UnnestReturn[T]:
            type R = FlattenUnnest[T]

trait Return[A, B, N <: Boolean]:
    type R

object Return:
    type Aux[A, B, N <: Boolean, O] = Return[A, B, N]:
        type R = O

    given number[A: SqlNumber, B: SqlNumber, N <: Boolean]: Aux[A, B, N, NumericResult[A, B, N]] =
        new Return[A, B, N]:
            type R = NumericResult[A, B, N]

    given dateTime[A: SqlDateTime, B: SqlDateTime, N <: Boolean]: Aux[A, B, N, DateTimeResult[A, B, N]] =
        new Return[A, B, N]:
            type R = DateTimeResult[A, B, N]

    given string[A: SqlString, B: SqlString]: Aux[A, B, false, String] =
        new Return[A, B, false]:
            type R = String

    given optionString[A: SqlString, B: SqlString]: Aux[A, B, true, Option[String]] =
        new Return[A, B, true]:
            type R = Option[String]

    given boolean[A: SqlBoolean, B: SqlBoolean]: Aux[A, B, false, Boolean] =
        new Return[A, B, false]:
            type R = Boolean

    given optionBoolean[A: SqlBoolean, B: SqlBoolean]: Aux[A, B, true, Option[Boolean]] =
        new Return[A, B, true]:
            type R = Option[Boolean]

    given json[A: SqlJson, B: SqlJson]: Aux[A, B, false, Json] =
        new Return[A, B, false]:
            type R = Json

    given optionJson[A: SqlJson, B: SqlJson]: Aux[A, B, true, Option[Json]] =
        new Return[A, B, true]:
            type R = Option[Json]

    given geometry[A: SqlGeometry, B: SqlGeometry]: Aux[A, B, false, Geometry] =
        new Return[A, B, false]:
            type R = Geometry

    given optionGeometry[A: SqlGeometry, B: SqlGeometry]: Aux[A, B, true, Option[Geometry]] =
        new Return[A, B, true]:
            type R = Option[Geometry]

    given interval[A: SqlInterval, B: SqlInterval]: Aux[A, B, false, Interval] =
        new Return[A, B, false]:
            type R = Interval

    given optionInterval[A: SqlInterval, B: SqlInterval]: Aux[A, B, true, Option[Interval]] =
        new Return[A, B, true]:
            type R = Option[Interval]

    given time[A: SqlTime, B: SqlTime, N <: Boolean]: Aux[A, B, N, TimeResult[A, B, N]] =
        new Return[A, B, N]:
            type R = TimeResult[A, B, N]

    given dateTimeAndString[A: SqlDateTime, B: SqlString, N <: Boolean]: Aux[A, B, N, DateTimeResult[A, B, N]] =
        new Return[A, B, N]:
            type R = DateTimeResult[A, B, N]

    given stringAndDateTime[A: SqlString, B: SqlDateTime, N <: Boolean]: Aux[A, B, N, DateTimeResult[A, B, N]] =
        new Return[A, B, N]:
            type R = DateTimeResult[A, B, N]

    given timeAndString[A: SqlTime, B: SqlString, N <: Boolean]: Aux[A, B, N, TimeResult[A, B, N]] =
        new Return[A, B, N]:
            type R = TimeResult[A, B, N]

    given stringAndTime[A: SqlString, B: SqlTime, N <: Boolean]: Aux[A, B, N, TimeResult[A, B, N]] =
        new Return[A, B, N]:
            type R = TimeResult[A, B, N]

    given array[A, B](using
        r: Return[A, B, IsOption[A] || IsOption[B]]
    ): Aux[Array[A], Array[B], false, Array[r.R]] =
        new Return[Array[A], Array[B], false]:
            type R = Array[r.R]

    given optionArray[A, B](using
        r: Return[A, B, IsOption[A] || IsOption[B]]
    ): Aux[Array[A], Array[B], true, Option[Array[r.R]]] =
        new Return[Array[A], Array[B], true]:
            type R = Option[Array[r.R]]

trait Union[A, B, CL <: Int]:
    type R

    def offset: Int

    def unionQueryItems(x: A, cursor: Int): R

object Union:
    type Aux[A, B, CL <: Int, O] = Union[A, B, CL]:
        type R = O

    given union[A, AK <: ExprKind, B, BK <: ExprKind, CL <: Int](using
        r: Return[A, B, IsOption[A] || IsOption[B]]
    ): Aux[Expr[A, AK], Expr[B, BK], CL, Expr[r.R, Column[CL]]] =
        new Union[Expr[A, AK], Expr[B, BK], CL]:
            type R = Expr[r.R, Column[CL]]

            def offset: Int = 1

            def unionQueryItems(x: Expr[A, AK], cursor: Int): R =
                Expr(SqlExpr.Column(None, s"c$cursor"))

    given tupleUnion[AH, AT <: Tuple, BH, BT <: Tuple, CL <: Int](using
        h: Union[AH, BH, CL],
        t: Union[AT, BT, CL],
        tt: ToTuple[t.R]
    ): Aux[AH *: AT, BH *: BT, CL, h.R *: tt.R] =
        new Union[AH *: AT, BH *: BT, CL]:
            type R = h.R *: tt.R

            def offset: Int = h.offset + t.offset

            def unionQueryItems(x: AH *: AT, cursor: Int): R =
                h.unionQueryItems(x.head, cursor) *:
                    tt.toTuple(t.unionQueryItems(x.tail, cursor + h.offset))

    given tuple1Union[AH, BH, CL <: Int](using
        h: Union[AH, BH, CL]
    ): Aux[AH *: EmptyTuple, BH *: EmptyTuple, CL, h.R *: EmptyTuple] =
        new Union[AH *: EmptyTuple, BH *: EmptyTuple, CL]:
            type R = h.R *: EmptyTuple

            def offset: Int = h.offset

            def unionQueryItems(x: AH *: EmptyTuple, cursor: Int): R =
                h.unionQueryItems(x.head, cursor) *: EmptyTuple

    given namedTupleUnion[AN <: Tuple, AV <: Tuple, BN <: Tuple, BV <: Tuple, CL <: Int](using
        u: Union[AV, BV, CL],
        t: ToTuple[u.R]
    ): Aux[NamedTuple[AN, AV], NamedTuple[BN, BV], CL, NamedTuple[AN, t.R]] =
        new Union[NamedTuple[AN, AV], NamedTuple[BN, BV], CL]:
            type R = NamedTuple[AN, t.R]

            def offset: Int = u.offset

            def unionQueryItems(x: NamedTuple[AN, AV], cursor: Int): R =
                NamedTuple(t.toTuple(u.unionQueryItems(x.toTuple, cursor)))

    given namedTupleUnionTuple[AN <: Tuple, AV <: Tuple, BV <: Tuple, CL <: Int](using
        u: Union[AV, BV, CL],
        t: ToTuple[u.R]
    ): Aux[NamedTuple[AN, AV], BV, CL, NamedTuple[AN, t.R]] =
        new Union[NamedTuple[AN, AV], BV, CL]:
            type R = NamedTuple[AN, t.R]

            def offset: Int = u.offset

            def unionQueryItems(x: NamedTuple[AN, AV], cursor: Int): R =
                NamedTuple(t.toTuple(u.unionQueryItems(x.toTuple, cursor)))