package sqala.static.dsl

import sqala.ast.expr.{SqlBinaryOperator, SqlExpr, SqlInRightOperand}
import sqala.metadata.*
import sqala.util.NonEmptyList.toNonEmptyList

import java.time.{OffsetDateTime, OffsetTime}
import scala.NamedTuple.NamedTuple
import scala.compiletime.ops.boolean.||

/**
 * Determines whether two types are compatible for comparison
 * operators (===, <>, >, <, etc.).
 */
trait CanCompare[A, B]

object CanCompare:
    given number[A: SqlNumber, B: SqlNumber]: CanCompare[A, B]()

    given dateTime[A: SqlDateTime, B: SqlDateTime]: CanCompare[A, B]()

    given time[A: SqlTime, B: SqlTime]: CanCompare[A, B]()

    given string[A: SqlString, B: SqlString]: CanCompare[A, B]()

    given boolean[A: SqlBoolean, B: SqlBoolean]: CanCompare[A, B]()

    given json[A: SqlJson, B: SqlJson]: CanCompare[A, B]()

    given geometry[A: SqlGeometry, B: SqlGeometry]: CanCompare[A, B]()

    given interval[A: SqlInterval, B: SqlInterval]: CanCompare[A, B]()

    given dateTimeAndString[A: SqlDateTime, B: SqlString]: CanCompare[A, B]()

    given stringAndDateTime[A: SqlString, B: SqlDateTime]: CanCompare[A, B]()

    given timeAndString[A: SqlTime, B: SqlString]: CanCompare[A, B]()

    given stringAndTime[A: SqlString, B: SqlTime]: CanCompare[A, B]()

    given custom[T, R](using CustomField[T, R]): CanCompare[T, T]()

    given customAndOptionCustom[T, R](using
        CustomField[T, R]
    ): CanCompare[T, Option[T]]()

    given optionCustom[T, R](using
        CustomField[T, R]
    ): CanCompare[Option[T], T]()

    given array[A, B](using
        CanCompare[A, B]
    ): CanCompare[Array[A], Array[B]]()

    given arrayAndOptionArray[A, B](using
        CanCompare[A, B]
    ): CanCompare[Array[A], Option[Array[B]]]()

    given optionArrayAndArray[A, B](using
        CanCompare[A, B]
    ): CanCompare[Option[Array[A]], Array[B]]()

    given optionArray[A, B](using
        CanCompare[A, B]
    ): CanCompare[Option[Array[A]], Option[Array[B]]]()

    given tuple[AH, AT <: Tuple, BH, BT <: Tuple](using
        CanCompare[AH, BH],
        CanCompare[AT, BT]
    ): CanCompare[AH *: AT, BH *: BT]()

    given tupleAndOptionTuple[AH, AT <: Tuple, BH, BT <: Tuple](using
        CanCompare[AH, BH],
        CanCompare[AT, BT]
    ): CanCompare[AH *: AT, Option[BH *: BT]]()

    given optionTupleAndTuple[AH, AT <: Tuple, BH, BT <: Tuple](using
        CanCompare[AH, BH],
        CanCompare[AT, BT]
    ): CanCompare[Option[AH *: AT], BH *: BT]()

    given optionTuple[AH, AT <: Tuple, BH, BT <: Tuple](using
        CanCompare[AH, BH],
        CanCompare[AT, BT]
    ): CanCompare[Option[AH *: AT], Option[BH *: BT]]()

    given tuple1[AH, BH](using
        CanCompare[AH, BH]
    ): CanCompare[AH *: EmptyTuple, BH *: EmptyTuple]()

    given tuple1AndOptionTuple1[AH, BH](using
        CanCompare[AH, BH]
    ): CanCompare[AH *: EmptyTuple, Option[BH *: EmptyTuple]]()

    given optionTuple1AndTuple1[AH, BH](using
        CanCompare[AH, BH]
    ): CanCompare[Option[AH *: EmptyTuple], BH *: EmptyTuple]()

    given optionTuple1AndOptionTuple1[AH, BH](using
        CanCompare[AH, BH]
    ): CanCompare[Option[AH *: EmptyTuple], Option[BH *: EmptyTuple]]()

/**
 * Lifts values, tuples, sequences, and arrays into `in` operands, 
 * computing the result type and kind tuple. `CL` is the
 * current query context level.
 */
trait In[A, B, CL <: Int]:
    /**
     * The result type.
     */
    type R

    /**
     * The kind tuple.
     */
    type KS <: Tuple

    /**
     * Converts the value to a list of expressions.
     */
    def asExprs(x: B): List[Expr[?, ?]]

    /**
     * Converts the value to a single expression, wrapping multiple
     * operands in a tuple. Subqueries are passed through without
     * wrapping.
     */
    def asSqlExpr(expr: SqlExpr, x: B): SqlExpr =
        val exprList = asExprs(x)
        exprList match
            case Nil =>
                SqlExpr.BooleanLiteral(false)
            case Expr(SqlExpr.Subquery(query)) :: Nil =>
                SqlExpr.In(expr, SqlInRightOperand.Subquery(query), false)
            case _ =>
                SqlExpr.In(
                    expr, 
                    SqlInRightOperand.Values(exprList.map(_.asSqlExpr).toNonEmptyList), 
                    false
                )

object In:
    type Aux[A, B, CL <: Int, O, OKS <: Tuple] = In[A, B, CL]:
        type R = O

        type KS = OKS

    given expr[A, B, CL <: Int](using
        aa: AsExpr[A, CL],
        ab: AsExpr[B, CL],
        r: Relation[aa.R, ab.R],
        kt: KindToTuple[ab.K]
    ): Aux[A, B, CL, r.R, kt.R] =
        new In[A, B, CL]:
            type R = r.R

            type KS = kt.R

            def asExprs(x: B): List[Expr[?, ?]] =
                ab.asExpr(x) :: Nil

    given tuple[A, H, T <: Tuple, CL <: Int](using
        aa: AsExpr[A, CL],
        ah: AsExpr[H, CL],
        t: In[A, T, CL],
        rh: Relation[aa.R, ah.R],
        r: Relation[rh.R, t.R],
        kth: KindToTuple[ah.K],
        c: CombineKindTuple[kth.R, t.KS]
    ): Aux[A, H *: T, CL, r.R, c.R] =
        new In[A, H *: T, CL]:
            type R = r.R

            type KS = c.R

            def asExprs(x: H *: T): List[Expr[?, ?]] =
                ah.asExpr(x.head) :: t.asExprs(x.tail)

    given tuple1[A, H, CL <: Int](using
        aa: AsExpr[A, CL],
        ah: AsExpr[H, CL],
        r: Relation[aa.R, ah.R],
        kth: KindToTuple[ah.K]
    ): Aux[A, H *: EmptyTuple, CL, r.R, kth.R] =
        new In[A, H *: EmptyTuple, CL]:
            type R = r.R

            type KS = kth.R

            def asExprs(x: H *: EmptyTuple): List[Expr[?, ?]] =
                ah.asExpr(x.head) :: Nil

    given seq[A, B, S <: Seq[B], CL <: Int](using
        aa: AsExpr[A, CL],
        ab: AsSqlExpr[B],
        r: Relation[aa.R, B]
    ): Aux[A, S, CL, r.R, Value *: EmptyTuple] =
        new In[A, S, CL]:
            type R = r.R

            type KS = Value *: EmptyTuple

            def asExprs(x: S): List[Expr[?, ?]] =
                x.toList.map(i => Expr(ab.asSqlExpr(i)))

    given array[A, B, CL <: Int](using
        aa: AsExpr[A, CL],
        ab: AsSqlExpr[B],
        r: Relation[aa.R, B],
    ): Aux[A, Array[B], CL, r.R, Value *: EmptyTuple] =
        new In[A, Array[B], CL]:
            type R = r.R

            type KS = Value *: EmptyTuple

            def asExprs(x: Array[B]): List[Expr[?, ?]] =
                x.toList.map(i => Expr(ab.asSqlExpr(i)))

/**
 * Computes the result type of addition (via `+`).
 */
trait Plus[A, B]:
    /**
     * The result type.
     */
    type R

    /**
     * Produces the SQL expression for the addition.
     */
    def plus(x: SqlExpr, y: SqlExpr): SqlExpr

object Plus:
    type Aux[A, B, O] = Plus[A, B]:
        type R = O

    given number[A: SqlNumber, B: SqlNumber]: Aux[A, B, NumericResult[A, B]] =
        new Plus[A, B]:
            type R = NumericResult[A, B]

            def plus(x: SqlExpr, y: SqlExpr): SqlExpr =
                SqlExpr.Binary(x, SqlBinaryOperator.Plus, y)

    given dateTimeAndInterval[A: SqlDateTime, B: SqlInterval]: Aux[A, B, WrapIf[OffsetDateTime, IsOption[A] || IsOption[B], Option]] =
        new Plus[A, B]:
            type R = WrapIf[OffsetDateTime, IsOption[A] || IsOption[B], Option]

            def plus(x: SqlExpr, y: SqlExpr): SqlExpr =
                SqlExpr.Binary(x, SqlBinaryOperator.Plus, y)

    given timeAndInterval[A: SqlTime, B: SqlInterval]: Aux[A, B, WrapIf[OffsetTime, IsOption[A] || IsOption[B], Option]] =
        new Plus[A, B]:
            type R = WrapIf[OffsetTime, IsOption[A] || IsOption[B], Option]

            def plus(x: SqlExpr, y: SqlExpr): SqlExpr =
                SqlExpr.Binary(x, SqlBinaryOperator.Plus, y)

    given string[A: SqlString, B: SqlString]: Aux[A, B, WrapIf[String, IsOption[A] || IsOption[B], Option]] =
        new Plus[A, B]:
            type R = WrapIf[String, IsOption[A] || IsOption[B], Option]

            def plus(x: SqlExpr, y: SqlExpr): SqlExpr =
                SqlExpr.Binary(x, SqlBinaryOperator.Concat, y)

/**
 * Computes the result type of subtraction (via `-`).
 */
trait Minus[A, B]:
    /**
     * The result type.
     */
    type R

object Minus:
    type Aux[A, B, O] = Minus[A, B]:
        type R = O

    given number[A: SqlNumber, B: SqlNumber]: Aux[A, B, NumericResult[A, B]] =
        new Minus[A, B]:
            type R = NumericResult[A, B]

    given dateTime[A: SqlDateTime, B: SqlDateTime]: Aux[A, B, WrapIf[Interval, IsOption[A] || IsOption[B], Option]] =
        new Minus[A, B]:
            type R = WrapIf[Interval, IsOption[A] || IsOption[B], Option]

    given time[A: SqlTime, B: SqlTime]: Aux[A, B, WrapIf[Interval, IsOption[A] || IsOption[B], Option]] =
        new Minus[A, B]:
            type R = WrapIf[Interval, IsOption[A] || IsOption[B], Option]

    given dateTimeAndInterval[A: SqlDateTime, B: SqlInterval]: Aux[A, B, WrapIf[OffsetDateTime, IsOption[A] || IsOption[B], Option]] =
        new Minus[A, B]:
            type R = WrapIf[OffsetDateTime, IsOption[A] || IsOption[B], Option]

    given timeAndInterval[A: SqlTime, B: SqlInterval]: Aux[A, B, WrapIf[OffsetTime, IsOption[A] || IsOption[B], Option]] =
        new Minus[A, B]:
            type R = WrapIf[OffsetTime, IsOption[A] || IsOption[B], Option]

/**
 * Computes the result type of a comparison.
 * Always wraps `Boolean` with `Option` if either side is optional.
 */
trait Relation[A, B]:
    /**
     * The result type.
     */
    type R

object Relation:
    type Aux[A, B, O] = Relation[A, B]:
        type R = O

    given relation[A, B](using 
        CanCompare[A, B]
    ): Aux[A, B, WrapIf[Boolean, IsOption[A] || IsOption[B], Option]] =
        new Relation[A, B]:
            type R = WrapIf[Boolean, IsOption[A] || IsOption[B], Option]

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

/**
 * Computes the return type of `unnest`.
 */
trait UnnestReturn[T]:
    /**
     * The result type.
     */
    type R

object UnnestReturn:
    type Aux[T, O] = UnnestReturn[T]:
        type R = O

    given flatten[T]: Aux[T, FlattenUnnest[T]] =
        new UnnestReturn[T]:
            type R = FlattenUnnest[T]

/**
 * Computes the result type of a conditional expression (such as
 * `caseWhen` or `ifNull`).
 */
trait Return[A, B]:
    /**
     * The result type.
     */
    type R

object Return:
    type Aux[A, B, O] = Return[A, B]:
        type R = O

    given number[A: SqlNumber, B: SqlNumber]: Aux[A, B, NumericResult[A, B]] =
        new Return[A, B]:
            type R = NumericResult[A, B]

    given string[A: SqlString, B: SqlString]: Aux[A, B, WrapIf[String, IsOption[A] || IsOption[B], Option]] =
        new Return[A, B]:
            type R = WrapIf[String, IsOption[A] || IsOption[B], Option]

    given boolean[A: SqlBoolean, B: SqlBoolean]: Aux[A, B, WrapIf[Boolean, IsOption[A] || IsOption[B], Option]] =
        new Return[A, B]:
            type R = WrapIf[Boolean, IsOption[A] || IsOption[B], Option]

    given json[A: SqlJson, B: SqlJson]: Aux[A, B, WrapIf[Json, IsOption[A] || IsOption[B], Option]] =
        new Return[A, B]:
            type R = WrapIf[Json, IsOption[A] || IsOption[B], Option]

    given geometry[A: SqlGeometry, B: SqlGeometry]: Aux[A, B, WrapIf[Geometry, IsOption[A] || IsOption[B], Option]] =
        new Return[A, B]:
            type R = WrapIf[Geometry, IsOption[A] || IsOption[B], Option]

    given interval[A: SqlInterval, B: SqlInterval]: Aux[A, B, WrapIf[Interval, IsOption[A] || IsOption[B], Option]] =
        new Return[A, B]:
            type R = WrapIf[Interval, IsOption[A] || IsOption[B], Option]

    given dateTime[A: SqlDateTime, B: SqlDateTime]: Aux[A, B, DateTimeResult[A, B]] =
        new Return[A, B]:
            type R = DateTimeResult[A, B]

    given time[A: SqlTime, B: SqlTime]: Aux[A, B, TimeResult[A, B]] =
        new Return[A, B]:
            type R = TimeResult[A, B]

    given dateTimeAndString[A: SqlDateTime, B: SqlString]: Aux[A, B, DateTimeResult[A, B]] =
        new Return[A, B]:
            type R = DateTimeResult[A, B]

    given stringAndDateTime[A: SqlString, B: SqlDateTime]: Aux[A, B, DateTimeResult[A, B]] =
        new Return[A, B]:
            type R = DateTimeResult[A, B]

    given timeAndString[A: SqlTime, B: SqlString]: Aux[A, B, TimeResult[A, B]] =
        new Return[A, B]:
            type R = TimeResult[A, B]

    given stringAndTime[A: SqlString, B: SqlTime]: Aux[A, B, TimeResult[A, B]] =
        new Return[A, B]:
            type R = TimeResult[A, B]

    given custom[T, R](using 
        CustomField[T, R]
    ): Aux[T, T, T] =
        new Return[T, T]:
            type R = T

    given customAndOptionCustom[T, R](using
        CustomField[T, R]
    ): Aux[T, Option[T], Option[T]] =
        new Return[T, Option[T]]:
            type R = Option[T]

    given optionCustomAndCustom[T, R](using
        CustomField[T, R]
    ): Aux[Option[T], T, Option[T]] =
        new Return[Option[T], T]:
            type R = Option[T]

    given array[A, B](using
        r: Return[A, B]
    ): Aux[Array[A], Array[B], Array[r.R]] =
        new Return[Array[A], Array[B]]:
            type R = Array[r.R]

    given arrayAndOptionArray[A, B](using
        r: Return[A, B]
    ): Aux[Array[A], Option[Array[B]], Option[Array[r.R]]] =
        new Return[Array[A], Option[Array[B]]]:
            type R = Option[Array[r.R]]

    given optionArrayAndArray[A, B](using
        r: Return[A, B]
    ): Aux[Option[Array[A]], Array[B], Option[Array[r.R]]] =
        new Return[Option[Array[A]], Array[B]]:
            type R = Option[Array[r.R]]

    given optionArray[A, B](using
        r: Return[A, B]
    ): Aux[Option[Array[A]], Option[Array[B]], Option[Array[r.R]]] =
        new Return[Option[Array[A]], Option[Array[B]]]:
            type R = Option[Array[r.R]]

/**
 * Maps union query items, producing column expressions
 * for `union`, `unionAll`, etc. `CL` is the query context level.
 */
trait Union[A, B, CL <: Int]:
    /**
     * The result type.
     */
    type R

    /**
     * The number of expressions consumed by each query item.
     */
    def offset: Int

    /**
     * Maps the query item at the given cursor position to a column
     * expression.
     */
    def unionQueryItems(x: A, cursor: Int): R

object Union:
    type Aux[A, B, CL <: Int, O] = Union[A, B, CL]:
        type R = O

    given union[A, AK <: ExprKind, B, BK <: ExprKind, CL <: Int](using
        r: Return[A, B]
    ): Aux[Expr[A, AK], Expr[B, BK], CL, Expr[r.R, Column[CL]]] =
        new Union[Expr[A, AK], Expr[B, BK], CL]:
            type R = Expr[r.R, Column[CL]]

            def offset: Int = 
                1

            def unionQueryItems(x: Expr[A, AK], cursor: Int): R =
                Expr(SqlExpr.Column(None, s"c$cursor"))

    given tupleUnion[AH, AT <: Tuple, BH, BT <: Tuple, CL <: Int](using
        h: Union[AH, BH, CL],
        t: Union[AT, BT, CL],
        tt: ToTuple[t.R]
    ): Aux[AH *: AT, BH *: BT, CL, h.R *: tt.R] =
        new Union[AH *: AT, BH *: BT, CL]:
            type R = h.R *: tt.R

            def offset: Int = 
                h.offset + t.offset

            def unionQueryItems(x: AH *: AT, cursor: Int): R =
                h.unionQueryItems(x.head, cursor) *:
                    tt.toTuple(t.unionQueryItems(x.tail, cursor + h.offset))

    given tuple1Union[AH, BH, CL <: Int](using
        h: Union[AH, BH, CL]
    ): Aux[AH *: EmptyTuple, BH *: EmptyTuple, CL, h.R *: EmptyTuple] =
        new Union[AH *: EmptyTuple, BH *: EmptyTuple, CL]:
            type R = h.R *: EmptyTuple

            def offset: Int = 
                h.offset

            def unionQueryItems(x: AH *: EmptyTuple, cursor: Int): R =
                h.unionQueryItems(x.head, cursor) *: EmptyTuple

    given namedTupleUnion[AN <: Tuple, AV <: Tuple, BN <: Tuple, BV <: Tuple, CL <: Int](using
        u: Union[AV, BV, CL],
        t: ToTuple[u.R]
    ): Aux[NamedTuple[AN, AV], NamedTuple[BN, BV], CL, NamedTuple[AN, t.R]] =
        new Union[NamedTuple[AN, AV], NamedTuple[BN, BV], CL]:
            type R = NamedTuple[AN, t.R]

            def offset: Int = 
                u.offset

            def unionQueryItems(x: NamedTuple[AN, AV], cursor: Int): R =
                NamedTuple(t.toTuple(u.unionQueryItems(x.toTuple, cursor)))

    given namedTupleUnionTuple[AN <: Tuple, AV <: Tuple, BV <: Tuple, CL <: Int](using
        u: Union[AV, BV, CL],
        t: ToTuple[u.R]
    ): Aux[NamedTuple[AN, AV], BV, CL, NamedTuple[AN, t.R]] =
        new Union[NamedTuple[AN, AV], BV, CL]:
            type R = NamedTuple[AN, t.R]

            def offset: Int = 
                u.offset

            def unionQueryItems(x: NamedTuple[AN, AV], cursor: Int): R =
                NamedTuple(t.toTuple(u.unionQueryItems(x.toTuple, cursor)))