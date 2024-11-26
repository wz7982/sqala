package sqala.dsl

import java.time.LocalDateTime
import scala.util.NotGiven
import scala.annotation.implicitNotFound
import scala.compiletime.{erasedValue, summonInline}

@implicitNotFound("The type ${T} cannot be converted to SQL expression.")
trait ComparableValue[T]:
    def exprs(x: T): List[Expr[?]]

    def asExpr(x: T): Expr[?] =
        val exprList = exprs(x)
        if exprList.size == 1 then exprList.head
        else Expr.Vector(exprList)

object ComparableValue:
    given valueAsExpr[T](using a: AsSqlExpr[T], n: NotGiven[T <:< Option[?]]): ComparableValue[T] with
        def exprs(x: T): List[Expr[?]] =
            Expr.Literal(x, a) :: Nil

    given tupleAsExpr[H, T <: Tuple](using 
        h: AsSqlExpr[H], 
        t: ComparableValue[T]
    ): ComparableValue[H *: T] with
        def exprs(x: H *: T): List[Expr[?]] =
            val head = Expr.Literal(x.head, h)
            val tail = t.exprs(x.tail)
            head :: tail

    given tuple1AsExpr[H](using h: AsSqlExpr[H]): ComparableValue[H *: EmptyTuple] with
        def exprs(x: H *: EmptyTuple): List[Expr[?]] =
            val head = Expr.Literal(x.head, h)
            head :: Nil

@implicitNotFound("Types ${A} and ${B} be cannot compared.")
trait CompareOperation[A, B]

object CompareOperation:
    given compare[A]: CompareOperation[A, A]()

    given optionCompare[A]: CompareOperation[A, Option[A]]()

    given valueCompare[A]: CompareOperation[Option[A], A]()

    given numericCompare[A: Number, B: Number]: CompareOperation[A, B]()

    given timeCompare[A: DateTime, B: DateTime]: CompareOperation[A, B]()

    given timeAndStringCompare[A: DateTime, B <: String | Option[String]]: CompareOperation[A, B]()

    given stringAndTimeCompare[A <: String | Option[String], B: DateTime]: CompareOperation[A, B]()

    given tupleCompare[LH, LT <: Tuple, RH, RT <: Tuple](using CompareOperation[LH, RH], CompareOperation[LT, RT]): CompareOperation[LH *: LT, RH *: RT]()

    given tuple1Compare[LH, RH](using CompareOperation[LH, RH]): CompareOperation[LH *: EmptyTuple, RH *: EmptyTuple]()

@implicitNotFound("Types ${A} and ${B} cannot be returned as results.")
trait ResultOperation[A, B]:
    type R

object ResultOperation:
    type Aux[A, B, O] = ResultOperation[A, B]:
        type R = O

    given result[A]: Aux[A, A, A] =
        new ResultOperation[A, A]:
            type R = A

    given optionResult[A]: Aux[A, Option[A], Option[A]] =
        new ResultOperation[A, Option[A]]:
            type R = Option[A]

    given valueResult[A]: Aux[Option[A], A, Option[A]] =
        new ResultOperation[Option[A], A]:
            type R = Option[A]

    given numericResult[A: Number, B: Number]: Aux[A, B, NumericResult[A, B]] =
        new ResultOperation[A, B]:
            type R = NumericResult[A, B]

    given timeResult[A: DateTime, B: DateTime]: Aux[A, B, Option[LocalDateTime]] =
        new ResultOperation[A, B]:
            type R = Option[LocalDateTime]

@implicitNotFound("Types ${A} and ${B} cannot be UNION.")
trait UnionOperation[A, B]:
    type R

    def unionQueryItems(x: A): R

object UnionOperation:
    type Aux[A, B, O] = UnionOperation[A, B]:
        type R = O

    given union[A, B](using
        r: ResultOperation[A, B]
    ): Aux[Expr[A], Expr[B], Expr[r.R]] =
        new UnionOperation[Expr[A], Expr[B]]:
            type R = Expr[r.R]

            def unionQueryItems(x: Expr[A]): R =
                Expr.Ref(x.asSqlExpr)

    given tupleUnion[LH, LT <: Tuple, RH, RT <: Tuple](using
        h: UnionOperation[LH, RH],
        t: UnionOperation[LT, RT],
        tt: ToTuple[t.R]
    ): Aux[LH *: LT, RH *: RT, h.R *: tt.R] =
        new UnionOperation[LH *: LT, RH *: RT]:
            type R = h.R *: tt.R

            def unionQueryItems(x: LH *: LT): R =
                h.unionQueryItems(x.head) *: tt.toTuple(t.unionQueryItems(x.tail))

    given tuple1Union[LH, RH](using
        h: UnionOperation[LH, RH]
    ): Aux[LH *: EmptyTuple, RH *: EmptyTuple, h.R *: EmptyTuple] =
        new UnionOperation[LH *: EmptyTuple, RH *: EmptyTuple]:
            type R = h.R *: EmptyTuple

            def unionQueryItems(x: LH *: EmptyTuple): R =
                h.unionQueryItems(x.head) *: EmptyTuple