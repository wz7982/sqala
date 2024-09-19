package sqala.dsl

import java.util.Date
import scala.annotation.implicitNotFound
import scala.compiletime.{erasedValue, summonInline}

@implicitNotFound("The type ${T} cannot be converted to SQL expression")
trait ComparableValue[T]:
    def asExpr(x: T): Expr[?, ?]

object ComparableValue:
    given valueAsExpr[T](using a: AsSqlExpr[T]): ComparableValue[T] with
        def asExpr(x: T): Expr[?, ?] = Expr.Literal(x, a)

    given tupleAsExpr[H, T <: Tuple](using h: AsSqlExpr[H], t: ComparableValue[T]): ComparableValue[H *: T] with
        def asExpr(x: H *: T): Expr[?, ?] =
            val head = Expr.Literal(x.head, h)
            val tail = t.asExpr(x.tail).asInstanceOf[Expr.Vector[?, ?]]
            Expr.Vector(head :: tail.items)

    given tuple1AsExpr[H](using h: AsSqlExpr[H]): ComparableValue[H *: EmptyTuple] with
        def asExpr(x: H *: EmptyTuple): Expr[?, ?] =
            val head = Expr.Literal(x.head, h)
            Expr.Vector(head :: Nil)

@implicitNotFound("Types ${A} and ${B} be cannot compared")
trait CompareOperation[A, B]

object CompareOperation:
    inline def summonInstances[A, T]: List[CompareOperation[?, ?]] =
        inline erasedValue[T] match
            case _: EmptyTuple => Nil
            case _: (Expr[t, k] *: ts) => summonInline[CompareOperation[A, t]] :: summonInstances[A, ts]

    given compare[A]: CompareOperation[A, A]()

    given optionCompare[A]: CompareOperation[A, Option[A]]()

    given valueComprea[A]: CompareOperation[Option[A], A]()

    given numericCompare[A: Number, B: Number]: CompareOperation[A, B]()

    given timeCompare[A: DateTime, B: DateTime]: CompareOperation[A, B]()

    given timeAndStringCompare[A: DateTime, B <: String | Option[String]]: CompareOperation[A, B]()
    
    given stringAndTimeCompare[A <: String | Option[String], B: DateTime]: CompareOperation[A, B]()

    given nothingCompare[B]: CompareOperation[Nothing, B]()

    given tupleCompare[LH, LT <: Tuple, RH, RT <: Tuple](using CompareOperation[LH, RH], CompareOperation[LT, RT]): CompareOperation[LH *: LT, RH *: RT]()

    given emptyTupleCompare: CompareOperation[EmptyTuple, EmptyTuple]()

@implicitNotFound("Types ${A} and ${B} be cannot subtract")
trait MinusOperation[A, B]:
    type R

object MinusOperation:
    transparent inline given numericMinus[A: Number, B: Number]: MinusOperation[A, B] = new MinusOperation[A, B]:
        type R = Option[BigDecimal]

    transparent inline given timeMinus[A: DateTime, B: DateTime]: MinusOperation[A, B] = new MinusOperation[A, B]:
        type R = Option[Date]

@implicitNotFound("Types ${A} and ${B} cannot be returned as results")
trait ResultOperation[A, B]:
    type R

object ResultOperation:
    transparent inline given result[A]: ResultOperation[A, A] = new ResultOperation[A, A]:
        type R = A

    transparent inline given optionResult[A]: ResultOperation[A, Option[A]] = new ResultOperation[A, Option[A]]:
        type R = Option[A]

    transparent inline given valueResult[A]: ResultOperation[Option[A], A] = new ResultOperation[Option[A], A]:
        type R = Option[A]

    transparent inline given numericResult[A: Number, B: Number]: ResultOperation[A, B] = new ResultOperation[A, B]:
        type R = Option[BigDecimal]

    transparent inline given timeResulte[A: DateTime, B: DateTime]: ResultOperation[A, B] = new ResultOperation[A, B]:
        type R = Option[Date]

    transparent inline given nothingResult[B]: ResultOperation[Nothing, B] = new ResultOperation[Nothing, B]:
        type R = B

@implicitNotFound("Types ${A} and ${B} cannot be UNION")
trait UnionOperation[A, B]:
    type R

object UnionOperation:
    transparent inline given union[A, K <: ExprKind, B, UK <: ExprKind](using r: ResultOperation[A, B]): UnionOperation[Expr[A, K], Expr[B, UK]] = new UnionOperation[Expr[A, K], Expr[B, UK]]:
        type R = Expr[r.R, ColumnKind]

    transparent inline given tupleUnion[LH, LT <: Tuple, RH, RT <: Tuple](using h: UnionOperation[LH, RH], t: UnionOperation[LT, RT]): UnionOperation[LH *: LT, RH *: RT] = new UnionOperation[LH *: LT, RH *: RT]:
        type R = h.R *: ToTuple[t.R]

    transparent inline given emptyTupleUnion: UnionOperation[EmptyTuple, EmptyTuple] = new UnionOperation[EmptyTuple, EmptyTuple]:
        type R = EmptyTuple

@implicitNotFound("Aggregate function or grouped column cannot be compared with non-aggregate function")
trait KindOperation[A <: ExprKind, B <: ExprKind]

object KindOperation:
    inline def summonInstances[A <: ExprKind, T]: List[KindOperation[?, ?]] =
        inline erasedValue[T] match
            case _: EmptyTuple => Nil
            case _: (Expr[t, k] *: ts) => summonInline[KindOperation[A, k]] :: summonInstances[A, ts]

    given value[B <: ExprKind]: KindOperation[ValueKind, B]()

    given agg[A <: AggKind | AggOperationKind | GroupKind, B <: AggKind | AggOperationKind | GroupKind | ValueKind]: KindOperation[A, B]()

    given nonAgg[A <: CommonKind | ColumnKind | WindowKind | DistinctKind, B <: CommonKind | ColumnKind | WindowKind | ValueKind | DistinctKind]: KindOperation[A, B]()