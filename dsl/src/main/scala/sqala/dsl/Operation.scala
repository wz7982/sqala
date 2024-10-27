package sqala.dsl

import java.util.Date
import scala.NamedTuple.NamedTuple
import scala.annotation.implicitNotFound
import scala.compiletime.{erasedValue, summonInline}

@implicitNotFound("The type ${T} cannot be converted to SQL expression.")
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

@implicitNotFound("Types ${A} and ${B} be cannot compared.")
trait CompareOperation[A, B]

object CompareOperation:
    inline def summonInstances[A, T]: List[CompareOperation[?, ?]] =
        inline erasedValue[T] match
            case _: EmptyTuple => Nil
            case _: (Expr[t, k] *: ts) => summonInline[CompareOperation[A, t]] :: summonInstances[A, ts]

    given compare[A: AsSqlExpr]: CompareOperation[A, A]()

    given optionCompare[A: AsSqlExpr]: CompareOperation[A, Option[A]]()

    given valueCompare[A: AsSqlExpr]: CompareOperation[Option[A], A]()

    given numericCompare[A: Number, B: Number]: CompareOperation[A, B]()

    given timeCompare[A: DateTime, B: DateTime]: CompareOperation[A, B]()

    given timeAndStringCompare[A: DateTime, B <: String | Option[String]]: CompareOperation[A, B]()
    
    given stringAndTimeCompare[A <: String | Option[String], B: DateTime]: CompareOperation[A, B]()

    given nothingCompare[B: AsSqlExpr]: CompareOperation[Nothing, B]()

    given tupleCompare[LH, LT <: Tuple, RH, RT <: Tuple](using CompareOperation[LH, RH], CompareOperation[LT, RT]): CompareOperation[LH *: LT, RH *: RT]()

    given tuple1Compare[LH, RH](using CompareOperation[LH, RH]): CompareOperation[LH *: EmptyTuple, RH *: EmptyTuple]()

    given valueAndTuple1Compare[A, B](using CompareOperation[A, B]): CompareOperation[A, Tuple1[B]]()

    given tuple1AndValueCompare[A, B](using CompareOperation[A, B]): CompareOperation[Tuple1[A], B]()

@implicitNotFound("Types ${A} and ${B} cannot be returned as results.")
trait ResultOperation[A, B]:
    type R

object ResultOperation:
    type Aux[A, B, O] = ResultOperation[A, B]:
        type R = O

    given result[A: AsSqlExpr]: Aux[A, A, A] = new ResultOperation[A, A]:
        type R = A

    given optionResult[A: AsSqlExpr]: Aux[A, Option[A], Option[A]] = new ResultOperation[A, Option[A]]:
        type R = Option[A]

    given valueResult[A: AsSqlExpr]: Aux[Option[A], A, Option[A]] = new ResultOperation[Option[A], A]:
        type R = Option[A]

    given numericResult[A: Number, B: Number]: Aux[A, B, Option[BigDecimal]] = new ResultOperation[A, B]:
        type R = Option[BigDecimal]

    given timeResulte[A: DateTime, B: DateTime]: Aux[A, B, Option[Date]] = new ResultOperation[A, B]:
        type R = Option[Date]

    given nothingResult[B: AsSqlExpr]: Aux[Nothing, B, B] = new ResultOperation[Nothing, B]:
        type R = B

@implicitNotFound("Types ${A} and ${B} cannot be UNION.")
trait UnionOperation[A, B]:
    type R

object UnionOperation:
    type Aux[A, B, O] = UnionOperation[A, B]:
        type R = O

    given union[A, K <: ExprKind, B, UK <: ExprKind](using r: ResultOperation[A, B]): Aux[Expr[A, K], Expr[B, UK], Expr[r.R, ColumnKind]] = new UnionOperation[Expr[A, K], Expr[B, UK]]:
        type R = Expr[r.R, ColumnKind]

    given tupleUnion[LH, LT <: Tuple, RH, RT <: Tuple](using h: UnionOperation[LH, RH], t: UnionOperation[LT, RT], tt: ToTuple[t.R]): Aux[LH *: LT, RH *: RT, h.R *: tt.R] = new UnionOperation[LH *: LT, RH *: RT]:
        type R = h.R *: tt.R

    given tuple1Union[LH, RH](using h: UnionOperation[LH, RH]): Aux[LH *: EmptyTuple, RH *: EmptyTuple, h.R *: EmptyTuple] = new UnionOperation[LH *: EmptyTuple, RH *: EmptyTuple]:
        type R = h.R *: EmptyTuple

    given namedTupleUnion[LN <: Tuple, LV <: Tuple, RN <: Tuple, RV <: Tuple](using u: UnionOperation[LV, RV], tt: ToTuple[u.R]): Aux[NamedTuple[LN, LV], NamedTuple[RN, RV], NamedTuple[LN, tt.R]] = new UnionOperation[NamedTuple[LN, LV], NamedTuple[RN, RV]]:
        type R = NamedTuple[LN, tt.R]

    given namedTupleAndTupleUnion[LN <: Tuple, LV <: Tuple, RV <: Tuple](using u: UnionOperation[LV, RV], tt: ToTuple[u.R]): Aux[NamedTuple[LN, LV], RV, NamedTuple[LN, tt.R]] = new UnionOperation[NamedTuple[LN, LV], RV]:
        type R = NamedTuple[LN, tt.R]

@implicitNotFound("Aggregate function or grouped column cannot be compared with non-aggregate function.")
trait KindOperation[A <: ExprKind, B <: ExprKind]

object KindOperation:
    inline def summonInstances[A <: ExprKind, T]: List[KindOperation[?, ?]] =
        inline erasedValue[T] match
            case _: EmptyTuple => Nil
            case _: (Expr[t, k] *: ts) => summonInline[KindOperation[A, k]] :: summonInstances[A, ts]

    given value[B <: ExprKind]: KindOperation[ValueKind, B]()

    given agg[A <: AggKind | AggOperationKind | GroupKind, B <: AggKind | AggOperationKind | GroupKind | ValueKind]: KindOperation[A, B]()

    given nonAgg[A <: CommonKind | ColumnKind | WindowKind | DistinctKind, B <: CommonKind | ColumnKind | WindowKind | ValueKind | DistinctKind]: KindOperation[A, B]()