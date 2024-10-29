package sqala.dsl

import java.util.Date
import scala.NamedTuple.NamedTuple
import scala.annotation.implicitNotFound
import scala.compiletime.{erasedValue, summonInline}

@implicitNotFound("The type ${T} cannot be converted to SQL expression.")
trait ComparableValue[T]:
    def exprs(x: T): List[Expr[?, ?]]

    def asExpr(x: T): Expr[?, ?] =
        val exprList = exprs(x)
        if exprList.size == 1 then Expr.Ref(exprList.head)
        else Expr.Vector(exprList)

object ComparableValue:
    given valueAsExpr[T](using a: AsSqlExpr[T]): ComparableValue[T] with
        def exprs(x: T): List[Expr[?, ?]] =
            Expr.Literal(x, a) :: Nil

    given tupleAsExpr[H, T <: Tuple](using h: AsSqlExpr[H], t: ComparableValue[T]): ComparableValue[H *: T] with
        def exprs(x: H *: T): List[Expr[?, ?]] =
            val head = Expr.Literal(x.head, h)
            val tail = t.exprs(x.tail)
            head :: tail

    given tuple1AsExpr[H](using h: AsSqlExpr[H]): ComparableValue[H *: EmptyTuple] with
        def exprs(x: H *: EmptyTuple): List[Expr[?, ?]] =
            val head = Expr.Literal(x.head, h)
            head :: Nil

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

    given result[A]: Aux[A, A, A] = 
        new ResultOperation[A, A]:
            type R = A

    given optionResult[A: AsSqlExpr]: Aux[A, Option[A], Option[A]] = 
        new ResultOperation[A, Option[A]]:
            type R = Option[A]

    given valueResult[A: AsSqlExpr]: Aux[Option[A], A, Option[A]] = 
        new ResultOperation[Option[A], A]:
            type R = Option[A]

    given numericResult[A: Number, B: Number]: Aux[A, B, NumericResult[A, B]] = 
        new ResultOperation[A, B]:
            type R = NumericResult[A, B]

    given timeResulte[A: DateTime, B: DateTime]: Aux[A, B, Option[Date]] = 
        new ResultOperation[A, B]:
            type R = Option[Date]

    given nothingResult[B: AsSqlExpr]: Aux[Nothing, B, B] = 
        new ResultOperation[Nothing, B]:
            type R = B

@implicitNotFound("Types ${A} and ${B} cannot be UNION.")
trait UnionOperation[A, B]:
    type R

    def unionQueryItems(x: A): R

object UnionOperation:
    type Aux[A, B, O] = UnionOperation[A, B]:
        type R = O

    given union[A, K <: ExprKind, B, UK <: ExprKind](using 
        r: ResultOperation[A, B]
    ): Aux[Expr[A, K], Expr[B, UK], Expr[r.R, CommonKind]] = 
        new UnionOperation[Expr[A, K], Expr[B, UK]]:
            type R = Expr[r.R, CommonKind]

            def unionQueryItems(x: Expr[A, K]): R =
                Expr.Ref(x)

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

    given namedTupleUnion[LN <: Tuple, LV <: Tuple, RN <: Tuple, RV <: Tuple](using 
        u: UnionOperation[LV, RV], 
        tt: ToTuple[u.R]
    ): Aux[NamedTuple[LN, LV], NamedTuple[RN, RV], NamedTuple[LN, tt.R]] = 
        new UnionOperation[NamedTuple[LN, LV], NamedTuple[RN, RV]]:
            type R = NamedTuple[LN, tt.R]

            def unionQueryItems(x: NamedTuple[LN, LV]): R =
                NamedTuple(tt.toTuple(u.unionQueryItems(x.toTuple)))

    given namedTupleUnionTuple[LN <: Tuple, LV <: Tuple, RV <: Tuple](using 
        u: UnionOperation[LV, RV], 
        tt: ToTuple[u.R]
    ): Aux[NamedTuple[LN, LV], RV, NamedTuple[LN, tt.R]] = 
        new UnionOperation[NamedTuple[LN, LV], RV]:
            type R = NamedTuple[LN, tt.R]

            def unionQueryItems(x: NamedTuple[LN, LV]): R =
                NamedTuple(tt.toTuple(u.unionQueryItems(x.toTuple)))

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