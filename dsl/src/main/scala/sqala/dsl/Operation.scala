package sqala.dsl

import java.util.Date
import scala.annotation.implicitNotFound
import scala.compiletime.{erasedValue, summonInline}

@implicitNotFound("Types ${A} and ${B} be cannot compared")
trait Operation[A, B]:
    type R

object Operation:
    inline def summonInstances[A, T]: List[Operation[?, ?]] =
        inline erasedValue[T] match
            case _: EmptyTuple => Nil
            case _: (Expr[t, k] *: ts) => summonInline[Operation[A, t]] :: summonInstances[A, ts]

    type Aux[A, B, O] = Operation[A, B] { type R = O }

    given compare[A]: Aux[A, A, A] = new Operation[A, A]:
        type R = A

    given compareOption[A]: Aux[A, Option[A], Option[A]] = new Operation[A, Option[A]]:
        type R = Option[A]

    given compreaValue[A]: Aux[Option[A], A, Option[A]] = new Operation[Option[A], A]:
        type R = Option[A]

    given numericCompare[A: Number, B: Number]: Aux[A, B, Option[BigDecimal]] = new Operation[A, B]:
        type R = Option[BigDecimal]

    given timeCompare[A: DateTime, B: DateTime]: Aux[A, B, Option[Date]] = new Operation[A, B]:
        type R = Option[Date]

    given timeAndStringCompare[A: DateTime, B <: String | Option[String]]: Aux[A, B, Option[Date]] = new Operation[A, B]:
        type R = Option[Date]
    
    given stringAndTimeCompare[A <: String | Option[String], B: DateTime]: Aux[A, B, Option[Date]] = new Operation[A, B]:
        type R = Option[Date]

    given nothingCompare[B]: Aux[Nothing, B, B] = new Operation[Nothing, B]:
        type R = B

@implicitNotFound("Aggregate function or grouped column cannot be compared with non-aggregate function")
trait OperationKind[A <: ExprKind, B <: ExprKind]

object OperationKind:
    inline def summonInstances[A <: ExprKind, T]: List[OperationKind[?, ?]] =
        inline erasedValue[T] match
            case _: EmptyTuple => Nil
            case _: (Expr[t, k] *: ts) => summonInline[OperationKind[A, k]] :: summonInstances[A, ts]

    given value[B <: ExprKind]: OperationKind[ValueKind, B]()

    given agg[A <: AggKind | AggOperationKind | GroupKind, B <: AggKind | AggOperationKind | GroupKind | ValueKind]: OperationKind[A, B]()

    given nonAgg[A <: CommonKind | ColumnKind | WindowKind, B <: CommonKind | ColumnKind | WindowKind | ValueKind]: OperationKind[A, B]()