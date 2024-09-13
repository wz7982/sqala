package sqala.dsl

import scala.annotation.implicitNotFound
import scala.compiletime.{erasedValue, summonInline}

@implicitNotFound("Types ${A} and ${B} be cannot compared")
trait Operation[A, B]

object Operation:
    inline def summonInstances[A, T]: List[Operation[?, ?]] =
        inline erasedValue[T] match
            case _: EmptyTuple => Nil
            case _: (Expr[t, k] *: ts) => summonInline[Operation[A, t]] :: summonInstances[A, ts]

    given compare[A]: Operation[A, A]()

    given compareOption[A]: Operation[A, Option[A]]()

    given compreaValue[A]: Operation[Option[A], A]()

    given numericCompare[A: Number, B: Number]: Operation[A, B]()

    given timeCompare[A: DateTime, B: DateTime]: Operation[A, B]()

    given timeAndStringCompare[A: DateTime, B <: String | Option[String]]: Operation[A, B]()

    given stringAndTimeCompare[A <: String | Option[String], B: DateTime]: Operation[A, B]()

    given nothingCompare[B]: Operation[Nothing, B]()

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