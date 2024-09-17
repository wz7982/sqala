package sqala.dsl

import scala.annotation.implicitNotFound

@implicitNotFound("The type ${T} cannot be merged into a SQL expression")
trait Merge[T]:
    type R

    type K <: CompositeKind | ValueKind

    def asExpr(x: T): Expr[R, K]

object Merge:
    transparent inline given mergeTuple[H, EK <: ExprKind, T <: Tuple](using t: Merge[T], k: KindOperation[EK, t.K]): Merge[Expr[H, EK] *: T] =
        new Merge[Expr[H, EK] *: T]:
            type R = H *: ToTuple[t.R]

            type K = ResultKind[EK, t.K]

            def asExpr(x: Expr[H, EK] *: T): Expr[R, K] =
                val head = x.head
                val tail = t.asExpr(x.tail).asInstanceOf[Expr.Vector[?, ?]]
                Expr.Vector(head :: tail.items)

    transparent inline given mergeEmptyTuple: Merge[EmptyTuple] =
        new Merge[EmptyTuple]:
            type R = EmptyTuple

            type K = ValueKind

            def asExpr(x: EmptyTuple): Expr[R, K] = Expr.Vector(Nil)