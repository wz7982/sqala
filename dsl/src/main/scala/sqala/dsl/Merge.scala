package sqala.dsl

import scala.annotation.implicitNotFound

@implicitNotFound("The type ${T} cannot be merged into a SQL expression")
trait Merge[T]:
    type R

    type K <: CompositeKind

    def asExpr(x: T): Expr[R, K]

object Merge:
    transparent inline given mergeExpr[T, EK <: ExprKind]: Merge[Expr[T, EK]] =
        new Merge[Expr[T, EK]]:
            type R = T

            type K = ResultKind[EK, ValueKind]

            def asExpr(x: Expr[T, EK]): Expr[R, K] =
                x.asInstanceOf[Expr[R, K]]

    transparent inline given mergeTuple[H, EK <: ExprKind, T <: Tuple](using t: Merge[T], k: KindOperation[EK, t.K]): Merge[Expr[H, EK] *: T] =
        new Merge[Expr[H, EK] *: T]:
            type R = H *: ToTuple[t.R]

            type K = ResultKind[EK, t.K]

            def asExpr(x: Expr[H, EK] *: T): Expr[R, K] =
                val head = x.head
                val tail = t.asExpr(x.tail).asInstanceOf[Expr.Vector[?, ?]]
                Expr.Vector(head :: tail.items)

    transparent inline given mergeTuple1[H, EK <: ExprKind]: Merge[Expr[H, EK] *: EmptyTuple] =
        new Merge[Expr[H, EK] *: EmptyTuple]:
            type R = H *: EmptyTuple

            type K = ResultKind[EK, ValueKind]

            def asExpr(x: Expr[H, EK] *: EmptyTuple): Expr[R, K] =
                val head = x.head
                Expr.Vector(head :: Nil)