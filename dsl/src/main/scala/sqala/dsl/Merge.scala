package sqala.dsl

import scala.annotation.implicitNotFound

@implicitNotFound("The type ${T} cannot be merged into a SQL expression")
trait Merge[T]:
    type R

    def asExpr(x: T): Expr[R, CommonKind]

object Merge:
    transparent inline given mergeExpr[T, K <: SimpleKind]: Merge[Expr[T, K]] =
        new Merge[Expr[T, K]]:
            type R = T

            def asExpr(x: Expr[T, K]): Expr[R, CommonKind] = x.asInstanceOf[Expr[R, CommonKind]]

    transparent inline given mergeTuple[H, K <: SimpleKind, T <: Tuple](using t: Merge[T]): Merge[Expr[H, K] *: T] =
        new Merge[Expr[H, K] *: T]:
            type R = H *: ToTuple[t.R]

            def asExpr(x: Expr[H, K] *: T): Expr[R, CommonKind] =
                val head = x.head
                val tail = t.asExpr(x.tail).asInstanceOf[Expr.Vector[?]]
                Expr.Vector(head :: tail.items)

    transparent inline given mergeEmptyTuple: Merge[EmptyTuple] =
        new Merge[EmptyTuple]:
            type R = EmptyTuple

            def asExpr(x: EmptyTuple): Expr[R, CommonKind] = Expr.Vector(Nil)