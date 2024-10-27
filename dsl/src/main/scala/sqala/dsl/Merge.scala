package sqala.dsl

import scala.NamedTuple.NamedTuple
import scala.annotation.implicitNotFound

@implicitNotFound("The type ${T} cannot be merged into a SQL expression.")
trait Merge[T]:
    type R

    type K <: CompositeKind

    def asExpr(x: T): Expr[R, K]

object Merge:
    type Aux[T, OR, OK <: CompositeKind] = Merge[T]:
        type R = OR

        type K = OK

    given mergeExpr[T: AsSqlExpr, EK <: ExprKind]: Aux[Expr[T, EK], T, ResultKind[EK, ValueKind]] =
        new Merge[Expr[T, EK]]:
            type R = T

            type K = ResultKind[EK, ValueKind]

            def asExpr(x: Expr[T, EK]): Expr[R, K] =
                x.asInstanceOf[Expr[R, K]]

    given mergeTuple[H: AsSqlExpr, EK <: ExprKind, T <: Tuple](using t: Merge[T], k: KindOperation[EK, t.K]): Aux[Expr[H, EK] *: T, H *: ToTuple[t.R], ResultKind[EK, t.K]] =
        new Merge[Expr[H, EK] *: T]:
            type R = H *: ToTuple[t.R]

            type K = ResultKind[EK, t.K]

            def asExpr(x: Expr[H, EK] *: T): Expr[R, K] =
                val head = x.head
                val tail = t.asExpr(x.tail).asInstanceOf[Expr.Vector[?, ?]]
                Expr.Vector(head :: tail.items)

    given mergeTuple1[H: AsSqlExpr, EK <: ExprKind]: Aux[Expr[H, EK] *: EmptyTuple, H, ResultKind[EK, ValueKind]] =
        new Merge[Expr[H, EK] *: EmptyTuple]:
            type R = H

            type K = ResultKind[EK, ValueKind]

            def asExpr(x: Expr[H, EK] *: EmptyTuple): Expr[R, K] =
                val head = x.head
                Expr.Vector(head :: Nil)

    given mergeNamedTuple[N <: Tuple, V <: Tuple](using m: Merge[V]): Aux[NamedTuple[N, V], m.R, m.K] =
        new Merge[NamedTuple[N, V]]:
            type R = m.R

            type K = m.K

            def asExpr(x: NamedTuple[N, V]): Expr[R, K] =
                m.asExpr(x.toTuple)