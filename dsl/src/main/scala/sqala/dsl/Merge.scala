package sqala.dsl

import scala.NamedTuple.NamedTuple
import scala.annotation.implicitNotFound

@implicitNotFound("The type ${T} cannot be merged into a SQL expression.")
trait Merge[T]:
    type R

    type K <: CompositeKind

    def exprs(x: T): List[Expr[?, ?]]

    def asExpr(x: T): Expr[R, K] = 
        val exprList = exprs(x)
        if exprList.size == 1 then Expr.Ref(exprList.head)
        else Expr.Vector(exprList)

object Merge:
    type Aux[T, OR, OK <: CompositeKind] = Merge[T]:
        type R = OR

        type K = OK

    given mergeExpr[T: AsSqlExpr, EK <: SimpleKind]: Aux[Expr[T, EK], T, ResultKind[EK, ValueKind]] =
        new Merge[Expr[T, EK]]:
            type R = T

            type K = ResultKind[EK, ValueKind]

            def exprs(x: Expr[T, EK]): List[Expr[?, ?]] =
                x :: Nil

    given mergeTuple[H: AsSqlExpr, EK <: SimpleKind, T <: Tuple](using 
        t: Merge[T], 
        k: KindOperation[EK, t.K], 
        tt: ToTuple[t.R]
    ): Aux[Expr[H, EK] *: T, H *: tt.R, ResultKind[EK, ValueKind]] =
        new Merge[Expr[H, EK] *: T]:
            type R = H *: tt.R

            type K = ResultKind[EK, ValueKind]

            def exprs(x: Expr[H, EK] *: T): List[Expr[?, ?]] =
                x.head :: t.exprs(x.tail)

    given mergeTuple1[H: AsSqlExpr, EK <: SimpleKind]: Aux[Expr[H, EK] *: EmptyTuple, H, ResultKind[EK, ValueKind]] =
        new Merge[Expr[H, EK] *: EmptyTuple]:
            type R = H

            type K = ResultKind[EK, ValueKind]

            def exprs(x: Expr[H, EK] *: EmptyTuple): List[Expr[?, ?]] =
                x.head :: Nil

    given mergeNamedTuple[N <: Tuple, V <: Tuple](using m: Merge[V]): Aux[NamedTuple[N, V], m.R, m.K] =
        new Merge[NamedTuple[N, V]]:
            type R = m.R

            type K = m.K

            def exprs(x: NamedTuple[N, V]): List[Expr[?, ?]] =
                m.exprs(x.toTuple)