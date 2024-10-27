package sqala.dsl

import scala.NamedTuple.NamedTuple
import scala.annotation.implicitNotFound

@implicitNotFound("Type ${T} cannot be converted to SQL expressions.")
trait AsExpr[T]:
    def asExprs(x: T): List[Expr[?, ?]]

object AsExpr:
    given exprAsExpr[T, K <: ExprKind]: AsExpr[Expr[T, K]] with
        def asExprs(x: Expr[T, K]): List[Expr[?, ?]] = x :: Nil
    
    given tupleAsExpr[X, K <: ExprKind, T <: Tuple](using 
        h: AsExpr[Expr[X, K]], 
        t: AsExpr[T]
    ): AsExpr[Expr[X, K] *: T] with
        def asExprs(x: Expr[X, K] *: T): List[Expr[?, ?]] = h.asExprs(x.head) ++ t.asExprs(x.tail)

    given tuple1AsExpr[X, K <: ExprKind](using h: AsExpr[Expr[X, K]]): AsExpr[Expr[X, K] *: EmptyTuple] with
        def asExprs(x: Expr[X, K] *: EmptyTuple): List[Expr[?, ?]] = h.asExprs(x.head)

    given namedTupleAsExpr[N <: Tuple, V <: Tuple](using a: AsExpr[V]): AsExpr[NamedTuple[N, V]] with
        def asExprs(x: NamedTuple[N, V]): List[Expr[?, ?]] = a.asExprs(x.toTuple)