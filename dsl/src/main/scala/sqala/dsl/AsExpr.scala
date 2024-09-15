package sqala.dsl

import scala.annotation.implicitNotFound

@implicitNotFound("Type ${T} cannot be converted to SQL expressions")
trait AsExpr[T]:
    def asExprs(x: T): List[Expr[?, ?]]

object AsExpr:
    given exprAsExpr[T, K <: ExprKind]: AsExpr[Expr[T, K]] with
        def asExprs(x: Expr[T, K]): List[Expr[?, ?]] = x :: Nil
    
    given tupleAsExpr[H, T <: Tuple](using h: AsExpr[H], t: AsExpr[T]): AsExpr[H *: T] with
        def asExprs(x: H *: T): List[Expr[?, ?]] = h.asExprs(x.head) ++ t.asExprs(x.tail)

    given emptyTupleAsExpr: AsExpr[EmptyTuple] with
        def asExprs(x: EmptyTuple): List[Expr[?, ?]] = Nil