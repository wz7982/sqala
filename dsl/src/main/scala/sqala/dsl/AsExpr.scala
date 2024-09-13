package sqala.dsl

import scala.NamedTuple.NamedTuple
import scala.annotation.implicitNotFound

@implicitNotFound("Type ${T} cannot be converted to SQL expressions")
trait AsExpr[T]:
    def asExprs(item: T): List[Expr[?, ?]]

object AsExpr:
    given exprAsExpr[T, K <: ExprKind]: AsExpr[Expr[T, K]] with
        override def asExprs(item: Expr[T, K]): List[Expr[?, ?]] = item :: Nil
    
    given tupleAsExpr[H, T <: Tuple](using h: AsExpr[H], t: AsExpr[T]): AsExpr[H *: T] with
        override def asExprs(item: H *: T): List[Expr[?, ?]] = h.asExprs(item.head) ++ t.asExprs(item.tail)

    given emptyTupleAsExpr: AsExpr[EmptyTuple] with
        override def asExprs(item: EmptyTuple): List[Expr[?, ?]] = Nil

    given namedTupleAsExpr[N <: Tuple, V <: Tuple](using a: AsExpr[V]): AsExpr[NamedTuple[N, V]] with
        override def asExprs(item: NamedTuple[N, V]): List[Expr[?, ?]] = a.asExprs(item.toTuple)