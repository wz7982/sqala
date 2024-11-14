package sqala.dsl

import scala.annotation.implicitNotFound

@implicitNotFound("Type ${T} cannot be converted to SQL expressions.")
trait AsExpr[T]:
    def asExprs(x: T): List[Expr[?]]

object AsExpr:
    given exprAsExpr[T: AsSqlExpr]: AsExpr[Expr[T]] with
        def asExprs(x: Expr[T]): List[Expr[?]] =
            x :: Nil

    given tupleAsExpr[X: AsSqlExpr, T <: Tuple](using
        h: AsExpr[Expr[X]],
        t: AsExpr[T]
    ): AsExpr[Expr[X] *: T] with
        def asExprs(x: Expr[X] *: T): List[Expr[?]] =
            h.asExprs(x.head) ++ t.asExprs(x.tail)

    given tuple1AsExpr[X: AsSqlExpr](using
        h: AsExpr[Expr[X]]
    ): AsExpr[Expr[X] *: EmptyTuple] with
        def asExprs(x: Expr[X] *: EmptyTuple): List[Expr[?]] =
            h.asExprs(x.head)