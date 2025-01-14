package sqala.static.dsl

import scala.annotation.implicitNotFound

@implicitNotFound("Type ${T} cannot be converted to SQL expressions.")
trait AsExpr[T]:
    def asExprs(x: T): List[Expr[?]]

object AsExpr:
    given exprAsExpr[T: AsSqlExpr]: AsExpr[Expr[T]] with
        def asExprs(x: Expr[T]): List[Expr[?]] =
            x :: Nil

    given valueAsExpr[T: AsSqlExpr as a]: AsExpr[T] with
        def asExprs(x: T): List[Expr[?]] =
            Expr.Literal(x, a) :: Nil

    given tupleAsExpr[X: AsExpr, T <: Tuple](using
        h: AsExpr[X],
        t: AsExpr[T]
    ): AsExpr[X *: T] with
        def asExprs(x: X *: T): List[Expr[?]] =
            h.asExprs(x.head) ++ t.asExprs(x.tail)

    given tuple1AsExpr[X: AsExpr](using
        h: AsExpr[X]
    ): AsExpr[X *: EmptyTuple] with
        def asExprs(x: X *: EmptyTuple): List[Expr[?]] =
            h.asExprs(x.head)