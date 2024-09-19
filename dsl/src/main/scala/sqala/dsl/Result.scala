package sqala.dsl

trait Result[T]:
    type R

object Result:
    transparent inline given tableResult[X, T <: Table[X]]: Result[T] = new Result[T]:
        type R = X

    transparent inline given exprResult[T, K <: ExprKind]: Result[Expr[T, K]] = new Result[Expr[T, K]]:
        type R = T

    transparent inline given tupleResult[H, T <: Tuple](using hr: Result[H], tr: Result[T]): Result[H *: T] = 
        new Result[H *: T]:
            type R = hr.R *: ToTuple[tr.R]

    transparent inline given tuple1Result[H](using hr: Result[H]): Result[H *: EmptyTuple] = 
        new Result[H *: EmptyTuple]:
            type R = hr.R *: EmptyTuple