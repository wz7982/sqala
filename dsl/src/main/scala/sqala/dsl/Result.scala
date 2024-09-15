package sqala.dsl

trait Result[T]:
    type R

object Result:
    transparent inline given tableResult[T]: Result[Table[T]] = new Result[Table[T]]:
        type R = T

    transparent inline given exprResult[T, K <: ExprKind]: Result[Expr[T, K]] = new Result[Expr[T, K]]:
        type R = T

    transparent inline given tupleResult[H, T <: Tuple](using hr: Result[H], tr: Result[T]): Result[H *: T] = 
        new Result[H *: T]:
            type R = hr.R *: ToTuple[tr.R]

    transparent inline given emptyTupleResult: Result[EmptyTuple] = new Result[EmptyTuple]:
        type R = EmptyTuple