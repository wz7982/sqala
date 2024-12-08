package sqala.static.common

import scala.annotation.implicitNotFound

@implicitNotFound("Type ${T} cannot be converted to SQL expressions.")
trait AsExpr[T]

object AsExpr:
    given exprAsExpr[T: AsSqlExpr]: AsExpr[T]()

    given tupleAsExpr[X: AsSqlExpr, T <: Tuple](using
        h: AsExpr[X],
        t: AsExpr[T]
    ): AsExpr[X *: T]()

    given tuple1AsExpr[X: AsSqlExpr](using
        h: AsExpr[X]
    ): AsExpr[X *: EmptyTuple]()