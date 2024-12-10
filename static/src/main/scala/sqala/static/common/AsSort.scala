package sqala.static.common

import scala.annotation.implicitNotFound

@implicitNotFound("Type ${T} cannot be converted to SQL expressions.")
trait AsSort[T]

object AsSort:
    given exprAsSort[T: AsSqlExpr]: AsSort[T]()

    given sortAsSort[T]: AsSort[Sort[T]]()

    given tupleAsSort[X, T <: Tuple](using
        h: AsSort[X],
        t: AsSort[T]
    ): AsSort[X *: T]()

    given tuple1AsSort[X](using
        h: AsSort[X]
    ): AsSort[X *: EmptyTuple]()