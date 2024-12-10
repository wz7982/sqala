package sqala.static.statement.query

import sqala.static.common.AsSqlExpr

import scala.NamedTuple.NamedTuple
import scala.annotation.implicitNotFound

@implicitNotFound("Type ${T} cannot be converted to SELECT items.")
trait AsSelectItem[T]

object AsSelectItem:
    given exprAsSelectItem[T: AsSqlExpr]: AsSelectItem[T]()

    given tupleAsSelectItem[H, T <: Tuple](using 
        sh: AsSelectItem[H], 
        st: AsSelectItem[T]
    ): AsSelectItem[H *: T]()

    given tuple1AsSelectItem[H](using sh: AsSelectItem[H]): AsSelectItem[H *: EmptyTuple]()

    given namedTupleAsSelectItem[N <: Tuple, V <: Tuple](using
        s: AsSelectItem[V]
    ): AsSelectItem[NamedTuple[N, V]]()