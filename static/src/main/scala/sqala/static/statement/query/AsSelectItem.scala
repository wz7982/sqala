package sqala.static.statement.query

import sqala.static.common.AsSqlExpr

import scala.NamedTuple.NamedTuple
import scala.annotation.implicitNotFound

@implicitNotFound("Type ${T} cannot be converted to SELECT clause.")
trait AsSelectItem[T]

object AsSelectItem:
    given expr[T: AsSqlExpr]: AsSelectItem[T]()

    given scalarQuery[T: AsSqlExpr]: AsSelectItem[Query[T, OneRow]]()

    given tuple[H, T <: Tuple](using 
        sh: AsSelectItem[H], 
        st: AsSelectItem[T]
    ): AsSelectItem[H *: T]()

    given tuple1[H](using sh: AsSelectItem[H]): AsSelectItem[H *: EmptyTuple]()

    given namedTuple[N <: Tuple, V <: Tuple](using
        s: AsSelectItem[V]
    ): AsSelectItem[NamedTuple[N, V]]()