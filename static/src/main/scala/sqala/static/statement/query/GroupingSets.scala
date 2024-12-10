package sqala.static.statement.query

import sqala.static.common.AsSqlExpr

import scala.annotation.implicitNotFound

@implicitNotFound("Type ${T} cannot be converted to GROUPING SETS expressions.")
trait GroupingSetsItem[T]

object GroupingSetsItem:
    given exprGrouping[T: AsSqlExpr]: GroupingSetsItem[T]()

    given tupleGrouping[T: AsSqlExpr, Tail <: Tuple](using
        GroupingSetsItem[T],
        GroupingSetsItem[Tail]
    ): GroupingSetsItem[T *: Tail]()

    given emptyTupleGrouping: GroupingSetsItem[EmptyTuple]()

    given unitGrouping: GroupingSetsItem[Unit]()

@implicitNotFound("Type ${T} cannot be converted to GROUPING SETS expressions.")
trait GroupingSets[T]

object GroupingSets:
    given exprGrouping[T: AsSqlExpr]: GroupingSets[T]()

    given tupleGrouping[H, T <: Tuple](using
        GroupingSetsItem[H],
        GroupingSets[T]
    ): GroupingSets[H *: T]()

    given tuple1Grouping[H](using GroupingSetsItem[H]): GroupingSets[H *: EmptyTuple]()