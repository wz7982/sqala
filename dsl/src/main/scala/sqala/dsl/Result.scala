package sqala.dsl

import sqala.dsl.statement.query.SubQuery

import scala.NamedTuple.NamedTuple
import scala.annotation.nowarn

trait Result[T]:
    type R

object Result:
    @nowarn("msg=New anonymous class definition will be duplicated at each inline site")
    transparent inline given tableResult[X, T <: Table[X]]: Result[T] = new Result[T]:
        type R = X

    @nowarn("msg=New anonymous class definition will be duplicated at each inline site")
    transparent inline given exprResult[T, K <: ExprKind]: Result[Expr[T, K]] = new Result[Expr[T, K]]:
        type R = T

    @nowarn("msg=New anonymous class definition will be duplicated at each inline site")
    transparent inline given tupleResult[H, T <: Tuple](using hr: Result[H], tr: Result[T]): Result[H *: T] = 
        new Result[H *: T]:
            type R = hr.R *: ToTuple[tr.R]

    @nowarn("msg=New anonymous class definition will be duplicated at each inline site")
    transparent inline given tuple1Result[H](using hr: Result[H]): Result[H *: EmptyTuple] = 
        new Result[H *: EmptyTuple]:
            type R = hr.R *: EmptyTuple

    @nowarn("msg=New anonymous class definition will be duplicated at each inline site")
    transparent inline given namedTupleResult[N <: Tuple, V <: Tuple](using r: Result[V]): Result[NamedTuple[N, V]] =
        new Result[NamedTuple[N, V]]:
            type R = NamedTuple[N, ToTuple[r.R]]

    @nowarn("msg=New anonymous class definition will be duplicated at each inline site")
    transparent inline given subQueryResult[N <: Tuple, V <: Tuple](using r: Result[V]): Result[SubQuery[N, V]] =
        new Result[SubQuery[N, V]]:
            type R = NamedTuple[N, ToTuple[r.R]]