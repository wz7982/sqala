package sqala.static.dsl

import scala.util.NotGiven

trait ToTuple[T]:
    type R <: Tuple

    def toTuple(x: T): R

object ToTuple:
    type Aux[T, O <: Tuple] = ToTuple[T]:
        type R = O

    given tupleToTuple[T <: Tuple]: Aux[T, T] = new ToTuple[T]:
        type R = T

        def toTuple(x: T): R = x

    given toTuple[T](using NotGiven[T <:< Tuple]): Aux[T, T *: EmptyTuple] = new ToTuple[T]:
        type R = T *: EmptyTuple

        def toTuple(x: T): R = x *: EmptyTuple