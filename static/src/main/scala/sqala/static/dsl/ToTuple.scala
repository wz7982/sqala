package sqala.static.dsl

import scala.util.NotGiven

/**
 * Converts a type to a tuple. Existing tuples pass through unchanged;
 * non-tuple types are wrapped in a single-element tuple.
 */
trait ToTuple[T]:
    /**
     * The tuple representation of `T`.
     */
    type R <: Tuple

    /**
     * Converts the value to its tuple form.
     */
    def toTuple(x: T): R

object ToTuple:
    type Aux[T, O <: Tuple] = ToTuple[T]:
        type R = O

    given tupleToTuple[T <: Tuple]: Aux[T, T] = new ToTuple[T]:
        type R = T

        def toTuple(x: T): R =
            x

    given toTuple[T](using NotGiven[T <:< Tuple]): Aux[T, T *: EmptyTuple] = new ToTuple[T]:
        type R = T *: EmptyTuple

        def toTuple(x: T): R =
            x *: EmptyTuple