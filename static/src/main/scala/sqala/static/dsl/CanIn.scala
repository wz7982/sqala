package sqala.static.dsl

trait CanIn[-L, -R <: Tuple]

object CanIn:
    given canInTuple[L, H, T <: Tuple](using CanEqual[L, H], CanIn[L, T]): CanIn[L, H *: T]()

    given canInEmptyTuple[L]: CanIn[L, EmptyTuple]()