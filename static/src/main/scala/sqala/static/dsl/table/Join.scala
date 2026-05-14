package sqala.static.dsl.table

import sqala.static.dsl.{ToOption, ToTuple}

trait Join[A, B]:
    type R

    def join(a: A, b: B): R

object Join:
    type Aux[A, B, O] = Join[A, B]:
        type R = O

    given join[A, B](using
        ta: ToTuple[A],
        tb: ToTuple[B]
    ): Aux[A, B, Tuple.Concat[ta.R, tb.R]] =
        new Join[A, B]:
            type R = Tuple.Concat[ta.R, tb.R]

            def join(a: A, b: B): R =
                ta.toTuple(a) ++ tb.toTuple(b)

trait LeftJoin[A, B]:
    type R

    def join(a: A, b: B): R

object LeftJoin:
    type Aux[A, B, O] = LeftJoin[A, B]:
        type R = O

    given join[A, B](using
        ta: ToTuple[A],
        to: ToOption[B],
        tb: ToTuple[to.R]
    ): Aux[A, B, Tuple.Concat[ta.R, tb.R]] =
        new LeftJoin[A, B]:
            type R = Tuple.Concat[ta.R, tb.R]

            def join(a: A, b: B): R =
                ta.toTuple(a) ++ tb.toTuple(to.toOption(b))

trait RightJoin[A, B]:
    type R

    def join(a: A, b: B): R

object RightJoin:
    type Aux[A, B, O] = RightJoin[A, B]:
        type R = O

    given join[A, B](using
        to: ToOption[A],
        ta: ToTuple[to.R],
        tb: ToTuple[B]
    ): Aux[A, B, Tuple.Concat[ta.R, tb.R]] =
        new RightJoin[A, B]:
            type R = Tuple.Concat[ta.R, tb.R]

            def join(a: A, b: B): R =
                ta.toTuple(to.toOption(a)) ++ tb.toTuple(b)

trait FullJoin[A, B]:
    type R

    def join(a: A, b: B): R

object FullJoin:
    type Aux[A, B, O] = FullJoin[A, B]:
        type R = O

    given join[A, B](using
        toa: ToOption[A],
        ta: ToTuple[toa.R],
        tob: ToOption[B],
        tb: ToTuple[tob.R]
    ): Aux[A, B, Tuple.Concat[ta.R, tb.R]] =
        new FullJoin[A, B]:
            type R = Tuple.Concat[ta.R, tb.R]

            def join(a: A, b: B): R =
                ta.toTuple(toa.toOption(a)) ++ tb.toTuple(tob.toOption(b))