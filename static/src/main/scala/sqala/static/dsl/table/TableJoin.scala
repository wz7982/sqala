package sqala.static.dsl.table

import sqala.static.dsl.{ToOption, ToTuple}

trait TableJoin[A, B]:
    type R

    def join(a: A, b: B): R

object TableJoin:
    type Aux[A, B, O] = TableJoin[A, B]:
        type R = O

    given join[A, B](using 
        ta: ToTuple[A], 
        tb: ToTuple[B]
    ): Aux[A, B, Tuple.Concat[ta.R, tb.R]] =
        new TableJoin[A, B]:
            type R = Tuple.Concat[ta.R, tb.R]

            def join(a: A, b: B): R =
                ta.toTuple(a) ++ tb.toTuple(b)

trait TableLeftJoin[A, B]:
    type R

    def join(a: A, b: B): R

object TableLeftJoin:
    type Aux[A, B, O] = TableLeftJoin[A, B]:
        type R = O

    given join[A, B](using 
        ta: ToTuple[A], 
        to: ToOption[B], 
        tb: ToTuple[to.R]
    ): Aux[A, B, Tuple.Concat[ta.R, tb.R]] =
        new TableLeftJoin[A, B]:
            type R = Tuple.Concat[ta.R, tb.R]

            def join(a: A, b: B): R =
                ta.toTuple(a) ++ tb.toTuple(to.toOption(b))

trait TableRightJoin[A, B]:
    type R

    def join(a: A, b: B): R

object TableRightJoin:
    type Aux[A, B, O] = TableRightJoin[A, B]:
        type R = O

    given join[A, B](using 
        to: ToOption[A], 
        ta: ToTuple[to.R], 
        tb: ToTuple[B]
    ): Aux[A, B, Tuple.Concat[ta.R, tb.R]] =
        new TableRightJoin[A, B]:
            type R = Tuple.Concat[ta.R, tb.R]

            def join(a: A, b: B): R =
                ta.toTuple(to.toOption(a)) ++ tb.toTuple(b)

trait TableFullJoin[A, B]:
    type R

    def join(a: A, b: B): R

object TableFullJoin:
    type Aux[A, B, O] = TableFullJoin[A, B]:
        type R = O

    given join[A, B](using 
        toa: ToOption[A], 
        ta: ToTuple[toa.R], 
        tob: ToOption[B], 
        tb: ToTuple[tob.R]
    ): Aux[A, B, Tuple.Concat[ta.R, tb.R]] =
        new TableFullJoin[A, B]:
            type R = Tuple.Concat[ta.R, tb.R]

            def join(a: A, b: B): R =
                ta.toTuple(toa.toOption(a)) ++ tb.toTuple(tob.toOption(b))