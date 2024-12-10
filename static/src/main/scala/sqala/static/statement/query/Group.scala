package sqala.static.statement.query

import sqala.static.common.compileTimeOnly

import scala.NamedTuple.NamedTuple

class Group[N <: Tuple, V <: Tuple] extends Selectable:
    type Fields = NamedTuple[N, V]

    def selectDynamic(name: String): Any =
        compileTimeOnly