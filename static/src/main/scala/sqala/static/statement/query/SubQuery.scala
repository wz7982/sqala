package sqala.static.statement.query

import sqala.static.common.*

import scala.NamedTuple.*

class SubQuery[N <: Tuple, V <: Tuple](
    private[sqala] val __columns__ : List[String]
) extends Selectable:
    type Fields = NamedTuple[N, V]

    def selectDynamic(name: String): Any = compileTimeOnly