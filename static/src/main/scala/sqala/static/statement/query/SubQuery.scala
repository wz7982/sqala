package sqala.static.statement.query

import sqala.static.common.compileTimeOnly

import scala.NamedTuple.*
import scala.compiletime.constValue

class SubQuery[N <: Tuple, V <: Tuple](private[sqala] val __columns__ : List[String]) extends Selectable:
    type Fields = NamedTuple[N, V]

    inline def selectDynamic(name: String): Any = compileTimeOnly