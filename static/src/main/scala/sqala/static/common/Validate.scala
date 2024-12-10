package sqala.static.common

import scala.annotation.implicitNotFound

@implicitNotFound("${E}")
trait Validate[P <: Boolean, E <: String]

object Validate:
    given [E <: String]: Validate[true, E]()