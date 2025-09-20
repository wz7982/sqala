package sqala.static.dsl.statement.query

sealed trait QuerySize
trait OneOrZero extends QuerySize
trait Many extends QuerySize