package sqala.dsl.statement.query

case class NamedTupleWrapper[N <: Tuple, V <: Tuple](values: V)