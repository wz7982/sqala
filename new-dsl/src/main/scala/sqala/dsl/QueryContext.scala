package sqala.dsl

case class QueryContext(val tableIndex: Int, val outerContainers: List[(String, Container)])