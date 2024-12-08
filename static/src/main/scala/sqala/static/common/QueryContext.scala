package sqala.static.common

import sqala.ast.expr.SqlExpr

import scala.collection.mutable.ListBuffer

class QueryContext(
    val groups: ListBuffer[(String, List[SqlExpr])] = ListBuffer()
)