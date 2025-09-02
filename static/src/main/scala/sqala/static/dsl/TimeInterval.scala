package sqala.static.dsl

import sqala.ast.expr.SqlTimeUnit

class TimeInterval(
    private[sqala] val value: String, 
    private[sqala] val unit: SqlTimeUnit
)