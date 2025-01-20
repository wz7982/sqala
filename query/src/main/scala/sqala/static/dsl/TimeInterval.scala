package sqala.static.dsl

import sqala.ast.expr.SqlTimeUnit

class TimeInterval(
    private[sqala] val value: Double, 
    private[sqala] val unit: SqlTimeUnit
)