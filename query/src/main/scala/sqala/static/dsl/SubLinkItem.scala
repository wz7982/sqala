package sqala.static.dsl

import sqala.ast.expr.SqlSubLinkType
import sqala.ast.statement.SqlQuery

class SubLinkItem[T](
    private[sqala] val query: SqlQuery, 
    private[sqala] val linkType: SqlSubLinkType
)