package sqala.static.dsl

import sqala.ast.expr.SqlSubLinkQuantifier
import sqala.ast.statement.SqlQuery

class SubLink[T](
    private[sqala] val quantifier: SqlSubLinkQuantifier,
    private[sqala] val tree: SqlQuery
)