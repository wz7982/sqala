package sqala.static.dsl

import sqala.ast.expr.SqlSubLinkQuantifier
import sqala.ast.statement.SqlQuery

class SubLink[T](
    private[sqala] val tree: SqlQuery,
    private[sqala] val quantifier: SqlSubLinkQuantifier
)