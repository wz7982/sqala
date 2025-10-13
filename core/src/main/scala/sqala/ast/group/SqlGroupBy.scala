package sqala.ast.group

import sqala.ast.quantifier.SqlQuantifier

case class SqlGroupBy(quantifier: Option[SqlQuantifier], items: List[SqlGroupingItem])