package sqala.ast.group

import sqala.ast.quantifier.SqlQuantifier

case class SqlGroupBy(items: List[SqlGroupingItem], quantifier: Option[SqlQuantifier])