package sqala.static.dsl.statement.query

import sqala.ast.expr.SqlExpr
import sqala.ast.group.SqlGroupingItem

/**
 * A cube specification used by `groupBy` and `groupingSets` clauses.
 */
final case class Cube(private[sqala] val exprs: List[SqlExpr])

/**
 * A rollup specification used by `groupBy` and `groupingSets` clauses.
 */
final case class Rollup(private[sqala] val exprs: List[SqlExpr])

/**
 * A grouping sets specification used by `groupBy` and `groupingSets` clauses.
 */
final case class GroupingSets(private[sqala] val items: List[SqlGroupingItem])