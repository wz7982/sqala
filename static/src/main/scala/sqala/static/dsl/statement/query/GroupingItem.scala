package sqala.static.dsl.statement.query

import sqala.ast.expr.SqlExpr
import sqala.ast.group.SqlGroupingItem
import sqala.util.NonEmptyList

/**
 * A cube specification used by `groupBy` and `groupingSets` clauses.
 */
final case class Cube(private[sqala] val exprs: NonEmptyList[SqlExpr])

/**
 * A rollup specification used by `groupBy` and `groupingSets` clauses.
 */
final case class Rollup(private[sqala] val exprs: NonEmptyList[SqlExpr])

/**
 * A grouping sets specification used by `groupBy` and `groupingSets` clauses.
 */
final case class GroupingSets(private[sqala] val items: NonEmptyList[SqlGroupingItem])