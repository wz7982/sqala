package sqala.static.dsl.statement.query

import sqala.util.NonEmptyList
import sqala.ir.IrExpr
import sqala.ir.IrGroupingItem

/**
 * A cube specification used by `groupBy` and `groupingSets` clauses.
 */
final case class Cube(private[sqala] val exprs: NonEmptyList[IrExpr])

/**
 * A rollup specification used by `groupBy` and `groupingSets` clauses.
 */
final case class Rollup(private[sqala] val exprs: NonEmptyList[IrExpr])

/**
 * A grouping sets specification used by `groupBy` and `groupingSets` clauses.
 */
final case class GroupingSets(private[sqala] val items: NonEmptyList[IrGroupingItem])