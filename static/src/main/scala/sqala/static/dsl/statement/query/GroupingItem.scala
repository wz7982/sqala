package sqala.static.dsl.statement.query

import sqala.ir.{IrExpr, IrGroupingItem}
import sqala.util.NonEmptyList

/**
 * A cube specification used by `groupBy` and `groupingSets` clauses.
 */
final class Cube(private[sqala] val exprs: NonEmptyList[IrExpr])

/**
 * A rollup specification used by `groupBy` and `groupingSets` clauses.
 */
final class Rollup(private[sqala] val exprs: NonEmptyList[IrExpr])

/**
 * A grouping sets specification used by `groupBy` and `groupingSets` clauses.
 */
final class GroupingSets(private[sqala] val items: NonEmptyList[IrGroupingItem])