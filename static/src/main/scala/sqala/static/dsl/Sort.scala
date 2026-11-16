package sqala.static.dsl

import sqala.ir.IrOrderingItem

/**
 * An intermediate sort specification used by `sortBy`,
 * and window ordering clause, combining an expression with an
 * ordering direction and nulls handling.
 */
final class Sort[T, K <: ExprKind](private[sqala] val ordering: IrOrderingItem)