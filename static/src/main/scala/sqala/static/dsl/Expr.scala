package sqala.static.dsl

import sqala.ir.IrExpr

/**
 * A typed expression wrapping an IR node. All DSL operations on
 * columns, values, and subqueries produce or consume `Expr` values.
 */
sealed class Expr[T, K <: ExprKind](private[sqala] val expr: IrExpr)

object Expr:
    private[sqala] def unapply[T, K <: ExprKind](expr: Expr[T, K]): Option[IrExpr] =
        Some(expr.expr)

/**
 * A typed expression representing a column reference.
 */
final class ColumnExpr[T, L <: Int](
    override val expr: IrExpr.Column
) extends Expr[T, Column[L]](expr)

/**
 * A typed expression representing an aggregate function.
 */
final class AggExpr[T, KS <: Tuple](
    override val expr: IrExpr.CountAsteriskFunc | IrExpr.ListAggFunc | IrExpr.GeneralFunc | IrExpr.JsonObjectAggFunc | IrExpr.JsonArrayAggFunc
) extends Expr[T, Agg[KS]](expr)