package sqala.static.dsl

import sqala.ast.expr.{SqlBinaryOperator, SqlExpr, SqlUnaryOperator}
import sqala.ir.IrExpr
import scala.annotation.targetName

/**
 * A typed expression wrapping an AST node. All DSL operations on
 * columns, values, and subqueries produce or consume `Expr` values.
 */
sealed class Expr[T, K <: ExprKind](private[sqala] val expr: IrExpr):
    /**
     * Normalizes the expression by folding constant sub-expressions
     * (e.g. `NOT TRUE` → `FALSE`, `x IN ()` → `FALSE`, double negation elimination).
     */
    private[sqala] def asIrExpr: IrExpr =
        import IrExpr.*

        expr match
            case Unary(SqlUnaryOperator.Not, e) =>
                e match
                    case Native(SqlExpr.BooleanLiteral(boolean)) =>
                        Native(SqlExpr.BooleanLiteral(!boolean))
                    case Like(expr, pattern, not) =>
                        Like(expr, pattern, !not)
                    case In(left, right, not) =>
                        In(left, right, !not)
                    case Binary(left, SqlBinaryOperator.IsDistinctFrom(not), right) =>
                        Binary(left, SqlBinaryOperator.IsDistinctFrom(!not), right)
                    case Binary(left, SqlBinaryOperator.Is(not), right) =>
                        Binary(left, SqlBinaryOperator.Is(!not), right)
                    case Between(expr, s, e, n) =>
                        Between(expr, s, e, !n)
                    case _ =>
                        IrExpr.Unary(SqlUnaryOperator.Not, e)
            case _ =>
                expr

object Expr:
    def unapply[T, K <: ExprKind](expr: Expr[T, K]): Option[IrExpr] =
        Some(expr.expr)

/**
 * A typed expression representing a column reference.
 */
case class ColumnExpr[T, L <: Int](
    override val expr: IrExpr.Column
) extends Expr[T, Column[L]](expr):
    private[sqala] override def asIrExpr: IrExpr.Column =
        expr

/**
 * A typed expression representing an aggregate function.
 */
case class AggExpr[T, KS <: Tuple](
    override val expr: IrExpr.CountAsteriskFunc | IrExpr.ListAggFunc | IrExpr.GeneralFunc | IrExpr.JsonObjectAggFunc | IrExpr.JsonArrayAggFunc
) extends Expr[T, Agg[KS]](expr):
    private[sqala] override def asIrExpr: IrExpr.CountAsteriskFunc | IrExpr.ListAggFunc | IrExpr.GeneralFunc | IrExpr.JsonObjectAggFunc | IrExpr.JsonArrayAggFunc =
        expr