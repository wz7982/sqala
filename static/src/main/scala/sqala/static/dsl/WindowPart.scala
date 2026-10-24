package sqala.static.dsl

import sqala.ast.expr.SqlExpr
import sqala.ast.window.SqlWindow
import sqala.ir.IrExpr

/**
 * An intermediate type produced by window functions (`rank`, `lag`,
 * etc.). A window function cannot be used as an expression until
 * `over` is called, matching the SQL requirement that window
 * functions require an `OVER` clause.
 */
final case class WindowPart[T, KS <: Tuple](private[sqala] val expr: IrExpr)