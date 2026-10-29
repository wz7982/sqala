package sqala.static.ext.dsl

import sqala.ir.IrTable

/**
 * Provides a graph table context.
 */
final class GraphContext

/**
 * Provides a pivot table context.
 */
final class PivotContext

/**
 * Provides a match-recognize context.
 */
final class EmptyMatchRecognizeContext

/**
 * Provides a match-recognize context.
 */
final case class MatchRecognizeContext[T <: IrTable](private[sqala] val table: IrTable)

/**
 * Provides a JSON table context.
 */
final class JsonContext