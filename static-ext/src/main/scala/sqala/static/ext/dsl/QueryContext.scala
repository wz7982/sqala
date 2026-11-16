package sqala.static.ext.dsl

import sqala.ir.IrTable

/**
 * Provides a graph table context.
 */
final class GraphContext

/**
 * Provides a match-recognize context.
 */
final class MatchRecognizeContext(private[sqala] val table: IrTable)

/**
 * Provides a JSON table context.
 */
final class JsonContext