package sqala.static.dsl

/**
 * Provides a query context, managing automatic table alias generation.
 */
final case class QueryContext[L <: Int](private[sqala] var tableIndex: Int):
    /**
     * Returns the next available table alias (`t1`, `t2`, ...).
     */
    private[sqala] def fetchAlias: String =
        tableIndex += 1
        s"t$tableIndex"

/**
 * Provides a window function context.
 */
final class OverContext

/**
 * Provides a grouping context.
 */
final class GroupingContext

/**
 * Provides a recursive query context.
 */
final class ConnectByContext

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
final class MatchRecognizeContext

/**
 * Provides a JSON table context.
 */
final class JsonContext