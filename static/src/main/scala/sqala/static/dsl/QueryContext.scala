package sqala.static.dsl

import sqala.ast.table.SqlTable

/**
 * Provides a query context, managing automatic table alias generation.
 */
final case class QueryContext[L <: Int](private[sqala] val tableIndex: TableIndexRef):
    /**
     * Returns the next available table alias (`t1`, `t2`, ...).
     */
    private[sqala] def fetchAlias: String =
        tableIndex.index += 1
        s"t${tableIndex.index}"

/**
  * Provides a reference to a table index, allowing for mutable access to the current table index within a query context.
  */
final case class TableIndexRef(private[sqala] var index: Int)

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
final class EmptyMatchRecognizeContext

/**
 * Provides a match-recognize context.
 */
final case class MatchRecognizeContext[ST <: SqlTable](private[sqala] val sqlTable: ST)

/**
 * Provides a JSON table context.
 */
final class JsonContext