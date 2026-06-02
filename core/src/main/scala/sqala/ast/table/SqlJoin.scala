package sqala.ast.table

import sqala.ast.expr.SqlExpr

/**
 * Join type for table joins.
 */
enum SqlJoinType:
    /**
     * `INNER JOIN`.
     *
     * Renders as `INNER`.
     */
    case Inner

    /**
     * `LEFT [OUTER] JOIN`.
     *
     * Renders as `LEFT`.
     */
    case Left

    /**
     * `RIGHT [OUTER] JOIN`.
     *
     * Renders as `RIGHT`.
     */
    case Right

    /**
     * `FULL [OUTER] JOIN`.
     *
     * Renders as `FULL`.
     */
    case Full

    /**
     * `CROSS JOIN`.
     *
     * Renders as `CROSS`.
     */
    case Cross

    /**
     * A custom join type with a free-form name.
     *
     * Renders as the given `type` string directly.
     */
    case Custom(`type`: String)

/**
 * Join condition for table joins.
 */
enum SqlJoinCondition:
    /**
     * An `ON` condition.
     *
     * Renders as `ON expr`.
     */
    case On(condition: SqlExpr)

    /**
     * A `USING` clause.
     *
     * Renders as `USING("column" [, ...])`.
     */
    case Using(columnNames: List[String])