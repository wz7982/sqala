package sqala.ast.table

import sqala.ast.expr.SqlExpr
import sqala.ast.token.SqlUnsafeCustomToken
import sqala.util.NonEmptyList

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
     * ⚠️ Unsafe extension point: allows arbitrary SQL fragments.
     * ⚠️ Do not pass user input directly!
     *
     * Renders as `(tokens(0) tokens(1) ... tokens(n))`.
     */
    case UnsafeCustom(tokens: List[SqlUnsafeCustomToken])

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
    case Using(columnNames: NonEmptyList[String])