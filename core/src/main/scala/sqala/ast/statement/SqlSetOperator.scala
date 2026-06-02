package sqala.ast.statement

import sqala.ast.quantifier.SqlQuantifier

/**
 * Set operators for combining query results.
 */
enum SqlSetOperator(val quantifier: Option[SqlQuantifier]):
    /**
     * `UNION` set operator.
     *
     * Renders as `UNION [DISTINCT|ALL]`.
     */
    case Union(override val quantifier: Option[SqlQuantifier]) extends SqlSetOperator(quantifier)

    /**
     * `EXCEPT` set operator.
     *
     * Renders as `EXCEPT [DISTINCT|ALL]`.
     */
    case Except(override val quantifier: Option[SqlQuantifier]) extends SqlSetOperator(quantifier)

    /**
     * `INTERSECT` set operator.
     *
     * Renders as `INTERSECT [DISTINCT|ALL]`.
     */
    case Intersect(override val quantifier: Option[SqlQuantifier]) extends SqlSetOperator(quantifier)