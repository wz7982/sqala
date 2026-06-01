package sqala.ast.statement

import sqala.ast.quantifier.SqlQuantifier

/**
 * Set operators for combining query results.
 *
 * @param quantifier optional `DISTINCT` or `ALL` modifier.
 */
enum SqlSetOperator(val quantifier: Option[SqlQuantifier]):
    /**
     * `UNION` set operator.
     *
     * Renders as `UNION [DISTINCT|ALL]`.
     *
     * @param quantifier optional quantifier.
     */
    case Union(override val quantifier: Option[SqlQuantifier]) extends SqlSetOperator(quantifier)

    /**
     * `EXCEPT` set operator.
     *
     * Renders as `EXCEPT [DISTINCT|ALL]`.
     *
     * @param quantifier optional quantifier.
     */
    case Except(override val quantifier: Option[SqlQuantifier]) extends SqlSetOperator(quantifier)

    /**
     * `INTERSECT` set operator.
     *
     * Renders as `INTERSECT [DISTINCT|ALL]`.
     *
     * @param quantifier optional quantifier.
     */
    case Intersect(override val quantifier: Option[SqlQuantifier]) extends SqlSetOperator(quantifier)