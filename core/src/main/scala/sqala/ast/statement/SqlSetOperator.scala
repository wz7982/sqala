package sqala.ast.statement

import sqala.ast.quantifier.SqlQuantifier

/**
 * Set operators for combining query results.
 */
enum SqlSetOperator(val quantifier: Option[SqlQuantifier], val precedence: Int):
    /**
     * `INTERSECT` set operator.
     *
     * Renders as `INTERSECT [DISTINCT|ALL]`.
     */
    case Intersect(
        override val quantifier: Option[SqlQuantifier]
    ) extends SqlSetOperator(quantifier, 20)

    /**
     * `UNION` set operator.
     *
     * Renders as `UNION [DISTINCT|ALL]`.
     */
    case Union(
        override val quantifier: Option[SqlQuantifier],
    ) extends SqlSetOperator(quantifier, 10)

    /**
     * `EXCEPT` set operator.
     *
     * Renders as `EXCEPT [DISTINCT|ALL]`.
     */
    case Except(
        override val quantifier: Option[SqlQuantifier]
    ) extends SqlSetOperator(quantifier, 10)

/**
  * Set corresponding clause.
  * 
  * Renders as `CORRESPONDING [BY (column [, ...])]`
  */
case class SqlSetCorresponding(columnNames: List[String])