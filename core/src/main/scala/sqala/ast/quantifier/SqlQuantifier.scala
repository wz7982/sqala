package sqala.ast.quantifier

import sqala.ast.expr.SqlExpr

/**
 * Quantifiers for aggregate functions and set operations.
 */
enum SqlQuantifier:
    /**
     * `ALL` quantifier (default behavior).
     *
     * Renders as `ALL`.
     */
    case All

    /**
     * `DISTINCT` quantifier.
     *
     * Renders as `DISTINCT`.
     */
    case Distinct

    /**
     * A custom quantifier with interpolated sub-expressions.
     *
     * Renders as `(words(0) exprs(0) words(1) exprs(1) ... words(n))`.
     *
     * The `words` and `exprs` are interleaved, and the whole
     * expression is wrapped in parentheses.
     *
     * @param words text fragments.
     * @param exprs sub-expressions to be interpolated between the words.
     */
    case Custom(words: List[String], exprs: List[SqlExpr])