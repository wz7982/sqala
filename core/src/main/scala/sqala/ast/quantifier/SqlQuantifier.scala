package sqala.ast.quantifier

import sqala.ast.token.SqlCustomToken

/**
 * Quantifiers for aggregate functions and set operations.
 */
enum SqlQuantifier:
    /**
     * `ALL` quantifier.
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
     * Renders as `(tokens(0) tokens(1) ... tokens(n))`.
     *
     * The `tokens` are interleaved, and the whole
     * expression is wrapped in parentheses.
     */
    case Custom(tokens: List[SqlCustomToken])