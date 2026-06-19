package sqala.ast.expr

/**
 * Quantifiers applied to subquery expressions.
 *
 * Used with comparison operators to qualify the result of a subquery.
 */
enum SqlSubqueryQuantifier:
    /**
     * `ANY` or `SOME` quantifier.
     *
     * Renders as `ANY`.
     */
    case Any

    /**
     * `ALL` quantifier.
     *
     * Renders as `ALL`.
     */
    case All