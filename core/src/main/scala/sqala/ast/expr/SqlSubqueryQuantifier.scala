package sqala.ast.expr

/**
 * Quantifiers applied to subquery expressions.
 *
 * Used with comparison operators to qualify the result of a subquery.
 */
enum SqlSubqueryQuantifier:
    /**
     * `ANY` / `SOME` quantifier.
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

    /**
     * `EXISTS` quantifier.
     *
     * Renders as `EXISTS`.
     */
    case Exists