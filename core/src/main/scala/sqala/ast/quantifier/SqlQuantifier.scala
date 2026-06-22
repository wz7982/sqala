package sqala.ast.quantifier

import sqala.ast.token.SqlUnsafeCustomToken

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
     * ⚠️ Unsafe extension point: allows arbitrary SQL fragments.
     * ⚠️ Do not pass user input directly!
     *
     * Renders as `(tokens(0) tokens(1) ... tokens(n))`.
     */
    case UnsafeCustom(tokens: List[SqlUnsafeCustomToken])