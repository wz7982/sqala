package sqala.ast.expr

import sqala.ast.token.SqlCustomToken

/**
 * Binary operators used in expressions.
 *
 * Each operator carries an integer precedence, where higher values bind tighter.
 */
enum SqlBinaryOperator(val precedence: Int):
    /**
     * Multiplication.
     *
     * Renders as `*`.
     */
    case Times extends SqlBinaryOperator(60)

    /**
     * Division.
     *
     * Renders as `/`.
     */
    case Div extends SqlBinaryOperator(60)

    /**
     * Addition.
     *
     * Renders as `+`.
     */
    case Plus extends SqlBinaryOperator(50)

    /**
     * Subtraction.
     *
     * Renders as `-`.
     */
    case Minus extends SqlBinaryOperator(50)

    /**
     * String concatenation.
     *
     * Renders as `||`.
     */
    case Concat extends SqlBinaryOperator(40)

    /**
     * Equality comparison.
     *
     * Renders as `=`.
     */
    case Equal extends SqlBinaryOperator(30)

    /**
     * Not-equal comparison.
     *
     * Renders as `<>`.
     */
    case NotEqual extends SqlBinaryOperator(30)

    /**
     * `IS DISTINCT FROM` null-safe comparison.
     *
     * Renders as `IS [NOT] DISTINCT FROM`.
     */
    case IsDistinctFrom(withNot: Boolean) extends SqlBinaryOperator(30)

    /**
     * `IS` value check.
     *
     * Renders as `IS [NOT]`.
     */
    case Is(withNot: Boolean) extends SqlBinaryOperator(30)

    /**
     * Greater-than comparison.
     *
     * Renders as `>`.
     */
    case GreaterThan extends SqlBinaryOperator(30)

    /**
     * Greater-than-or-equal comparison.
     *
     * Renders as `>=`.
     */
    case GreaterThanEqual extends SqlBinaryOperator(30)

    /**
     * Less-than comparison.
     *
     * Renders as `<`.
     */
    case LessThan extends SqlBinaryOperator(30)

    /**
     * Less-than-or-equal comparison.
     *
     * Renders as `<=`.
     */
    case LessThanEqual extends SqlBinaryOperator(30)

    /**
     * `OVERLAPS` temporal overlap test.
     *
     * Renders as `OVERLAPS`.
     */
    case Overlaps extends SqlBinaryOperator(30)

    /**
     * Logical AND.
     *
     * Renders as `AND`.
     */
    case And extends SqlBinaryOperator(20)

    /**
     * Logical OR.
     *
     * Renders as `OR`.
     */
    case Or extends SqlBinaryOperator(10)

    /**
     * A custom binary operator with a free-form name.
     *
     * Renders as the given `tokens` directly.
     */
    case Custom(tokens: List[SqlCustomToken]) extends SqlBinaryOperator(0)

/**
 * Unary operators used in expressions.
 */
enum SqlUnaryOperator:
    /**
     * Unary plus.
     *
     * Renders as `+`.
     */
    case Positive

    /**
     * Unary minus (negation).
     *
     * Renders as `-`.
     */
    case Negative

    /**
     * Logical NOT.
     *
     * Renders as `NOT`.
     */
    case Not

    /**
     * A custom unary operator with a free-form name.
     *
     * Renders as the given `tokens` directly.
     */
    case Custom(tokens: List[SqlCustomToken])

/**
 * Quantified comparison operators used in expressions.
 */
enum SqlQuantifiedComparisonOperator:
    /**
     * Equality comparison.
     *
     * Renders as `=`.
     */
    case Equal
    /**
     * Not-equal comparison.
     *
     * Renders as `<>`.
     */
    case NotEqual
    /**
     * Greater-than comparison.
     *
     * Renders as `>`.
     */
    case GreaterThan
    /**
     * Greater-than-or-equal comparison.
     *
     * Renders as `>=`.
     */
    case GreaterThanEqual
    /**
     * Less-than comparison.
     *
     * Renders as `<`.
     */
    case LessThan
    /**
     * Less-than-or-equal comparison.
     *
     * Renders as `<=`.
     */
    case LessThanEqual
    /**
     * A custom quantified comparison operator with a free-form name.
     *
     * Renders as the given `tokens` directly.
     */
    case Custom(tokens: List[SqlCustomToken])