package sqala.parser

/**
 * A lexical token produced by Lexer.
 */
enum Token:
    /** An identifier. `quoted` is true when it was written as `"name"`. */
    case Identifier(name: String, quoted: Boolean)

    /** A single-quoted string literal, already unescaped. */
    case StringLiteral(value: String)

    /** A numeric literal. */
    case NumberLiteral(value: BigDecimal)

    /** An operator or punctuation symbol, such as `(`, `<>`, or `||`. */
    case Symbol(value: String)

    /** The end of the input. */
    case EndOfInput

/**
 * A token together with its source position.
 */
final case class TokenPosition(
    token: Token,
    offset: Int,
    line: Int,
    column: Int
)