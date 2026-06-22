package sqala.ast.token

import sqala.ast.expr.SqlExpr

/**
 * ⚠️ A unsafe custom token that can be used in SQL queries.
 * ⚠️ Do not pass user input directly!
 */
enum SqlUnsafeCustomToken:
    /**
     * A keyword token.
     *
     * Renders as the given `keyword` string directly.
     */
    case Keyword(keyword: String)
    /**
     * An expression token.
     *
     * Renders as the given `expr` string directly.
     */
    case Expr(expr: SqlExpr)