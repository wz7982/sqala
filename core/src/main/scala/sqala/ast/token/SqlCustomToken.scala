package sqala.ast.token

import sqala.ast.expr.SqlExpr

/**
 * A custom token that can be used in SQL queries.
 */
enum SqlCustomToken:
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