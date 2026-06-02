package sqala.ast.statement

import sqala.ast.expr.SqlExpr

/**
 * An item in a `SELECT` clause.
 */
enum SqlSelectItem:
    /**
     * A `*` wildcard, optionally qualified with a table name.
     *
     * Renders as `*|"table".*`.
     *
     * @param tableName optional table qualifier.
     */
    case Asterisk(tableName: Option[String])

    /**
     * An expression with an optional alias.
     *
     * Renders as `expr [AS "alias"]`.
     *
     * @param expr the select expression.
     * @param alias optional column alias.
     */
    case Expr(expr: SqlExpr, alias: Option[String])