package sqala.printer

import sqala.ast.expr.SqlExpr
import sqala.ast.limit.SqlLimit
import sqala.ast.statement.SqlStatement

/**
 * SQLite dialect printer.
 *
 * Uses `LIMIT n OFFSET n`, `INSERT OR REPLACE INTO` for upserts,
 * and `GROUP_CONCAT` in place of `LISTAGG`.
 *
 * @param standardEscapeStrings `true` treats backslashes literally;
 *                              `false` uses backslashes as escape characters.
 */
class SqlitePrinter(override val standardEscapeStrings: Boolean) extends SqlPrinter(standardEscapeStrings):
    /**
     * Prints a `LIMIT n OFFSET n` clause.
     *
     * @param limit the limit node.
     */
    override def printLimit(limit: SqlLimit): Unit =
        sqlBuilder.append("LIMIT ")
        printExpr(limit.fetch.map(_.limit).getOrElse(SqlExpr.NumberLiteral(Long.MaxValue)))
        for f <- limit.offset do
            sqlBuilder.append(" OFFSET ")
            printExpr(f)

    /**
     * Prints an upsert using `INSERT OR REPLACE INTO`.
     *
     * @param upsert the UPSERT AST node.
     */
    override def printUpsert(upsert: SqlStatement.Upsert): Unit =
        sqlBuilder.append("INSERT OR REPLACE INTO ")
        printTable(upsert.table)

        sqlBuilder.append(" (")
        printList(upsert.columns)(printIdent)
        sqlBuilder.append(")")

        sqlBuilder.append(" VALUES (")
        printList(upsert.values)(printExpr)
        sqlBuilder.append(")")

    /**
     * Prints a `GROUP_CONCAT` function, rewriting `LISTAGG(expr, sep)` to
     * `GROUP_CONCAT(expr, sep ORDER BY ...)`.
     *
     * @param expr the LISTAGG expression node.
     */
    override def printListAggFuncExpr(expr: SqlExpr.ListAggFunc): Unit =
        val func = SqlExpr.GeneralFunc(
            expr.quantifier,
            "GROUP_CONCAT",
            expr.expr :: expr.separator :: Nil,
            expr.withinGroup,
            Nil,
            expr.filter
        )
        printExpr(func)