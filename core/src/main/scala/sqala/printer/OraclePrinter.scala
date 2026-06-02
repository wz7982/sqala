package sqala.printer

import sqala.ast.expr.{SqlExpr, SqlTimeLiteralUnit, SqlTimeZoneMode, SqlType}
import sqala.ast.table.SqlTableAlias

/**
 * Oracle dialect printer.
 *
 * Suppresses `RECURSIVE` in CTEs, rewrites time literals to `TO_TIMESTAMP`/`TO_DATE`,
 * maps type names, and omits `AS` before table aliases.
 *
 * @param standardEscapeStrings `true` treats backslashes literally;
 *                              `false` uses backslashes as escape characters.
 */
class OraclePrinter(override val standardEscapeStrings: Boolean) extends SqlPrinter(standardEscapeStrings):
    /**
     * Suppresses the `RECURSIVE` keyword (not needed in Oracle).
     */
    override def printCteRecursive(): Unit =
        ()

    /**
     * Prints a time literal, rewriting to Oracle-specific functions:
     * `TIMESTAMP WITH TIME ZONE` → `TO_TIMESTAMP_TZ(...)`,
     * `TIMESTAMP` → `TO_TIMESTAMP(...)`, `DATE` → `TO_DATE(...)`.
     *
     * @param expr the time literal expression node.
     */
    override def printTimeLiteralExpr(expr: SqlExpr.TimeLiteral): Unit =
        expr.unit match
            case SqlTimeLiteralUnit.Timestamp(Some(SqlTimeZoneMode.With)) =>
                val func = SqlExpr.GeneralFunc(
                    None,
                    "TO_TIMESTAMP_TZ",
                    SqlExpr.StringLiteral(expr.time) :: SqlExpr.StringLiteral("YYYY-MM-DD HH24:MI:SS.FF9 TZH:TZM") :: Nil,
                    Nil,
                    Nil,
                    None
                )
                printExpr(func)
            case SqlTimeLiteralUnit.Timestamp(_) =>
                val func = SqlExpr.GeneralFunc(
                    None,
                    "TO_TIMESTAMP",
                    SqlExpr.StringLiteral(expr.time) :: SqlExpr.StringLiteral("YYYY-MM-DD HH24:MI:SS.FF9") :: Nil,
                    Nil,
                    Nil,
                    None
                )
                printExpr(func)
            case SqlTimeLiteralUnit.Date =>
                val func = SqlExpr.GeneralFunc(
                    None,
                    "TO_DATE",
                    SqlExpr.StringLiteral(expr.time) :: SqlExpr.StringLiteral("YYYY-MM-DD") :: Nil,
                    Nil,
                    Nil,
                    None
                )
                printExpr(func)
            case _ =>
                super.printTimeLiteralExpr(expr)

    /**
     * Prints a type name with Oracle-specific mappings:
     * unbounded `VARCHAR` → `VARCHAR(4000)`, `LONG` → `INT`.
     *
     * @param type the data type node.
     */
    override def printType(`type`: SqlType): Unit =
        `type` match
            case SqlType.Varchar(None) =>
                sqlBuilder.append("VARCHAR(4000)")
            case SqlType.Long =>
                sqlBuilder.append("INT")
            case _ =>
                super.printType(`type`)

    /**
     * Prints a table alias without the `AS` keyword.
     *
     * @param alias the table alias node.
     */
    override def printTableAlias(alias: SqlTableAlias): Unit =
        sqlBuilder.append(" ")
        printIdent(alias.alias)
        if alias.columnAliases.nonEmpty then
            sqlBuilder.append("(")
            printList(alias.columnAliases)(i => printIdent(i))
            sqlBuilder.append(")")