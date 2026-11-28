package sqala.jdbc.printer

import sqala.ast.expr.SqlExpr
import sqala.jdbc.JdbcArg
import sqala.printer.MysqlPrinter

/**
 * MySQL dialect printer.
 */
class MysqlJdbcPrinter(
    override val standardEscapeStrings: Boolean
) extends MysqlPrinter(standardEscapeStrings) with JdbcSqlPrinter:
    override def printIntervalLiteralExpr(expr: SqlExpr.IntervalLiteral): Unit =
        sqlBuilder.append("INTERVAL ? ")
        printIntervalField(expr.field)
        collectedArgs.addOne(JdbcArg(expr.value))