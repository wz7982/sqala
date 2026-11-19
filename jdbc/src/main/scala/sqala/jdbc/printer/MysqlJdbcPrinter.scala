package sqala.jdbc.printer

import sqala.ast.expr.{SqlExpr, SqlTimeType, SqlType}
import sqala.printer.MysqlPrinter

/**
 * MySQL dialect printer.
 */
class MysqlJdbcPrinter(
    override val standardEscapeStrings: Boolean
) extends MysqlPrinter(standardEscapeStrings) with JdbcSqlPrinter:
    override def printTimeLiteralExpr(expr: SqlExpr.TimeLiteral): Unit =
        val time = SqlExpr.StringLiteral(expr.time)
        val `type` = expr.`type` match
            case SqlTimeType.Date =>
                SqlType.Date
            case SqlTimeType.Time(mode) =>
                SqlType.Time(mode)
            case SqlTimeType.Timestamp(mode) =>
                SqlType.Timestamp(mode)
        printExpr(SqlExpr.Cast(time, `type`))

    override def printIntervalLiteralExpr(expr: SqlExpr.IntervalLiteral): Unit =
        sqlBuilder.append("INTERVAL ? ")
        printIntervalField(expr.field)
        collectedArgs.addOne(expr.value)