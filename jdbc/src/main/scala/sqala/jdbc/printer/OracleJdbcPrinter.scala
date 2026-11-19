package sqala.jdbc.printer

import sqala.ast.expr.*
import sqala.ast.table.{SqlGraphQuantifier, SqlRowPatternQuantifier}
import sqala.printer.OraclePrinter

/**
 * Oracle dialect printer.
 */
class OracleJdbcPrinter(
    override val standardEscapeStrings: Boolean
) extends OraclePrinter(standardEscapeStrings) with JdbcSqlPrinter:
    override def printTimeLiteralExpr(expr: SqlExpr.TimeLiteral): Unit =
        expr.`type` match
            case SqlTimeType.Timestamp(Some(SqlTimeZoneMode.With)) =>
                val func = SqlExpr.GeneralFunc(
                    None,
                    "TO_TIMESTAMP_TZ",
                    SqlExpr.StringLiteral(expr.time) :: 
                    SqlExpr.InlinedLiteral(SqlExpr.StringLiteral("YYYY-MM-DD HH24:MI:SS.FF9 TZH:TZM")) :: 
                    Nil,
                    Nil,
                    Nil,
                    None
                )
                printExpr(func)
            case SqlTimeType.Timestamp(_) =>
                val func = SqlExpr.GeneralFunc(
                    None,
                    "TO_TIMESTAMP",
                    SqlExpr.StringLiteral(expr.time) :: 
                    SqlExpr.InlinedLiteral(SqlExpr.StringLiteral("YYYY-MM-DD HH24:MI:SS.FF9")) :: 
                    Nil,
                    Nil,
                    Nil,
                    None
                )
                printExpr(func)
            case SqlTimeType.Date =>
                val func = SqlExpr.GeneralFunc(
                    None,
                    "TO_DATE",
                    SqlExpr.StringLiteral(expr.time) :: 
                    SqlExpr.InlinedLiteral(SqlExpr.StringLiteral("YYYY-MM-DD")) :: 
                    Nil,
                    Nil,
                    Nil,
                    None
                )
                printExpr(func)
            case _ =>
                super.printTimeLiteralExpr(expr)

    override def printIntervalLiteralExpr(expr: SqlExpr.IntervalLiteral): Unit = 
        expr.field match
            case SqlIntervalField.To(_, _) =>
                super.printIntervalLiteralExpr(expr)
            case SqlIntervalField.Single(unit) =>
                val interval = SqlExpr.StringLiteral(expr.value)
                unit match
                    case SqlTimeUnit.Year =>
                        val func =
                            SqlExpr.GeneralFunc(
                                None,
                                "NUMTOYMINTERVAL",
                                interval :: 
                                SqlExpr.InlinedLiteral(SqlExpr.StringLiteral("YEAR")) :: 
                                Nil,
                                Nil,
                                Nil,
                                None
                            )
                        printExpr(func)
                    case SqlTimeUnit.Month =>
                        val func =
                            SqlExpr.GeneralFunc(
                                None,
                                "NUMTOYMINTERVAL",
                                interval :: 
                                SqlExpr.InlinedLiteral(SqlExpr.StringLiteral("MONTH")) :: 
                                Nil,
                                Nil,
                                Nil,
                                None
                            )
                        printExpr(func)
                    case SqlTimeUnit.Day =>
                        val func =
                            SqlExpr.GeneralFunc(
                                None,
                                "NUMTODSINTERVAL",
                                interval :: 
                                SqlExpr.InlinedLiteral(SqlExpr.StringLiteral("DAY")) :: 
                                Nil,
                                Nil,
                                Nil,
                                None
                            )
                        printExpr(func)
                    case SqlTimeUnit.Hour =>
                        val func =
                            SqlExpr.GeneralFunc(
                                None,
                                "NUMTODSINTERVAL",
                                interval :: 
                                SqlExpr.InlinedLiteral(SqlExpr.StringLiteral("HOUR")) :: 
                                Nil,
                                Nil,
                                Nil,
                                None
                            )
                        printExpr(func)
                    case SqlTimeUnit.Minute =>
                        val func =
                            SqlExpr.GeneralFunc(
                                None,
                                "NUMTODSINTERVAL",
                                interval :: 
                                SqlExpr.InlinedLiteral(SqlExpr.StringLiteral("MINUTE")) :: 
                                Nil,
                                Nil,
                                Nil,
                                None
                            )
                        printExpr(func)
                    case SqlTimeUnit.Second =>
                        val func =
                            SqlExpr.GeneralFunc(
                                None,
                                "NUMTODSINTERVAL",
                                interval :: 
                                SqlExpr.InlinedLiteral(SqlExpr.StringLiteral("SECOND")) :: 
                                Nil,
                                Nil,
                                Nil,
                                None
                            )
                        printExpr(func)
                    case SqlTimeUnit.UnsafeCustom(_) =>
                        super.printIntervalLiteralExpr(expr)

    override def printGraphQuantifier(quantifier: SqlGraphQuantifier): Unit =
        sqlBuilder.append("{\\")
        super.printGraphQuantifier(quantifier)
        sqlBuilder.append("\\}")

    override def printRowPatternQuantifier(quantifier: SqlRowPatternQuantifier): Unit =
        sqlBuilder.append("{\\")
        super.printRowPatternQuantifier(quantifier)
        sqlBuilder.append("\\}")