package sqala.jdbc.printer

import sqala.ast.expr.*
import sqala.printer.PostgresqlPrinter

/**
 * PostgreSQL dialect printer.
 */
class PostgresqlJdbcPrinter(
    override val standardEscapeStrings: Boolean
) extends PostgresqlPrinter(standardEscapeStrings) with JdbcSqlPrinter:
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
        def makeInterval(args: List[SqlExpr]): SqlExpr =
            SqlExpr.GeneralFunc(
                None,
                "MAKE_INTERVAL",
                args,
                Nil,
                Nil,
                None
            )
        
        expr.field match
            case SqlIntervalField.To(_, _) =>
                super.printIntervalLiteralExpr(expr)
            case SqlIntervalField.Single(unit) =>
                val interval = SqlExpr.StringLiteral(expr.value)
                unit match
                    case SqlTimeUnit.Year =>
                        val args = 
                            interval :: Nil
                        printExpr(makeInterval(args))
                    case SqlTimeUnit.Month =>
                        val args = 
                            SqlExpr.StringLiteral("0") :: interval :: Nil
                        printExpr(makeInterval(args))
                    case SqlTimeUnit.Day =>
                        val args = 
                            SqlExpr.StringLiteral("0") :: SqlExpr.StringLiteral("0") :: 
                            SqlExpr.StringLiteral("0") :: interval :: Nil
                        printExpr(makeInterval(args))
                    case SqlTimeUnit.Hour =>
                        val args = 
                            SqlExpr.StringLiteral("0") :: SqlExpr.StringLiteral("0") :: 
                            SqlExpr.StringLiteral("0") :: SqlExpr.StringLiteral("0") :: 
                            interval :: Nil
                        printExpr(makeInterval(args))
                    case SqlTimeUnit.Minute =>
                        val args = 
                            SqlExpr.StringLiteral("0") :: SqlExpr.StringLiteral("0") :: 
                            SqlExpr.StringLiteral("0") :: SqlExpr.StringLiteral("0") :: 
                            SqlExpr.StringLiteral("0") :: interval :: Nil
                        printExpr(makeInterval(args))
                    case SqlTimeUnit.Second =>
                        val args = 
                            SqlExpr.StringLiteral("0") :: SqlExpr.StringLiteral("0") :: 
                            SqlExpr.StringLiteral("0") :: SqlExpr.StringLiteral("0") :: 
                            SqlExpr.StringLiteral("0") :: SqlExpr.StringLiteral("0") :: 
                            interval :: Nil
                        printExpr(makeInterval(args))
                    case SqlTimeUnit.UnsafeCustom(_) =>
                        super.printIntervalLiteralExpr(expr)