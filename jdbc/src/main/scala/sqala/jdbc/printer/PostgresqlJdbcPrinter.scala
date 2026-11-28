package sqala.jdbc.printer

import sqala.ast.expr.*
import sqala.printer.{PostgresqlPrinter, PrintException}

/**
 * PostgreSQL dialect printer.
 */
class PostgresqlJdbcPrinter(
    override val standardEscapeStrings: Boolean
) extends PostgresqlPrinter(standardEscapeStrings) with JdbcSqlPrinter:
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
                val interval =
                    try
                        SqlExpr.NumberLiteral(expr.value.toInt)
                    catch
                        case e: NumberFormatException =>
                            throw new PrintException(s"Non-integer interval quantity cannot be printed as a SQL literal.")
                unit match
                    case SqlTimeUnit.Year =>
                        val args = 
                            interval :: Nil
                        printExpr(makeInterval(args))
                    case SqlTimeUnit.Month =>
                        val args = 
                            SqlExpr.NumberLiteral(0) :: interval :: Nil
                        printExpr(makeInterval(args))
                    case SqlTimeUnit.Day =>
                        val args = 
                            SqlExpr.NumberLiteral(0) :: SqlExpr.NumberLiteral(0) :: 
                            SqlExpr.NumberLiteral(0) :: interval :: Nil
                        printExpr(makeInterval(args))
                    case SqlTimeUnit.Hour =>
                        val args = 
                            SqlExpr.NumberLiteral(0) :: SqlExpr.NumberLiteral(0) :: 
                            SqlExpr.NumberLiteral(0) :: SqlExpr.NumberLiteral(0) :: 
                            interval :: Nil
                        printExpr(makeInterval(args))
                    case SqlTimeUnit.Minute =>
                        val args = 
                            SqlExpr.NumberLiteral(0) :: SqlExpr.NumberLiteral(0) :: 
                            SqlExpr.NumberLiteral(0) :: SqlExpr.NumberLiteral(0) :: 
                            SqlExpr.NumberLiteral(0) :: interval :: Nil
                        printExpr(makeInterval(args))
                    case SqlTimeUnit.Second =>
                        val args = 
                            SqlExpr.NumberLiteral(0) :: SqlExpr.NumberLiteral(0) :: 
                            SqlExpr.NumberLiteral(0) :: SqlExpr.NumberLiteral(0) :: 
                            SqlExpr.NumberLiteral(0) :: SqlExpr.NumberLiteral(0) :: 
                            interval :: Nil
                        printExpr(makeInterval(args))
                    case SqlTimeUnit.UnsafeCustom(_) =>
                        super.printIntervalLiteralExpr(expr)