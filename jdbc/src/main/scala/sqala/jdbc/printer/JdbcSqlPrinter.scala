package sqala.jdbc.printer

import sqala.ast.expr.{SqlExpr, SqlTimeType, SqlTimeZoneMode}
import sqala.jdbc.{JdbcArg, JdbcEncoder}
import sqala.printer.StandardSqlPrinter

import java.time.*
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder, DateTimeParseException}
import java.time.temporal.ChronoField
import scala.collection.mutable.ArrayBuffer

/**
 * A JDBC-oriented SQL printer that parameterizes literal values.
 *
 * Ordinary string, numeric, and boolean literals are rendered as `?`
 * placeholders, and their arguments are collected in `collectedArgs` in the
 * same order as the placeholders appear in the generated SQL. The SQL
 * string and the collected arguments can then be passed to a
 * `PreparedStatement`.
 *
 * Literals wrapped in `SqlExpr.InlinedLiteral` are still rendered inline,
 * so they are not added to `collectedArgs`.
 */
trait JdbcSqlPrinter extends StandardSqlPrinter:
    /**
     * Parameter arguments collected while printing, in the same order as the
     * emitted `?` placeholders.
     *
     * This buffer is mutated as the printer walks the AST. Read the final
     * arguments through `args`.
     */
    val collectedArgs: ArrayBuffer[JdbcArg[?]] = new ArrayBuffer()

    /**
     * Returns a snapshot of the collected arguments, in the same order as
     * the `?` placeholders in the generated SQL.
     */
    def args: Array[JdbcArg[?]] =
        collectedArgs.toArray

    private def printParameter[T](value: T)(using e: JdbcEncoder[T]): Unit =
        sqlBuilder.append("?")
        collectedArgs.addOne(JdbcArg(value)(using e))

    override def printStringLiteralExpr(expr: SqlExpr.StringLiteral): Unit =
        printParameter(expr.string)

    override def printBooleanLiteralExpr(expr: SqlExpr.BooleanLiteral): Unit =
        printParameter(expr.boolean)

    override def printIntLiteralExpr(n: Int): Unit =
        printParameter(n)

    override def printLongLiteralExpr(n: Long): Unit =
        printParameter(n)

    override def printFloatLiteralExpr(n: Float): Unit =
        printParameter(n)

    override def printDoubleLiteralExpr(n: Double): Unit =
        printParameter(n)

    override def printBigDecimalLiteralExpr(n: BigDecimal): Unit =
        printParameter(n)

    override def printNumberLiteralExpr(expr: SqlExpr.NumberLiteral[?]): Unit =
        printBigDecimalLiteralExpr(BigDecimal(expr.number.toString))

    override def printTimeLiteralExpr(expr: SqlExpr.TimeLiteral): Unit =
        try
            expr.`type` match
                case SqlTimeType.Date =>
                    printParameter(JdbcSqlPrinter.parseDate(expr.time))
                case SqlTimeType.Timestamp(Some(SqlTimeZoneMode.With)) =>
                    printParameter(JdbcSqlPrinter.parseOffsetDateTime(expr.time))
                case SqlTimeType.Timestamp(_) =>
                    printParameter(JdbcSqlPrinter.parseLocalDateTime(expr.time))
                case SqlTimeType.Time(Some(SqlTimeZoneMode.With)) =>
                    printParameter(JdbcSqlPrinter.parseOffsetTime(expr.time))
                case SqlTimeType.Time(_) =>
                    printParameter(JdbcSqlPrinter.parseLocalTime(expr.time))
        catch
            case _: DateTimeParseException =>
                super.printTimeLiteralExpr(expr)

/**
 * Formatters used to parse the lossless time strings produced by
 * `AsSqlExpr`.
 */
object JdbcSqlPrinter:
    private val dateFormatter: DateTimeFormatter =
        DateTimeFormatter.ISO_LOCAL_DATE

    private val localDateTimeFormatter: DateTimeFormatter =
        new DateTimeFormatterBuilder()
            .appendPattern("uuuu-MM-dd HH:mm:ss")
            .optionalStart()
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
            .optionalEnd()
            .toFormatter()

    private val localTimeFormatter: DateTimeFormatter =
        new DateTimeFormatterBuilder()
            .appendPattern("HH:mm:ss")
            .optionalStart()
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
            .optionalEnd()
            .toFormatter()

    private val offsetDateTimeFormatter: DateTimeFormatter =
        new DateTimeFormatterBuilder()
            .append(localDateTimeFormatter)
            .appendLiteral(' ')
            .appendPattern("XXX")
            .toFormatter()

    private val offsetTimeFormatter: DateTimeFormatter =
        new DateTimeFormatterBuilder()
            .append(localTimeFormatter)
            .appendLiteral(' ')
            .appendPattern("XXX")
            .toFormatter()

    private def parseDate(time: String): LocalDate =
        LocalDate.parse(time, dateFormatter)

    private def parseLocalDateTime(time: String): LocalDateTime =
        LocalDateTime.parse(time, localDateTimeFormatter)

    private def parseOffsetDateTime(time: String): OffsetDateTime =
        OffsetDateTime.parse(time, offsetDateTimeFormatter)

    private def parseLocalTime(time: String): LocalTime =
        LocalTime.parse(time, localTimeFormatter)

    private def parseOffsetTime(time: String): OffsetTime =
        OffsetTime.parse(time, offsetTimeFormatter)