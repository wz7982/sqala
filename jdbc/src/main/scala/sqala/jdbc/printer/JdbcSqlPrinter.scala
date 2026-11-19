package sqala.jdbc.printer

import sqala.ast.expr.SqlExpr
import sqala.printer.StandardSqlPrinter

import scala.collection.mutable.ArrayBuffer

/**
 * A JDBC-oriented SQL printer that parameterizes literal values.
 *
 * Ordinary string, numeric, and boolean literals are rendered as `?`
 * placeholders, and their values are collected in `collectedArgs` in the
 * same order as the placeholders appear in the generated SQL. The SQL
 * string and the collected values can then be passed to a
 * `PreparedStatement`.
 *
 * Literals wrapped in `SqlExpr.InlinedLiteral` are still rendered inline,
 * so they are not added to `collectedArgs`.
 */
trait JdbcSqlPrinter extends StandardSqlPrinter:
    /**
     * Parameter values collected while printing, in the same order as the
     * emitted `?` placeholders.
     *
     * This buffer is mutated as the printer walks the AST. Read the final
     * values through `args`.
     */
    val collectedArgs: ArrayBuffer[Any] = new ArrayBuffer()

    /**
     * Returns a snapshot of the collected parameter values, in the same
     * order as the `?` placeholders in the generated SQL.
     */
    def args: Array[Any] =
        collectedArgs.toArray

    override def printStringLiteralExpr(expr: SqlExpr.StringLiteral): Unit =
        sqlBuilder.append("?")
        collectedArgs.addOne(expr.string)

    override def printNumberLiteralExpr(expr: SqlExpr.NumberLiteral): Unit =
        sqlBuilder.append("?")
        collectedArgs.addOne(expr.number)

    override def printBooleanLiteralExpr(expr: SqlExpr.BooleanLiteral): Unit =
        sqlBuilder.append("?")
        collectedArgs.addOne(expr.boolean)