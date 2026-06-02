package sqala.printer

import sqala.ast.expr.*
import sqala.ast.order.SqlNullsOrdering.{First, Last}
import sqala.ast.order.SqlOrdering
import sqala.ast.order.SqlOrdering.{Asc, Desc}
import sqala.ast.order.SqlOrderingItem

/**
 * SQL Server dialect printer.
 *
 * Uses bracket quoting (`[` `]`), suppresses `RECURSIVE` in CTEs, adds `N` prefix
 * to string literals, rewrites time literals via `CAST`, maps type names,
 * uses `STRING_AGG`, and emulates `NULLS FIRST|LAST` via `CASE WHEN`.
 *
 * @param standardEscapeStrings `true` treats backslashes literally;
 *                              `false` uses backslashes as escape characters.
 */
class SqlserverPrinter(override val standardEscapeStrings: Boolean) extends SqlPrinter(standardEscapeStrings):
    /** 
     * Use bracket quoting for identifiers. *
     */
    override val leftQuote: Char = '['

    /** 
     * Use bracket quoting for identifiers. *
     */
    override val rightQuote: Char = ']'

    /**
     * Suppresses the `RECURSIVE` keyword (not needed in SQL Server).
     */
    override def printCteRecursive(): Unit =
        ()

    /**
     * Prints a string literal with an `N` prefix for Unicode support.
     *
     * @param expr the string literal expression node.
     */
    override def printStringLiteralExpr(expr: SqlExpr.StringLiteral): Unit =
        sqlBuilder.append("N")
        printChars(expr.string)

    /**
     * Prints a time literal, rewriting to `CAST(string AS type)`:
     * `TIMESTAMP WITH TIME ZONE` → `CAST(... AS DATETIMEOFFSET)`,
     * `TIMESTAMP` → `CAST(... AS DATETIME2)`, `DATE` → `CAST(... AS DATE)`.
     *
     * @param expr the time literal expression node.
     */
    override def printTimeLiteralExpr(expr: SqlExpr.TimeLiteral): Unit =
        expr.unit match
            case SqlTimeLiteralUnit.Timestamp(Some(SqlTimeZoneMode.With)) =>
                val cast = SqlExpr.Cast(
                    SqlExpr.StringLiteral(expr.time),
                    SqlType.Timestamp(Some(SqlTimeZoneMode.With))
                )
                printExpr(cast)
            case SqlTimeLiteralUnit.Timestamp(_) =>
                val cast = SqlExpr.Cast(
                    SqlExpr.StringLiteral(expr.time),
                    SqlType.Timestamp(None)
                )
                printExpr(cast)
            case SqlTimeLiteralUnit.Date =>
                val cast = SqlExpr.Cast(
                    SqlExpr.StringLiteral(expr.time),
                    SqlType.Date
                )
                printExpr(cast)
            case _ =>
                super.printTimeLiteralExpr(expr)

    /**
     * Prints a type name with SQL Server-specific mappings:
     * unbounded `VARCHAR` → `NVARCHAR(MAX)`, bounded `VARCHAR(n)` → `NVARCHAR(n)`,
     * `TIMESTAMP WITH TIME ZONE` → `DATETIMEOFFSET`,
     * `TIMESTAMP` → `DATETIME2`.
     *
     * @param type the data type node.
     */
    override def printType(`type`: SqlType): Unit =
        `type` match
            case SqlType.Varchar(None) =>
                sqlBuilder.append("NVARCHAR(MAX)")
            case SqlType.Varchar(Some(l)) =>
                sqlBuilder.append("NVARCHAR")
                sqlBuilder.append("(")
                sqlBuilder.append(l)
                sqlBuilder.append(")")
            case SqlType.Timestamp(Some(SqlTimeZoneMode.With)) =>
                sqlBuilder.append("DATETIMEOFFSET")
            case SqlType.Timestamp(_) =>
                sqlBuilder.append("DATETIME2")
            case _ =>
                super.printType(`type`)

    /**
     * Prints a `STRING_AGG` function, rewriting `LISTAGG(expr, sep)` to
     * `STRING_AGG(expr, sep) WITHIN GROUP (ORDER BY ...)`.
     *
     * @param expr the LISTAGG expression node.
     */
    override def printListAggFuncExpr(expr: SqlExpr.ListAggFunc): Unit =
        val func = SqlExpr.GeneralFunc(
            expr.quantifier,
            "STRING_AGG",
            expr.expr :: expr.separator :: Nil,
            Nil,
            expr.withinGroup,
            expr.filter
        )
        printExpr(func)

    /**
     * Prints an ordering item, emulating `NULLS FIRST|LAST` with a
     * `CASE WHEN` expression since SQL Server does not support it natively.
     *
     * @param orderBy the ordering item node.
     */
    override def printOrderingItem(orderBy: SqlOrderingItem): Unit =
        def printOrdering(ordering: SqlOrdering): Unit =
            ordering match
                case Asc => sqlBuilder.append("ASC")
                case Desc => sqlBuilder.append("DESC")

        val order = orderBy.ordering match
            case None | Some(Asc) => Asc
            case _ => Desc
        val orderExpr =
            SqlExpr.Case(
                SqlCaseBranch(
                    SqlExpr.Binary(orderBy.expr, SqlBinaryOperator.Is, SqlExpr.NullLiteral),
                    SqlExpr.NumberLiteral(1)
                ) :: Nil,
                Some(SqlExpr.NumberLiteral(0))
            )
        (order, orderBy.nullsOrdering) match
            case (_, None) | (Asc, Some(First)) | (Desc, Some(Last)) =>
                printExpr(orderBy.expr)
                sqlBuilder.append(" ")
                printOrdering(order)
            case (Asc, Some(Last)) | (Desc, Some(First)) =>
                printExpr(orderExpr)
                sqlBuilder.append(" ")
                printOrdering(order)
                sqlBuilder.append(", ")
                printExpr(orderBy.expr)
                sqlBuilder.append(" ")
                printOrdering(order)