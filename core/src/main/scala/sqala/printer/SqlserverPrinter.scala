package sqala.printer

import sqala.ast.expr.*
import sqala.ast.order.SqlNullsOrdering.{First, Last}
import sqala.ast.order.SqlOrdering
import sqala.ast.order.SqlOrdering.{Asc, Desc}
import sqala.ast.order.SqlOrderingItem

class SqlserverPrinter(override val standardEscapeStrings: Boolean) extends SqlPrinter(standardEscapeStrings):
    override val leftQuote: Char = '['

    override val rightQuote: Char = ']'

    override def printCteRecursive(): Unit =
        ()

    override def printStringLiteralExpr(expr: SqlExpr.StringLiteral): Unit =
        sqlBuilder.append("N")
        printChars(expr.string)

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
                SqlWhen(
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