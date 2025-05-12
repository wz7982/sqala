package sqala.printer

import sqala.ast.expr.*
import sqala.ast.limit.SqlLimit
import sqala.ast.order.SqlOrderItem
import sqala.ast.order.SqlOrderNullsOption.{First, Last}
import sqala.ast.order.SqlOrderOption.{Asc, Desc}
import sqala.ast.statement.SqlStatement

class SqlitePrinter(override val enableJdbcPrepare: Boolean) extends SqlPrinter(enableJdbcPrepare):
    override def printLimit(limit: SqlLimit): Unit =
        sqlBuilder.append("LIMIT ")
        printExpr(limit.offset)
        sqlBuilder.append(", ")
        printExpr(limit.limit)

    override def printUpsert(upsert: SqlStatement.Upsert): Unit =
        sqlBuilder.append("INSERT OR REPLACE INTO ")
        printTable(upsert.table)

        sqlBuilder.append(" (")
        printList(upsert.columns)(printExpr)
        sqlBuilder.append(")")

        sqlBuilder.append(" VALUES (")
        printList(upsert.values)(printExpr)
        sqlBuilder.append(")")

    override def printOrderItem(item: SqlOrderItem): Unit =
        val order = item.order match
            case None | Some(Asc) => Asc
            case _ => Desc
        val orderExpr = SqlExpr.Case(SqlCase(SqlExpr.NullTest(item.expr, false), SqlExpr.NumberLiteral(1)) :: Nil, SqlExpr.NumberLiteral(0))
        (order, item.nullsOrder) match
            case (_, None) | (Asc, Some(First)) | (Desc, Some(Last)) =>
                printExpr(item.expr)
                sqlBuilder.append(s" ${order.order}")
            case (Asc, Some(Last)) | (Desc, Some(First)) =>
                printExpr(orderExpr)
                sqlBuilder.append(s" ${order.order},\n")
                printSpace()
                printExpr(item.expr)
                sqlBuilder.append(s" ${order.order}")

    override def printBinaryExpr(expr: SqlExpr.Binary): Unit = expr match
        case SqlExpr.Binary(left, op, SqlExpr.Interval(value, unit)) =>
            val intervalValue = op match
                case SqlBinaryOperator.Minus => value * -1
                case _ => value
            val printValue =
                if intervalValue >= 0 then
                    "+" + intervalValue
                else intervalValue.toString
            sqlBuilder.append("DATETIME(")
            printExpr(left)
            sqlBuilder.append(", ")
            sqlBuilder.append(s"'$printValue ${unit.unit.toLowerCase + "s"}')")
        case _ => super.printBinaryExpr(expr)

    override def printTimeLiteralExpr(expr: SqlExpr.TimeLiteral): Unit =
        val name = expr.unit match
            case SqlTimeLiteralUnit.Timestamp => "DATETIME"
            case SqlTimeLiteralUnit.Date => "DATE"
            case SqlTimeLiteralUnit.Time => "TIME"
        sqlBuilder.append(name)
        sqlBuilder.append("(")
        printStringLiteralExpr(SqlExpr.StringLiteral(expr.time))
        sqlBuilder.append(")")

    override def printFuncExpr(expr: SqlExpr.Func): Unit =
        if expr.name.toUpperCase == "STRING_AGG" && expr.param.isEmpty && expr.filter.isEmpty && expr.withinGroup.isEmpty then
            sqlBuilder.append("GROUP_CONCAT")
            sqlBuilder.append("(")
            printList(expr.args)(printExpr)
            if expr.orderBy.nonEmpty then
                sqlBuilder.append(" ORDER BY ")
                printList(expr.orderBy)(printOrderItem)
            sqlBuilder.append(")")
        else super.printFuncExpr(expr)

    override def printCastType(castType: SqlCastType): Unit =
        val t = castType match
            case SqlCastType.Varchar => "TEXT"
            case SqlCastType.Int4 => "INTEGER"
            case SqlCastType.Int8 => "INTEGER"
            case SqlCastType.Float4 => "REAL"
            case SqlCastType.Float8 => "REAL"
            case SqlCastType.DateTime => "TEXT"
            case SqlCastType.Json => "TEXT"
            case SqlCastType.Custom(c) => c
        sqlBuilder.append(t)

    override def printIntervalExpr(expr: SqlExpr.Interval): Unit = {}