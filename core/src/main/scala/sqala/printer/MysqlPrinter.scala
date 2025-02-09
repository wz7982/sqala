package sqala.printer

import sqala.ast.expr.SqlExpr.*
import sqala.ast.expr.{SqlCase, SqlCastType, SqlExpr}
import sqala.ast.limit.SqlLimit
import sqala.ast.statement.{SqlQuery, SqlStatement}
import sqala.ast.order.SqlOrderBy
import sqala.ast.order.SqlOrderByNullsOption.*
import sqala.ast.order.SqlOrderByOption.*

class MysqlPrinter(override val enableJdbcPrepare: Boolean) extends SqlPrinter(enableJdbcPrepare):
    override val leftQuote: String = "`"

    override val rightQuote: String = "`"

    override def printLimit(limit: SqlLimit): Unit =
        sqlBuilder.append("LIMIT ")
        printExpr(limit.offset)
        sqlBuilder.append(", ")
        printExpr(limit.limit)

    override def printUpsert(upsert: SqlStatement.Upsert): Unit =
        sqlBuilder.append("INSERT INTO ")
        printTable(upsert.table)

        sqlBuilder.append(" (")
        printList(upsert.columns)(printExpr)
        sqlBuilder.append(")")

        sqlBuilder.append(" VALUES (")
        printList(upsert.values)(printExpr)
        sqlBuilder.append(")")

        sqlBuilder.append(" ON DUPLICATE KEY UPDATE ")

        printList(upsert.updateList): u =>
            printExpr(u)
            sqlBuilder.append(" = VALUES (")
            printExpr(u)
            sqlBuilder.append(")")

    override def printIntervalExpr(expr: SqlExpr.Interval): Unit =
        sqlBuilder.append("INTERVAL '")
        sqlBuilder.append(expr.value)
        sqlBuilder.append("' ")
        sqlBuilder.append(expr.unit.unit)

    override def printFuncExpr(expr: SqlExpr.Func): Unit =
        if expr.name.toUpperCase == "STRING_AGG" && expr.param.isEmpty && expr.filter.isEmpty && expr.withinGroup.isEmpty then
            val (args, separator) = if expr.args.size == 2 then
                (expr.args.head :: Nil) -> expr.args.last
            else
                expr.args -> SqlExpr.StringLiteral("")
            sqlBuilder.append("GROUP_CONCAT")
            sqlBuilder.append("(")
            printList(args)(printExpr)
            if expr.orderBy.nonEmpty then
                sqlBuilder.append(" ORDER BY ")
                printList(expr.orderBy)(printOrderBy)
            sqlBuilder.append(" SEPARATOR ")
            printExpr(separator)
            sqlBuilder.append(")")
        else super.printFuncExpr(expr)

    override def printValues(values: SqlQuery.Values): Unit =
        printSpace()
        sqlBuilder.append("VALUES ")
        printList(values.values.map(SqlExpr.Tuple(_))): v =>
            sqlBuilder.append("ROW")
            printExpr(v)

    override def printCastType(castType: SqlCastType): Unit =
        val t = castType match
            case SqlCastType.Varchar => "CHAR"
            case SqlCastType.Int4 => "SIGNED"
            case SqlCastType.Int8 => "SIGNED"
            case SqlCastType.Float4 => "FLOAT"
            case SqlCastType.Float8 => "DOUBLE"
            case SqlCastType.DateTime => "DATETIME"
            case SqlCastType.Json => "JSON"
            case SqlCastType.Custom(c) => c
        sqlBuilder.append(t)

    override def printOrderBy(orderBy: SqlOrderBy): Unit =
        val order = orderBy.order match
            case None | Some(Asc) => Asc
            case _ => Desc
        val orderExpr = Case(SqlCase(NullTest(orderBy.expr, false), NumberLiteral(1)) :: Nil, NumberLiteral(0))
        (order, orderBy.nullsOrder) match
            case (_, None) | (Asc, Some(First)) | (Desc, Some(Last)) =>
                printExpr(orderBy.expr)
                sqlBuilder.append(s" ${order.order}")
            case (Asc, Some(Last)) | (Desc, Some(First)) =>
                printExpr(orderExpr)
                sqlBuilder.append(s" ${order.order},\n")
                printSpace()
                printExpr(orderBy.expr)
                sqlBuilder.append(s" ${order.order}")