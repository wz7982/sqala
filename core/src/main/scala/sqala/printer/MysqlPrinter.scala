package sqala.printer

import sqala.ast.expr.*
import sqala.ast.limit.SqlLimit
import sqala.ast.order.SqlNullsOrdering.{First, Last}
import sqala.ast.order.SqlOrdering.{Asc, Desc}
import sqala.ast.order.SqlOrderingItem
import sqala.ast.statement.{SqlQuery, SqlStatement}

class MysqlPrinter(override val enableJdbcPrepare: Boolean) extends SqlPrinter(enableJdbcPrepare):
    override val leftQuote: String = "`"

    override val rightQuote: String = "`"

    override def printLimit(limit: SqlLimit): Unit =
        sqlBuilder.append("LIMIT ")
        printExpr(limit.offset.getOrElse(SqlExpr.NumberLiteral(0L)))
        sqlBuilder.append(", ")
        printExpr(limit.fetch.map(_.limit).getOrElse(SqlExpr.NumberLiteral(Long.MaxValue)))

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

    override def printBinaryExpr(expr: SqlExpr.Binary): Unit =
        expr.operator match
            case SqlBinaryOperator.Concat =>
                val concat = SqlExpr.Func("CONCAT", expr.left :: expr.right :: Nil)
                printExpr(concat)
            case _ =>
                super.printBinaryExpr(expr) 

    override def printIntervalExpr(expr: SqlExpr.Interval): Unit =
        sqlBuilder.append("INTERVAL '")
        sqlBuilder.append(expr.value)
        sqlBuilder.append("' ")
        sqlBuilder.append(expr.unit.unit)

    override def printFuncExpr(expr: SqlExpr.Func): Unit =
        if expr.name.equalsIgnoreCase("STRING_AGG") && expr.quantifier.isEmpty && expr.filter.isEmpty && expr.withinGroup.isEmpty then
            val (args, separator) = if expr.args.size == 2 then
                (expr.args.head :: Nil) -> expr.args.last
            else
                expr.args -> SqlExpr.StringLiteral("")
            sqlBuilder.append("GROUP_CONCAT")
            sqlBuilder.append("(")
            printList(args)(printExpr)
            if expr.orderBy.nonEmpty then
                sqlBuilder.append(" ORDER BY ")
                printList(expr.orderBy)(printOrderingItem)
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

    override def printOrderingItem(orderBy: SqlOrderingItem): Unit =
        val order = orderBy.ordering match
            case None | Some(Asc) => Asc
            case _ => Desc
        val orderExpr = 
            SqlExpr.Case(SqlWhen(SqlExpr.NullTest(orderBy.expr, false), SqlExpr.NumberLiteral(1)) :: Nil, Some(SqlExpr.NumberLiteral(0)))
        (order, orderBy.nullsOrdering) match
            case (_, None) | (Asc, Some(First)) | (Desc, Some(Last)) =>
                printExpr(orderBy.expr)
                sqlBuilder.append(s" ${order.order}")
            case (Asc, Some(Last)) | (Desc, Some(First)) =>
                printExpr(orderExpr)
                sqlBuilder.append(s" ${order.order},\n")
                printSpace()
                printExpr(orderBy.expr)
                sqlBuilder.append(s" ${order.order}")