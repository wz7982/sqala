package sqala.printer

import sqala.ast.expr.SqlExpr
import sqala.ast.limit.SqlLimit
import sqala.ast.statement.{SqlQuery, SqlStatement}

class MysqlPrinter(override val prepare: Boolean) extends SqlPrinter(prepare):
    override val leftQuote: String = "`"

    override val rightQuote: String = "`"

    override def printLimit(limit: SqlLimit): Unit =
        sqlBuilder.append(" LIMIT ")
        printExpr(limit.offset)
        sqlBuilder.append(", ")
        printExpr(limit.limit)

    override def printIntervalExpr(expr: SqlExpr.Interval): Unit =
        sqlBuilder.append("INTERVAL '")
        sqlBuilder.append(expr.value)
        sqlBuilder.append("' ")
        sqlBuilder.append(expr.unit.get.unit)

    override def printValues(values: SqlQuery.Values): Unit =
        sqlBuilder.append("VALUES ")
        printList(values.values.map(SqlExpr.Vector(_))): v =>
            sqlBuilder.append("ROW")
            printExpr(v)

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