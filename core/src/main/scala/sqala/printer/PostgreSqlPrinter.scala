package sqala.printer

import sqala.ast.expr.SqlExpr
import sqala.ast.statement.SqlStatement

class PostgreSqlPrinter(override val prepare: Boolean) extends SqlPrinter(prepare):
    override def printUpsert(upsert: SqlStatement.Upsert): Unit =
        sqlBuilder.append("INSERT INTO ")
        printTable(upsert.table)

        sqlBuilder.append(" (")
        printList(upsert.columns)(printExpr)
        sqlBuilder.append(")")

        sqlBuilder.append(" VALUES (")
        printList(upsert.values)(printExpr)
        sqlBuilder.append(")")

        sqlBuilder.append(" DO UPDATE SET ")

        printList(upsert.updateList): u =>
            printExpr(u)
            sqlBuilder.append(" = ")
            sqlBuilder.append("EXCLUDED.")
            printExpr(u)

    override def printIntervalExpr(expr: SqlExpr.Interval): Unit =
        sqlBuilder.append("INTERVAL")
        sqlBuilder.append(" '")
        sqlBuilder.append(expr.value)
        sqlBuilder.append("'")