package sqala.printer

import sqala.ast.expr.SqlExpr
import sqala.ast.limit.SqlLimit
import sqala.ast.statement.SqlStatement

class SqlitePrinter(override val prepare: Boolean) extends SqlPrinter(prepare):
    override def printLimit(limit: SqlLimit): Unit =
        if prepare then
            sqlBuilder.append(" LIMIT ?, ?")
            args.append(limit.offset)
            args.append(limit.limit)
        else
            sqlBuilder.append(s" LIMIT ${limit.offset}, ${limit.limit}")

    override def printUpsert(upsert: SqlStatement.Upsert): Unit =
        sqlBuilder.append("INSERT OR REPLACE INTO ")
        printTable(upsert.table)

        sqlBuilder.append(" (")
        printList(upsert.columns)(printExpr)
        sqlBuilder.append(")")

        sqlBuilder.append(" VALUES (")
        printList(upsert.values)(printExpr)
        sqlBuilder.append(")")

    override def printIntervalExpr(expr: SqlExpr.Interval): Unit = {}