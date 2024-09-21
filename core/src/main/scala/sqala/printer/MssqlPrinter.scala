package sqala.printer

import sqala.ast.expr.{SqlBinaryOperator, SqlExpr}
import sqala.ast.limit.SqlLimit
import sqala.ast.statement.SqlStatement

class MssqlPrinter(override val prepare: Boolean, override val indent: Int) extends SqlPrinter(prepare, indent):
    override val leftQuote: String = "["

    override val rightQuote: String = "]"

    override def printLimit(limit: SqlLimit): Unit =
        sqlBuilder.append("OFFSET ")
        printExpr(limit.offset)
        sqlBuilder.append(" ROWS FETCH NEXT ")
        printExpr(limit.limit)
        sqlBuilder.append(" ROWS ONLY")

    override def printCteRecursive(): Unit = {}

    override def printUpsert(upsert: SqlStatement.Upsert): Unit =
        sqlBuilder.append("MERGE INTO ")
        printTable(upsert.table)
        sqlBuilder.append(s" ${leftQuote}t1$rightQuote")

        sqlBuilder.append(" USING (")
        sqlBuilder.append("SELECT ")
        for index <- upsert.columns.indices do
            printExpr(upsert.values(index))
            sqlBuilder.append(" AS ")
            printExpr(upsert.columns(index))
            if index < upsert.columns.size - 1 then
                sqlBuilder.append(",")
                sqlBuilder.append(" ")
        sqlBuilder.append(s") ${leftQuote}t2$rightQuote")

        sqlBuilder.append(" ON (")
        for index <- upsert.pkList.indices do
            sqlBuilder.append(s"${leftQuote}t1$rightQuote.")
            printExpr(upsert.pkList(index))
            sqlBuilder.append(" = ")
            sqlBuilder.append(s"${leftQuote}t2$rightQuote.")
            printExpr(upsert.pkList(index))
            if index < upsert.pkList.size - 1 then
                sqlBuilder.append(" AND ")
        sqlBuilder.append(")")

        sqlBuilder.append(" WHEN MATCHED THEN UPDATE SET ")
        printList(upsert.updateList): u =>
            sqlBuilder.append(s"${leftQuote}t1$rightQuote.")
            printExpr(u)
            sqlBuilder.append(" = ")
            sqlBuilder.append(s"${leftQuote}t2$rightQuote.")
            printExpr(u)

        sqlBuilder.append(" WHEN NOT MATCHED THEN INSERT (")
        printList(upsert.columns): c =>
            sqlBuilder.append(s"${leftQuote}t1$rightQuote.")
            printExpr(c)
        sqlBuilder.append(")")

        sqlBuilder.append(" VALUES (")
        printList(upsert.values)(printExpr)
        sqlBuilder.append(")")

    override def printBinaryExpr(expr: SqlExpr.Binary): Unit = expr match
        case SqlExpr.Binary(left, op, SqlExpr.Interval(value, unit)) =>
            val printValue = op match
                case SqlBinaryOperator.Minus => value * -1
                case _ => value
            sqlBuilder.append("DATEADD(")
            sqlBuilder.append(unit.unit)
            sqlBuilder.append(", ")
            sqlBuilder.append(printValue)
            sqlBuilder.append(", ")
            printExpr(left)
            sqlBuilder.append(")")
        case _ => super.printBinaryExpr(expr)

    override def printIntervalExpr(expr: SqlExpr.Interval): Unit = {}

    override def printExtractExpr(expr: SqlExpr.Extract): Unit =
        sqlBuilder.append("DATEPART(")
        sqlBuilder.append(expr.unit.unit)
        sqlBuilder.append(", ")
        printExpr(expr.expr)
        sqlBuilder.append(")")