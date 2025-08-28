package sqala.printer

import sqala.ast.expr.{SqlCastType, SqlExpr}
import sqala.ast.statement.SqlStatement
import sqala.ast.table.SqlTable

class Db2Printer(override val enableJdbcPrepare: Boolean) extends SqlPrinter(enableJdbcPrepare):
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
        sqlBuilder.append(" (")
        printList(upsert.columns): c =>
            printExpr(c)
        sqlBuilder.append(")")

        sqlBuilder.append(" VALUES (")
        printList(upsert.values)(printExpr)
        sqlBuilder.append(")")

    override def printCteRecursive(): Unit = {}

    override def printFuncExpr(expr: SqlExpr.Func): Unit =
        if expr.name.equalsIgnoreCase("STRING_AGG") && expr.quantifier.isEmpty && expr.filter.isEmpty && expr.withinGroup.isEmpty then
            sqlBuilder.append("LISTAGG")
            sqlBuilder.append("(")
            printList(expr.args)(printExpr)
            sqlBuilder.append(")")
            if expr.orderBy.nonEmpty then
                sqlBuilder.append(" WITHIN GROUP (ORDER BY ")
                printList(expr.orderBy)(printOrderingItem)
                sqlBuilder.append(")")
        else super.printFuncExpr(expr)

    override def printCastType(castType: SqlCastType): Unit =
        val t = castType match
            case SqlCastType.Varchar => "VARCHAR"
            case SqlCastType.Int4 => "INTEGER"
            case SqlCastType.Int8 => "BIGINT"
            case SqlCastType.Float4 => "REAL"
            case SqlCastType.Float8 => "DOUBLE"
            case SqlCastType.DateTime => "TIMESTAMP"
            case SqlCastType.Json => "JSON"
            case SqlCastType.Custom(c) => c
        sqlBuilder.append(t)

    override def printIntervalExpr(expr: SqlExpr.Interval): Unit =
        sqlBuilder.append("INTERVAL '")
        sqlBuilder.append(expr.value)
        sqlBuilder.append("' ")
        sqlBuilder.append(expr.unit.unit)

    override def printTable(table: SqlTable): Unit =
        table match
            case SqlTable.Func(functionName, args, alias) =>
                sqlBuilder.append("TABLE(")
                sqlBuilder.append(s"$functionName")
                sqlBuilder.append("(")
                printList(args)(printExpr)
                sqlBuilder.append("))")
                for a <- alias do
                    printTableAlias(a)
            case _ => super.printTable(table)