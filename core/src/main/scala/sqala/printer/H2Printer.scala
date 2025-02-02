package sqala.printer

import sqala.ast.expr.*
import sqala.ast.statement.SqlStatement

class H2Printer extends SqlPrinter:
    override val leftQuote: String = "`"

    override val rightQuote: String = "`"

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
            printExpr(c)
        sqlBuilder.append(")")

        sqlBuilder.append(" VALUES (")
        printList(upsert.values)(printExpr)
        sqlBuilder.append(")")

    override def printFuncExpr(expr: SqlExpr.Func): Unit =
        if expr.name.toUpperCase == "STRING_AGG" && expr.param.isEmpty && expr.filter.isEmpty && expr.withinGroup.isEmpty then
            sqlBuilder.append("LISTAGG")
            sqlBuilder.append("(")
            printList(expr.args)(printExpr)
            sqlBuilder.append(")")
            if expr.orderBy.nonEmpty then
                sqlBuilder.append(" WITHIN GROUP (ORDER BY ")
                printList(expr.orderBy)(printOrderBy)
                sqlBuilder.append(")")
        else super.printFuncExpr(expr)

    override def printCastType(castType: SqlCastType): Unit =
        val t = castType match
            case SqlCastType.Varchar => "VARCHAR"
            case SqlCastType.Int4 => "INT4"
            case SqlCastType.Int8 => "INT8"
            case SqlCastType.Float4 => "FLOAT4"
            case SqlCastType.Float8 => "FLOAT8"
            case SqlCastType.DateTime => "TIMESTAMP"
            case SqlCastType.Json => "JSON"
            case SqlCastType.Custom(c) => c
        sqlBuilder.append(t)

    override def printIntervalExpr(expr: SqlExpr.Interval): Unit =
        sqlBuilder.append("INTERVAL '")
        sqlBuilder.append(expr.value)
        sqlBuilder.append("' ")
        sqlBuilder.append(expr.unit.unit)