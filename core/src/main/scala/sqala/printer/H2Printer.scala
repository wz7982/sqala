package sqala.printer

import sqala.ast.expr.SqlExpr
import sqala.ast.statement.SqlStatement

class H2Printer(override val standardEscapeStrings: Boolean) extends SqlPrinter(standardEscapeStrings):
    override def printUpsert(upsert: SqlStatement.Upsert): Unit =
        sqlBuilder.append("MERGE INTO ")
        printTable(upsert.table)
        sqlBuilder.append(" ")
        printIdent("t1")

        sqlBuilder.append(" USING (")
        sqlBuilder.append("SELECT ")
        for index <- upsert.columns.indices do
            printExpr(upsert.values(index))
            sqlBuilder.append(" AS ")
            printExpr(upsert.columns(index))
            if index < upsert.columns.size - 1 then
                sqlBuilder.append(",")
                sqlBuilder.append(" ")
        sqlBuilder.append(") ")
        printIdent("t1")

        sqlBuilder.append(" ON (")
        for index <- upsert.pkList.indices do
            printIdent("t1")
            sqlBuilder.append(".")
            printExpr(upsert.pkList(index))
            sqlBuilder.append(" = ")
            printIdent("t2")
            sqlBuilder.append(".")
            printExpr(upsert.pkList(index))
            if index < upsert.pkList.size - 1 then
                sqlBuilder.append(" AND ")
        sqlBuilder.append(")")

        sqlBuilder.append(" WHEN MATCHED THEN UPDATE SET ")
        printList(upsert.updateList): u =>
            printIdent("t1")
            sqlBuilder.append(".")
            printExpr(u)
            sqlBuilder.append(" = ")
            printIdent("t2")
            sqlBuilder.append(".")
            printExpr(u)

        sqlBuilder.append(" WHEN NOT MATCHED THEN INSERT (")
        printList(upsert.columns): c =>
            printExpr(c)
        sqlBuilder.append(")")

        sqlBuilder.append(" VALUES (")
        printList(upsert.values)(printExpr)
        sqlBuilder.append(")")

    override def printVectorDistanceFuncExpr(expr: SqlExpr.VectorDistanceFunc): Unit = {}