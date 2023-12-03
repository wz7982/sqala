package sqala.printer

import sqala.ast.expr.SqlExpr
import sqala.ast.limit.SqlLimit
import sqala.ast.statement.{SqlQuery, SqlStatement}

class SqlServerPrinter(override val prepare: Boolean) extends SqlPrinter(prepare):
    override def printLimit(limit: SqlLimit): Unit =
        if prepare then
            sqlBuilder.append(" OFFSET ? ROWS FETCH NEXT ? ROWS ONLY")
            args.append(limit.offset)
            args.append(limit.limit)
        else
            sqlBuilder.append(s" OFFSET ${limit.offset} ROWS FETCH NEXT ${limit.limit} ROWS ONLY")

    override def printForUpdate(): Unit = sqlBuilder.append(" WITH (UPDLOCK)")

    override def printSelect(select: SqlQuery.Select): Unit =
        sqlBuilder.append("SELECT ")

        select.param.foreach(p => sqlBuilder.append(p + " "))

        if select.select.isEmpty then sqlBuilder.append("*") else printList(select.select)(printSelectItem)

        for _ <- select.from do
            sqlBuilder.append(" FROM ")
            printList(select.from)(printTable)

        if select.forUpdate then
            printForUpdate()

        for w <- select.where do
            sqlBuilder.append(" WHERE ")
            printExpr(w)

        if select.groupBy.nonEmpty then
            sqlBuilder.append(" GROUP BY ")
            printList(select.groupBy)(printExpr)

        for h <- select.having do
            sqlBuilder.append(" HAVING ")
            printExpr(h)

        if select.orderBy.nonEmpty then
            sqlBuilder.append(" ORDER BY ")
            printList(select.orderBy)(printOrderBy)

        for l <- select.limit do printLimit(l)

    override def printCteRecursive(): Unit = {}

    override def printUpsert(upsert: SqlStatement.Upsert): Unit =
        sqlBuilder.append("MERGE INTO ")
        printTable(upsert.table)
        sqlBuilder.append(s" ${quote}t1$quote")

        sqlBuilder.append(" USING (")
        sqlBuilder.append("SELECT ")
        for index <- upsert.columns.indices do
            printExpr(upsert.values(index))
            sqlBuilder.append(" AS ")
            printExpr(upsert.columns(index))
            if index < upsert.columns.size - 1 then
                sqlBuilder.append(",")
                sqlBuilder.append(" ")
        sqlBuilder.append(s") ${quote}t2$quote")

        sqlBuilder.append(" ON (")
        for index <- upsert.pkList.indices do
            sqlBuilder.append(s"${quote}t1$quote.")
            printExpr(upsert.pkList(index))
            sqlBuilder.append(" = ")
            sqlBuilder.append(s"${quote}t2$quote.")
            printExpr(upsert.pkList(index))
            if index < upsert.pkList.size - 1 then
                sqlBuilder.append(" AND ")
        sqlBuilder.append(")")

        sqlBuilder.append(" WHEN MATCHED THEN UPDATE SET ")
        printList(upsert.updateList): u =>
            sqlBuilder.append(s"${quote}t1$quote.")
            printExpr(u)
            sqlBuilder.append(" = ")
            sqlBuilder.append(s"${quote}t2$quote.")
            printExpr(u)

        sqlBuilder.append(" WHEN NOT MATCHED THEN INSERT (")
        printList(upsert.columns): c =>
            sqlBuilder.append(s"${quote}t1$quote.")
            printExpr(c)
        sqlBuilder.append(")")

        sqlBuilder.append(" VALUES (")
        printList(upsert.values)(printExpr)
        sqlBuilder.append(")")

    override def printIntervalExpr(expr: SqlExpr.Interval): Unit = {}