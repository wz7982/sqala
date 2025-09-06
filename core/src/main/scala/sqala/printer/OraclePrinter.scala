package sqala.printer

import sqala.ast.expr.*
import sqala.ast.statement.SqlStatement
import sqala.ast.table.{SqlTable, SqlTableAlias}

class OraclePrinter(override val enableJdbcPrepare: Boolean) extends SqlPrinter(enableJdbcPrepare):
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
        sqlBuilder.append(s" FROM ${leftQuote}dual$rightQuote) ${leftQuote}t2$rightQuote")

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

    override def printVectorDistanceFuncExpr(expr: SqlExpr.VectorDistanceFunc): Unit =
        expr.mode match
            case SqlVectorDistanceMode.Euclidean =>
                printExpr(
                    SqlExpr.StandardValueFunc("L2_DISTANCE", expr.left :: expr.right :: Nil)
                )
            case SqlVectorDistanceMode.Cosine =>
                printExpr(
                    SqlExpr.StandardValueFunc("COSINE_DISTANCE", expr.left :: expr.right :: Nil)
                )
            case SqlVectorDistanceMode.Dot =>
                printExpr(
                    SqlExpr.Binary(
                        SqlExpr.StandardValueFunc("INNER_PRODUCT", expr.left :: expr.right :: Nil),
                        SqlBinaryOperator.Times,
                        SqlExpr.NumberLiteral(-1)
                    )
                )
            case SqlVectorDistanceMode.Manhattan =>
                printExpr(
                    SqlExpr.StandardValueFunc("L1_DISTANCE", expr.left :: expr.right :: Nil)
                )

    override def printType(`type`: SqlType): Unit =
        `type` match
            case SqlType.Varchar(None) => 
                sqlBuilder.append("VARCHAR(4000)")
            case SqlType.Long =>
                sqlBuilder.append("INT")
            case _ =>
                super.printType(`type`)

    override def printTableAlias(alias: SqlTableAlias): Unit =
        sqlBuilder.append(s" $leftQuote${alias.tableAlias}$rightQuote")
        if alias.columnAlias.nonEmpty then
            sqlBuilder.append("(")
            printList(alias.columnAlias)(i => sqlBuilder.append(s"$leftQuote$i$rightQuote"))
            sqlBuilder.append(")")

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