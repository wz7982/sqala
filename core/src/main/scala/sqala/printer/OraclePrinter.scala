package sqala.printer

import sqala.ast.expr.{SqlBinaryOperator, SqlExpr, SqlType, SqlVectorDistanceMode}
import sqala.ast.statement.SqlStatement
import sqala.ast.table.SqlTableAlias

class OraclePrinter(override val standardEscapeStrings: Boolean) extends SqlPrinter(standardEscapeStrings):
    override def printCteRecursive(): Unit = {}

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
        sqlBuilder.append(" FROM ")
        printIdent("dual")
        sqlBuilder.append(") ")
        printIdent("t2")

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

    override def printVectorDistanceFuncExpr(expr: SqlExpr.VectorDistanceFunc): Unit =
        expr.mode match
            case SqlVectorDistanceMode.Euclidean =>
                printExpr(
                    SqlExpr.StandardFunc(
                        "L2_DISTANCE", 
                        expr.left :: expr.right :: Nil,
                        None,
                        Nil,
                        Nil,
                        None
                    )
                )
            case SqlVectorDistanceMode.Cosine =>
                printExpr(
                    SqlExpr.StandardFunc(
                        "COSINE_DISTANCE", 
                        expr.left :: expr.right :: Nil,
                        None,
                        Nil,
                        Nil,
                        None
                    )
                )
            case SqlVectorDistanceMode.Dot =>
                printExpr(
                    SqlExpr.Binary(
                        SqlExpr.StandardFunc(
                            "INNER_PRODUCT", 
                            expr.left :: expr.right :: Nil,
                            None,
                            Nil,
                            Nil,
                            None
                        ),
                        SqlBinaryOperator.Times,
                        SqlExpr.NumberLiteral(-1)
                    )
                )
            case SqlVectorDistanceMode.Manhattan =>
                printExpr(
                    SqlExpr.StandardFunc(
                        "L1_DISTANCE", 
                        expr.left :: expr.right :: Nil,
                        None,
                        Nil,
                        Nil,
                        None
                    )
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
        sqlBuilder.append(" ")
        printIdent(alias.tableAlias)
        if alias.columnAlias.nonEmpty then
            sqlBuilder.append("(")
            printList(alias.columnAlias)(i => printIdent(i))
            sqlBuilder.append(")")