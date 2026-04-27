package sqala.printer

import sqala.ast.expr.{SqlExpr, SqlTimeLiteralUnit, SqlTimeZoneMode, SqlType}
import sqala.ast.statement.SqlStatement
import sqala.ast.table.SqlTableAlias

class OraclePrinter(override val standardEscapeStrings: Boolean) extends SqlPrinter(standardEscapeStrings):
    override def printCteRecursive(): Unit =
        ()

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
        printIdent("DUAL")
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

    override def printTimeLiteralExpr(expr: SqlExpr.TimeLiteral): Unit =
        expr.unit match
            case SqlTimeLiteralUnit.Timestamp(Some(SqlTimeZoneMode.With)) =>
                val func = SqlExpr.GeneralFunc(
                    None,
                    "TO_TIMESTAMP_TZ",
                    SqlExpr.StringLiteral(expr.time) :: SqlExpr.StringLiteral("YYYY-MM-DD HH24:MI:SS.FF9 TZH:TZM") :: Nil,
                    Nil,
                    Nil,
                    None
                )
                printExpr(func)
            case SqlTimeLiteralUnit.Timestamp(_) =>
                val func = SqlExpr.GeneralFunc(
                    None,
                    "TO_TIMESTAMP",
                    SqlExpr.StringLiteral(expr.time) :: SqlExpr.StringLiteral("YYYY-MM-DD HH24:MI:SS.FF9") :: Nil,
                    Nil,
                    Nil,
                    None
                )
                printExpr(func)
            case SqlTimeLiteralUnit.Date =>
                val func = SqlExpr.GeneralFunc(
                    None,
                    "TO_DATE",
                    SqlExpr.StringLiteral(expr.time) :: SqlExpr.StringLiteral("YYYY-MM-DD") :: Nil,
                    Nil,
                    Nil,
                    None
                )
                printExpr(func)
            case _ =>
                super.printTimeLiteralExpr(expr)

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
        printIdent(alias.alias)
        if alias.columnAliases.nonEmpty then
            sqlBuilder.append("(")
            printList(alias.columnAliases)(i => printIdent(i))
            sqlBuilder.append(")")