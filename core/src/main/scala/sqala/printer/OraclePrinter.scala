package sqala.printer

import sqala.ast.expr.{SqlExpr, SqlTimeLiteralUnit, SqlTimeZoneMode, SqlType}
import sqala.ast.table.SqlTableAlias

class OraclePrinter(override val standardEscapeStrings: Boolean) extends SqlPrinter(standardEscapeStrings):
    override def printCteRecursive(): Unit =
        ()

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