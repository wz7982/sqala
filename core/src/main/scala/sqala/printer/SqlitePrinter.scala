package sqala.printer

import sqala.ast.expr.SqlExpr
import sqala.ast.limit.SqlLimit
import sqala.ast.statement.SqlStatement

class SqlitePrinter(override val standardEscapeStrings: Boolean) extends SqlPrinter(standardEscapeStrings):
    override def printLimit(limit: SqlLimit): Unit =
        sqlBuilder.append("LIMIT ")
        printExpr(limit.fetch.map(_.limit).getOrElse(SqlExpr.NumberLiteral(Long.MaxValue)))
        for f <- limit.offset do
            sqlBuilder.append(" OFFSET ")
            printExpr(f)

    override def printUpsert(upsert: SqlStatement.Upsert): Unit =
        sqlBuilder.append("INSERT OR REPLACE INTO ")
        printTable(upsert.table)

        sqlBuilder.append(" (")
        printList(upsert.columns)(printIdent)
        sqlBuilder.append(")")

        sqlBuilder.append(" VALUES (")
        printList(upsert.values)(printExpr)
        sqlBuilder.append(")")

    override def printListAggFuncExpr(expr: SqlExpr.ListAggFunc): Unit =
        val func = SqlExpr.GeneralFunc(
            expr.quantifier,
            "GROUP_CONCAT",
            expr.expr :: expr.separator :: Nil,
            expr.withinGroup,
            Nil,
            expr.filter
        )
        printExpr(func)