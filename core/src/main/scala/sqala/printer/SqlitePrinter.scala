package sqala.printer

import sqala.ast.expr.SqlExpr
import sqala.ast.limit.{SqlFetchMode, SqlFetchUnit, SqlLimit}
import sqala.ast.statement.SqlStatement

/**
 * SQLite dialect printer.
 */
class SqlitePrinter(override val standardEscapeStrings: Boolean) extends StandardSqlPrinter(standardEscapeStrings):
    override def printLimit(limit: SqlLimit): Unit =
        val standardMode = limit.fetch match
            case None | Some(_, SqlFetchUnit.RowCount, SqlFetchMode.Only) =>
                false
            case _ =>
                true

        if standardMode then
            super.printLimit(limit)
        else
            sqlBuilder.append("LIMIT ")
            printExpr(limit.fetch.map(_.limit).getOrElse(SqlExpr.NumberLiteral(Long.MaxValue)))
            for f <- limit.offset do
                sqlBuilder.append(" OFFSET ")
                printExpr(f)

    override def printUpsertStatement(statement: SqlStatement.Upsert): Unit =
        sqlBuilder.append("INSERT OR REPLACE INTO ")
        printTable(statement.table)

        sqlBuilder.append(" (")
        printList(statement.columns.toList)(printIdent)
        sqlBuilder.append(")")

        sqlBuilder.append(" VALUES (")
        printList(statement.values.toList)(printExpr)
        sqlBuilder.append(")")

    override def printListAggFuncExpr(expr: SqlExpr.ListAggFunc): Unit =
        if expr.onOverflow.nonEmpty then
            super.printListAggFuncExpr(expr)
        else
            val func = SqlExpr.GeneralFunc(
                expr.quantifier,
                "GROUP_CONCAT",
                expr.expr :: expr.separator :: Nil,
                expr.withinGroup,
                Nil,
                None
            )
            printExpr(func)