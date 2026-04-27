package sqala.printer

import sqala.ast.expr.SqlExpr
import sqala.ast.limit.{SqlFetchMode, SqlFetchUnit, SqlLimit}
import sqala.ast.statement.SqlStatement

class PostgresqlPrinter(override val standardEscapeStrings: Boolean) extends SqlPrinter(standardEscapeStrings):
    override def printLimit(limit: SqlLimit): Unit =
        val standardMode = limit.fetch match
            case None | Some(_, SqlFetchUnit.RowCount, SqlFetchMode.Only) =>
                false
            case _ =>
                true
        if standardMode then super.printLimit(limit)
        else
            for f <- limit.fetch do
                sqlBuilder.append("LIMIT ")
                printExpr(f.limit)
            for o <- limit.offset do
                if limit.fetch.isDefined then
                    sqlBuilder.append(" ")
                sqlBuilder.append("OFFSET ")
                printExpr(o)

    override def printUpsert(upsert: SqlStatement.Upsert): Unit =
        sqlBuilder.append("INSERT INTO ")
        printTable(upsert.table)

        sqlBuilder.append(" (")
        printList(upsert.columns)(printIdent)
        sqlBuilder.append(")")

        sqlBuilder.append(" VALUES (")
        printList(upsert.values)(printExpr)
        sqlBuilder.append(")")

        sqlBuilder.append(" ON CONFLICT (")
        printList(upsert.pkList)(printIdent)
        sqlBuilder.append(")")

        sqlBuilder.append(" DO UPDATE SET ")

        printList(upsert.updateList): u =>
            printIdent(u)
            sqlBuilder.append(" = ")
            sqlBuilder.append("EXCLUDED.")
            printIdent(u)

    override def printListAggFuncExpr(expr: SqlExpr.ListAggFunc): Unit =
        val func = SqlExpr.GeneralFunc(
            expr.quantifier,
            "STRING_AGG",
            expr.expr :: expr.separator :: Nil,
            expr.withinGroup,
            Nil,
            expr.filter
        )
        printExpr(func)