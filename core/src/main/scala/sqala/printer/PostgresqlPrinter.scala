package sqala.printer

import sqala.ast.expr.{SqlBinaryOperator, SqlExpr, SqlVectorDistanceMode}
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
        printList(upsert.columns)(printExpr)
        sqlBuilder.append(")")

        sqlBuilder.append(" VALUES (")
        printList(upsert.values)(printExpr)
        sqlBuilder.append(")")

        sqlBuilder.append(" ON CONFLICT (")
        printList(upsert.pkList)(printExpr)
        sqlBuilder.append(")")

        sqlBuilder.append(" DO UPDATE SET ")

        printList(upsert.updateList): u =>
            printExpr(u)
            sqlBuilder.append(" = ")
            sqlBuilder.append("EXCLUDED.")
            printExpr(u)

    override def printListAggFuncExpr(expr: SqlExpr.ListAggFunc): Unit =
        sqlBuilder.append("STRING_AGG(")
        expr.quantifier.foreach: q => 
            printQuantifier(q)
            sqlBuilder.append(" ")
        printExpr(expr.expr)
        sqlBuilder.append(", ")
        printExpr(expr.separator)
        if expr.withinGroup.nonEmpty then
            sqlBuilder.append(" ORDER BY ")
            printList(expr.withinGroup)(printOrderingItem)
        sqlBuilder.append(")")
        for f <- expr.filter do
            sqlBuilder.append(" FILTER (WHERE ")
            printExpr(f)
            sqlBuilder.append(")")

    override def printVectorDistanceFuncExpr(expr: SqlExpr.VectorDistanceFunc): Unit =
        expr.mode match
            case SqlVectorDistanceMode.Euclidean =>
                printExpr(
                    SqlExpr.Binary(expr.left, SqlBinaryOperator.Custom("<->"), expr.right)
                )
            case SqlVectorDistanceMode.Cosine =>
                printExpr(
                    SqlExpr.Binary(expr.left, SqlBinaryOperator.Custom("<=>"), expr.right)
                )
            case SqlVectorDistanceMode.Dot =>
                printExpr(
                    SqlExpr.Binary(expr.left, SqlBinaryOperator.Custom("<#>"), expr.right)
                )
            case SqlVectorDistanceMode.Manhattan =>
                printExpr(
                    SqlExpr.Binary(expr.left, SqlBinaryOperator.Custom("<+>"), expr.right)
                )