package sqala.printer

import sqala.ast.expr.*
import sqala.ast.limit.*
import sqala.ast.statement.SqlStatement

class PostgresqlPrinter(override val enableJdbcPrepare: Boolean) extends SqlPrinter(enableJdbcPrepare):
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

    override def printType(`type`: SqlType): Unit =
        `type` match
            case SqlType.Json => 
                sqlBuilder.append("JSONB")
            case _ =>
                super.printType(`type`)

    override def printVectorExpr(expr: SqlExpr.Vector): Unit =
        printExpr(expr.expr)
        sqlBuilder.append(" :: VECTOR")

    override def printListAggFuncExpr(expr: SqlExpr.ListAggFunc): Unit =
        sqlBuilder.append("STRING_AGG(")
        expr.quantifier.foreach: q => 
            sqlBuilder.append(q.quantifier)
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