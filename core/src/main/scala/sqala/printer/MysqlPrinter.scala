package sqala.printer

import sqala.ast.expr.*
import sqala.ast.limit.SqlLimit
import sqala.ast.order.SqlNullsOrdering.{First, Last}
import sqala.ast.order.SqlOrdering.{Asc, Desc}
import sqala.ast.order.SqlOrderingItem
import sqala.ast.statement.{SqlQuery, SqlStatement}

class MysqlPrinter(override val enableJdbcPrepare: Boolean) extends SqlPrinter(enableJdbcPrepare):
    override val leftQuote: String = "`"

    override val rightQuote: String = "`"

    override def printLimit(limit: SqlLimit): Unit =
        sqlBuilder.append("LIMIT ")
        printExpr(limit.offset.getOrElse(SqlExpr.NumberLiteral(0L)))
        sqlBuilder.append(", ")
        printExpr(limit.fetch.map(_.limit).getOrElse(SqlExpr.NumberLiteral(Long.MaxValue)))

    override def printUpsert(upsert: SqlStatement.Upsert): Unit =
        sqlBuilder.append("INSERT INTO ")
        printTable(upsert.table)

        sqlBuilder.append(" (")
        printList(upsert.columns)(printExpr)
        sqlBuilder.append(")")

        sqlBuilder.append(" VALUES (")
        printList(upsert.values)(printExpr)
        sqlBuilder.append(")")

        sqlBuilder.append(" ON DUPLICATE KEY UPDATE ")

        printList(upsert.updateList): u =>
            printExpr(u)
            sqlBuilder.append(" = VALUES (")
            printExpr(u)
            sqlBuilder.append(")")

    override def printBinaryExpr(expr: SqlExpr.Binary): Unit =
        expr.operator match
            case SqlBinaryOperator.Concat =>
                printExpr(
                    SqlExpr.StandardFunc(
                        "CONCAT", 
                        expr.left :: expr.right :: Nil,
                        None,
                        Nil,
                        Nil,
                        None
                    )
                )
            case SqlBinaryOperator.IsNotDistinctFrom =>
                printExpr(
                    SqlExpr.Binary(
                        expr.left,
                        SqlBinaryOperator.Custom("<=>"),
                        expr.right
                    )
                )
            case SqlBinaryOperator.IsDistinctFrom =>
                printExpr(
                    SqlExpr.Unary(
                        SqlExpr.Binary(
                            expr.left,
                            SqlBinaryOperator.Custom("<=>"),
                            expr.right
                        ),
                        SqlUnaryOperator.Not
                    )
                )
            case _ =>
                super.printBinaryExpr(expr)

    override def printValues(values: SqlQuery.Values): Unit =
        printSpace()
        sqlBuilder.append("VALUES ")
        printList(values.values.map(SqlExpr.Tuple(_))): v =>
            sqlBuilder.append("ROW")
            printExpr(v)

    override def printType(`type`: SqlType): Unit =
        `type` match
            case SqlType.Varchar(maxLength) =>
                sqlBuilder.append("CHAR")
                for l <- maxLength do
                    sqlBuilder.append("(")
                    sqlBuilder.append(l)
                    sqlBuilder.append(")")
            case SqlType.Int => 
                sqlBuilder.append("SIGNED")
            case SqlType.Long => 
                sqlBuilder.append("SIGNED")
            case SqlType.Timestamp(None | Some(SqlTimeZoneMode.Without)) =>
                sqlBuilder.append("DATETIME")
            case _ =>
                super.printType(`type`)

    override def printListAggFuncExpr(expr: SqlExpr.ListAggFunc): Unit =
        sqlBuilder.append("GROUP_CONCAT(")
        expr.quantifier.foreach: q => 
            sqlBuilder.append(q.quantifier)
            sqlBuilder.append(" ")
        printExpr(expr.expr)
        if expr.withinGroup.nonEmpty then
            sqlBuilder.append(" ORDER BY ")
            printList(expr.withinGroup)(printOrderingItem)
        sqlBuilder.append(" SEPARATOR ")
        printExpr(expr.separator)
        sqlBuilder.append(")")
        for f <- expr.filter do
            sqlBuilder.append(" FILTER (WHERE ")
            printExpr(f)
            sqlBuilder.append(")")

    override def printVectorDistanceFuncExpr(expr: SqlExpr.VectorDistanceFunc): Unit =
        expr.mode match
            case SqlVectorDistanceMode.Euclidean =>
                printExpr(
                    SqlExpr.StandardFunc(
                        "DISTANCE", 
                        expr.left :: expr.right :: SqlExpr.StringLiteral("EUCLIDEAN") :: Nil,
                        None,
                        Nil,
                        Nil,
                        None
                    )
                )
            case SqlVectorDistanceMode.Cosine =>
                printExpr(
                    SqlExpr.StandardFunc(
                        "DISTANCE", 
                        expr.left :: expr.right :: SqlExpr.StringLiteral("COSINE") :: Nil,
                        None,
                        Nil,
                        Nil,
                        None
                    )
                )
            case SqlVectorDistanceMode.Dot =>
                printExpr(
                    SqlExpr.StandardFunc(
                        "DISTANCE", 
                        expr.left :: expr.right :: SqlExpr.StringLiteral("DOT") :: Nil,
                        None,
                        Nil,
                        Nil,
                        None
                    )
                )
            case SqlVectorDistanceMode.Manhattan =>

    override def printOrderingItem(orderBy: SqlOrderingItem): Unit =
        val order = orderBy.ordering match
            case None | Some(Asc) => Asc
            case _ => Desc
        val orderExpr = 
            SqlExpr.Case(
                SqlWhen(
                    SqlExpr.NullTest(orderBy.expr, false), 
                    SqlExpr.NumberLiteral(1)
                ) :: Nil, 
                Some(SqlExpr.NumberLiteral(0))
            )
        (order, orderBy.nullsOrdering) match
            case (_, None) | (Asc, Some(First)) | (Desc, Some(Last)) =>
                printExpr(orderBy.expr)
                sqlBuilder.append(" ")
                sqlBuilder.append(order.order)
            case (Asc, Some(Last)) | (Desc, Some(First)) =>
                printExpr(orderExpr)
                sqlBuilder.append(" ")
                sqlBuilder.append(order.order)
                sqlBuilder.append(",\n")
                printSpace()
                printExpr(orderBy.expr)
                sqlBuilder.append(" ")
                sqlBuilder.append(order.order)