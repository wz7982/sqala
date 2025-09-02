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
            case SqlBinaryOperator.EuclideanDistance =>
                val func = 
                    SqlExpr.Func("DISTANCE", expr.left :: expr.right :: SqlExpr.StringLiteral("EUCLIDEAN") :: Nil)
                printExpr(func)
            case SqlBinaryOperator.CosineDistance =>
                val func =
                    SqlExpr.Func("DISTANCE", expr.left :: expr.right :: SqlExpr.StringLiteral("COSINE") :: Nil)
                printExpr(func)
            case SqlBinaryOperator.DotDistance =>
                val func =
                    SqlExpr.Func("DISTANCE", expr.left :: expr.right :: SqlExpr.StringLiteral("DOT") :: Nil)
                printExpr(func)
            case SqlBinaryOperator.Concat =>
                val func = SqlExpr.Func("CONCAT", expr.left :: expr.right :: Nil)
                printExpr(func)
            case _ =>
                super.printBinaryExpr(expr) 

    override def printVectorExpr(expr: SqlExpr.Vector): Unit =
        sqlBuilder.append("STRING_TO_VECTOR(")
        super.printVectorExpr(expr)
        sqlBuilder.append(")")

    override def printFuncExpr(expr: SqlExpr.Func): Unit =
        if expr.name.equalsIgnoreCase("STRING_AGG") && expr.quantifier.isEmpty && expr.filter.isEmpty && expr.withinGroup.isEmpty then
            val (args, separator) = if expr.args.size == 2 then
                (expr.args.head :: Nil) -> expr.args.last
            else
                expr.args -> SqlExpr.StringLiteral("")
            sqlBuilder.append("GROUP_CONCAT")
            sqlBuilder.append("(")
            printList(args)(printExpr)
            if expr.orderBy.nonEmpty then
                sqlBuilder.append(" ORDER BY ")
                printList(expr.orderBy)(printOrderingItem)
            sqlBuilder.append(" SEPARATOR ")
            printExpr(separator)
            sqlBuilder.append(")")
        else super.printFuncExpr(expr)

    override def printValues(values: SqlQuery.Values): Unit =
        printSpace()
        sqlBuilder.append("VALUES ")
        printList(values.values.map(SqlExpr.Tuple(_))): v =>
            sqlBuilder.append("ROW")
            printExpr(v)

    override def printType(`type`: SqlType): Unit =
        `type` match
            case SqlType.Varchar(maxLength) =>
                sqlBuilder.append(s"CHAR${maxLength.map(l => s"($l)").getOrElse("")}")
            case SqlType.Int => 
                sqlBuilder.append("SIGNED")
            case SqlType.Long => 
                sqlBuilder.append("SIGNED")
            case _ =>
                super.printType(`type`)

    override def printOrderingItem(orderBy: SqlOrderingItem): Unit =
        val order = orderBy.ordering match
            case None | Some(Asc) => Asc
            case _ => Desc
        val orderExpr = 
            SqlExpr.Case(SqlWhen(SqlExpr.NullTest(orderBy.expr, false), SqlExpr.NumberLiteral(1)) :: Nil, Some(SqlExpr.NumberLiteral(0)))
        (order, orderBy.nullsOrdering) match
            case (_, None) | (Asc, Some(First)) | (Desc, Some(Last)) =>
                printExpr(orderBy.expr)
                sqlBuilder.append(s" ${order.order}")
            case (Asc, Some(Last)) | (Desc, Some(First)) =>
                printExpr(orderExpr)
                sqlBuilder.append(s" ${order.order},\n")
                printSpace()
                printExpr(orderBy.expr)
                sqlBuilder.append(s" ${order.order}")