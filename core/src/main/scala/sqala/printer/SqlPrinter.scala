package sqala.printer

import sqala.ast.expr.SqlBinaryOperator.*
import sqala.ast.expr.{SqlExpr, SqlWindowFrame}
import sqala.ast.limit.SqlLimit
import sqala.ast.order.{SqlOrderBy, SqlOrderByOption}
import sqala.ast.statement.{SqlQuery, SqlSelectItem, SqlStatement, SqlWithItem}
import sqala.ast.table.{SqlJoinCondition, SqlTable, SqlTableAlias}

import scala.collection.mutable.ArrayBuffer

abstract class SqlPrinter(val prepare: Boolean):
    val sqlBuilder: StringBuilder = StringBuilder()

    val leftQuote: String = "\""

    val rightQuote: String = "\""

    val args: ArrayBuffer[Any] = ArrayBuffer()

    def sql: String = sqlBuilder.toString

    def printStatement(statement: SqlStatement): Unit = statement match
        case update: SqlStatement.Update => printUpdate(update)
        case insert: SqlStatement.Insert => printInsert(insert)
        case delete: SqlStatement.Delete => printDelete(delete)
        case upsert: SqlStatement.Upsert => printUpsert(upsert)
        case truncate: SqlStatement.Truncate => printTruncate(truncate)

    def printUpdate(update: SqlStatement.Update): Unit =
        sqlBuilder.append("UPDATE ")
        printTable(update.table)
        sqlBuilder.append(" SET ")

        printList(update.setList): i =>
            printExpr(i._1)
            sqlBuilder.append(" = ")
            printExpr(i._2)

        for i <- update.where do
            sqlBuilder.append(" WHERE ")
            printExpr(i)

    def printInsert(insert: SqlStatement.Insert): Unit =
        sqlBuilder.append("INSERT INTO ")
        printTable(insert.table)

        if insert.columns.nonEmpty then
            sqlBuilder.append(" (")
            printList(insert.columns)(printExpr)
            sqlBuilder.append(")")

        if insert.query.isDefined then
            sqlBuilder.append(" ")
            printQuery(insert.query.get)
        else
            sqlBuilder.append(" VALUES ")
            printList(insert.values.map(SqlExpr.Vector(_)))(printExpr)

    def printDelete(delete: SqlStatement.Delete): Unit =
        sqlBuilder.append("DELETE FROM ")
        printTable(delete.table)

        for i <- delete.where do
            sqlBuilder.append(" WHERE ")
            printExpr(i)

    def printUpsert(upsert: SqlStatement.Upsert): Unit

    def printTruncate(truncate: SqlStatement.Truncate): Unit =
        sqlBuilder.append("TRUNCATE ")
        printTable(truncate.table)

    def printQuery(query: SqlQuery): Unit = query match
        case select: SqlQuery.Select => printSelect(select)
        case union: SqlQuery.Union => printUnion(union)
        case values: SqlQuery.Values => printValues(values)
        case cte: SqlQuery.Cte => printCte(cte)

    def printSelect(select: SqlQuery.Select): Unit =
        sqlBuilder.append("SELECT ")

        select.param.foreach(p => sqlBuilder.append(p.param + " "))

        if select.select.isEmpty then sqlBuilder.append("*") else printList(select.select)(printSelectItem)

        if select.from.nonEmpty then
            sqlBuilder.append(" FROM ")
            printList(select.from)(printTable)

        for w <- select.where do
            sqlBuilder.append(" WHERE ")
            printExpr(w)

        if select.groupBy.nonEmpty then
            sqlBuilder.append(" GROUP BY ")
            printList(select.groupBy)(printExpr)

        for h <- select.having do
            sqlBuilder.append(" HAVING ")
            printExpr(h)

        if select.orderBy.nonEmpty then
            sqlBuilder.append(" ORDER BY ")
            printList(select.orderBy)(printOrderBy)

        for l <- select.limit do printLimit(l)

        if select.forUpdate then printForUpdate()

    def printUnion(union: SqlQuery.Union): Unit =
        sqlBuilder.append("(")
        printQuery(union.left)
        sqlBuilder.append(")")
        sqlBuilder.append(" ")
        sqlBuilder.append(union.unionType.unionType)
        sqlBuilder.append(" ")
        sqlBuilder.append("(")
        printQuery(union.right)
        sqlBuilder.append(")")

    def printValues(values: SqlQuery.Values): Unit =
        sqlBuilder.append("VALUES ")
        printList(values.values.map(SqlExpr.Vector(_)))(printExpr)

    def printCte(cte: SqlQuery.Cte): Unit =
        sqlBuilder.append("WITH")
        if cte.recursive then printCteRecursive()

        def printWithItem(item: SqlWithItem): Unit =
            sqlBuilder.append(" ")
            sqlBuilder.append(s"$leftQuote${item.name}$rightQuote")
            if item.columns.nonEmpty then
                sqlBuilder.append("(")
                printList(item.columns)(c => sqlBuilder.append(s"$leftQuote${c}$rightQuote"))
                sqlBuilder.append(")")
            sqlBuilder.append(" AS ")
            sqlBuilder.append("(")
            printQuery(item.query)
            sqlBuilder.append(")")

        printList(cte.queryItems)(printWithItem)
        sqlBuilder.append(" ")
        printQuery(cte.query)

    def printExpr(expr: SqlExpr): Unit = expr match
        case a: SqlExpr.AllColumn => printAllColumnExpr(a)
        case c: SqlExpr.Column => printColumnExpr(c)
        case SqlExpr.Null => printNullExpr()
        case SqlExpr.UnknownValue => printUnknownValueExpr()
        case s: SqlExpr.StringLiteral => printStringLiteralExpr(s)
        case n: SqlExpr.NumberLiteral => printNumberLiteralExpr(n)
        case b: SqlExpr.BooleanLiteral => printBooleanLiteralExpr(b)
        case v: SqlExpr.Vector => printVectorExpr(v)
        case u: SqlExpr.Unary => printUnaryExpr(u)
        case b: SqlExpr.Binary => printBinaryExpr(b)
        case f: SqlExpr.Func => printFuncExpr(f)
        case a: SqlExpr.Agg => printAggExpr(a)
        case i: SqlExpr.In => printInExpr(i)
        case b: SqlExpr.Between => printBetweenExpr(b)
        case c: SqlExpr.Case => printCaseExpr(c)
        case c: SqlExpr.Cast => printCastExpr(c)
        case w: SqlExpr.Window => printWindowExpr(w)
        case q: SqlExpr.SubQuery => printSubQueryExpr(q)
        case q: SqlExpr.SubQueryPredicate => printSubQueryPredicateExpr(q)
        case i: SqlExpr.Interval => printIntervalExpr(i)

    def printAllColumnExpr(expr: SqlExpr.AllColumn): Unit =
        expr.tableName.foreach(n => sqlBuilder.append(s"$leftQuote$n$rightQuote."))
        sqlBuilder.append("*")

    def printColumnExpr(expr: SqlExpr.Column): Unit =
        expr.tableName.foreach(n => sqlBuilder.append(s"$leftQuote$n$rightQuote."))
        sqlBuilder.append(s"$leftQuote${expr.columnName}$rightQuote")

    def printNullExpr(): Unit = sqlBuilder.append("NULL")

    def printUnknownValueExpr(): Unit = sqlBuilder.append("?")

    def printStringLiteralExpr(expr: SqlExpr.StringLiteral): Unit =
        if prepare then
            sqlBuilder.append("?")
            args.append(expr.string)
        else
            sqlBuilder.append(s"'${expr.string}'")

    def printNumberLiteralExpr(expr: SqlExpr.NumberLiteral): Unit =
        if prepare then
            sqlBuilder.append("?")
            args.append(expr.number)
        else
            sqlBuilder.append(expr.number)

    def printBooleanLiteralExpr(expr: SqlExpr.BooleanLiteral): Unit =
        if prepare then
            sqlBuilder.append("?")
            args.append(expr.boolean)
        else
            if expr.boolean then 
                sqlBuilder.append("TRUE")
            else
                sqlBuilder.append("FALSE")

    def printVectorExpr(expr: SqlExpr.Vector): Unit =
        sqlBuilder.append("(")
        printList(expr.items)(printExpr)
        sqlBuilder.append(")")

    def printUnaryExpr(expr: SqlExpr.Unary): Unit =
        sqlBuilder.append(expr.op)
        sqlBuilder.append("(")
        printExpr(expr.expr)
        sqlBuilder.append(")")

    def printBinaryExpr(expr: SqlExpr.Binary): Unit =
        def hasBracketsLeft(parent: SqlExpr.Binary, child: SqlExpr): Boolean = (parent.op, child) match
            case (And, SqlExpr.Binary(_, Or, _)) => true
            case (Times, SqlExpr.Binary(_, Plus | Minus, _)) => true
            case (Div | Mod | Minus, _) => true
            case (Custom(_), _) => true
            case _ => false

        def hasBracketsRight(parent: SqlExpr.Binary, child: SqlExpr): Boolean = (parent.op, child) match
            case (And, SqlExpr.Binary(_, Or, _)) => true
            case (Times | Div | Mod, SqlExpr.Binary(_, Plus | Minus, _)) => true
            case (Div | Mod | Minus, _) => true
            case (Custom(_), _) => true
            case _ => false

        if hasBracketsLeft(expr, expr.left) then
            sqlBuilder.append("(")
            printExpr(expr.left)
            sqlBuilder.append(")")
        else
            printExpr(expr.left)

        sqlBuilder.append(s" ${expr.op.operator} ")

        if hasBracketsRight(expr, expr.right) then
            sqlBuilder.append("(")
            printExpr(expr.right)
            sqlBuilder.append(")")
        else
            printExpr(expr.right)

    def printFuncExpr(expr: SqlExpr.Func): Unit =
        sqlBuilder.append(expr.name)
        sqlBuilder.append("(")
        printList(expr.args)(printExpr)
        sqlBuilder.append(")")

    def printAggExpr(expr: SqlExpr.Agg): Unit =
        sqlBuilder.append(expr.name)
        sqlBuilder.append("(")
        if expr.distinct then sqlBuilder.append("DISTINCT ")
        if expr.name.toUpperCase == "COUNT" && expr.args.isEmpty then sqlBuilder.append("*")
        printList(expr.args)(printExpr)
        if expr.orderBy.nonEmpty then
            sqlBuilder.append(" ORDER BY ")
            printList(expr.orderBy)(printOrderBy)
        for (k, v) <- expr.attrs do
            sqlBuilder.append(s" $k ")
            printExpr(v)
        sqlBuilder.append(")")

    def printInExpr(expr: SqlExpr.In): Unit =
        expr match
            case SqlExpr.In(exp, inExpr, not) =>
                printExpr(exp)
                if not then sqlBuilder.append(" NOT")
                sqlBuilder.append(" IN ")
                printExpr(inExpr)

    def printBetweenExpr(expr: SqlExpr.Between): Unit =
        printExpr(expr.expr)
        if expr.not then sqlBuilder.append(" NOT")
        sqlBuilder.append(" BETWEEN ")
        printExpr(expr.start)
        sqlBuilder.append(" AND ")
        printExpr(expr.end)

    def printCaseExpr(expr: SqlExpr.Case): Unit =
        sqlBuilder.append("CASE")
        for branch <- expr.branches do
            sqlBuilder.append(" WHEN ")
            printExpr(branch.whenExpr)
            sqlBuilder.append(" THEN ")
            printExpr(branch.thenExpr)
        sqlBuilder.append(" ELSE ")
        printExpr(expr.default)
        sqlBuilder.append(" END")

    def printCastExpr(expr: SqlExpr.Cast): Unit =
        sqlBuilder.append("CAST(")
        printExpr(expr)
        sqlBuilder.append(s" AS ${expr.castType}")

    def printWindowExpr(expr: SqlExpr.Window): Unit =
        printExpr(expr.expr)
        sqlBuilder.append(" OVER (")
        if expr.partitionBy.nonEmpty then
            sqlBuilder.append("PARTITION BY ")
            printList(expr.partitionBy)(printExpr)
        if expr.orderBy.nonEmpty then
            if expr.partitionBy.nonEmpty then
                sqlBuilder.append(" ")
            sqlBuilder.append("ORDER BY ")
            printList(expr.orderBy)(printOrderBy)
        expr.frame match
            case Some(SqlWindowFrame.Rows(start, end)) =>
                sqlBuilder.append(" ROWS BETWEEN ")
                sqlBuilder.append(start.showString)
                sqlBuilder.append(" AND")
                sqlBuilder.append(end.showString)
            case Some(SqlWindowFrame.Range(start, end)) =>
                sqlBuilder.append(" RANGE BETWEEN ")
                sqlBuilder.append(start.showString)
                sqlBuilder.append(" AND")
                sqlBuilder.append(end.showString)
            case None =>
        sqlBuilder.append(")")

    def printSubQueryExpr(expr: SqlExpr.SubQuery): Unit =
        sqlBuilder.append("(")
        printQuery(expr.query)
        sqlBuilder.append(")")

    def printSubQueryPredicateExpr(expr: SqlExpr.SubQueryPredicate): Unit =
        sqlBuilder.append(expr.predicate.predicate)
        sqlBuilder.append("(")
        printQuery(expr.query)
        sqlBuilder.append(")")

    def printIntervalExpr(expr: SqlExpr.Interval): Unit

    def printTableAlias(alias: SqlTableAlias): Unit =
        sqlBuilder.append(s" AS $leftQuote${alias.tableAlias}$rightQuote")
        if alias.columnAlias.nonEmpty then
            sqlBuilder.append("(")
            printList(alias.columnAlias)(i => sqlBuilder.append(s"$leftQuote$i$rightQuote"))
            sqlBuilder.append(")")

    def printTable(table: SqlTable): Unit = table match
        case SqlTable.IdentTable(tableName, alias) =>
            sqlBuilder.append(s"$leftQuote$tableName$rightQuote")
            for a <- alias do 
                printTableAlias(a)
        case SqlTable.SubQueryTable(query, lateral, alias) =>
            if lateral then sqlBuilder.append("LATERAL ")
            sqlBuilder.append("(")
            printQuery(query)
            sqlBuilder.append(")")
            printTableAlias(alias)
        case SqlTable.JoinTable(left, joinType, right, condition) =>
            printTable(left)
            sqlBuilder.append(s" ${joinType.joinType} ")
            right match
                case _: SqlTable.JoinTable => 
                    sqlBuilder.append("(")
                    printTable(right)
                    sqlBuilder.append(")")
                case _ =>
                    printTable(right)
            for c <- condition do
                c match
                    case SqlJoinCondition.On(onCondition) =>
                        sqlBuilder.append(" ON ")
                        printExpr(onCondition)
                    case SqlJoinCondition.Using(usingCondition) =>
                        sqlBuilder.append(" USING ")
                        printExpr(usingCondition)

    def printSelectItem(item: SqlSelectItem): Unit =
        printExpr(item.expr)
        item.alias.foreach(a => sqlBuilder.append(s" AS $leftQuote$a$rightQuote"))

    def printOrderBy(orderBy: SqlOrderBy): Unit =
        printExpr(orderBy.expr)
        val order = orderBy.order match
            case None | Some(SqlOrderByOption.Asc) => SqlOrderByOption.Asc
            case _ => SqlOrderByOption.Desc
        sqlBuilder.append(s" ${order.order}")
        for o <- orderBy.nullsOrder do
            sqlBuilder.append(s" ${o.order}")

    def printLimit(limit: SqlLimit): Unit =
        sqlBuilder.append(" LIMIT ")
        printExpr(limit.limit)
        sqlBuilder.append(" OFFSET ")
        printExpr(limit.offset)

    def printForUpdate(): Unit = sqlBuilder.append(" FOR UPDATE")

    def printCteRecursive(): Unit = sqlBuilder.append(" RECURSIVE")

    def printList[T](list: List[T])(printer: T => Unit): Unit =
        for i <- list.indices do
            printer(list(i))
            if i < list.size - 1 then
                sqlBuilder.append(", ")