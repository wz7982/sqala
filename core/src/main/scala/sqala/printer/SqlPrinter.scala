package sqala.printer

import sqala.ast.expr.SqlBinaryOperator.*
import sqala.ast.expr.{SqlExpr, SqlUnaryOperator, SqlWindowFrame}
import sqala.ast.group.SqlGroupItem
import sqala.ast.limit.SqlLimit
import sqala.ast.order.{SqlOrderBy, SqlOrderByOption}
import sqala.ast.statement.{SqlQuery, SqlSelectItem, SqlStatement, SqlWithItem}
import sqala.ast.table.{SqlJoinCondition, SqlTable, SqlTableAlias}

import scala.collection.mutable.ArrayBuffer

abstract class SqlPrinter(val prepare: Boolean, val indent: Int = 4):
    val sqlBuilder: StringBuilder = StringBuilder()

    val leftQuote: String = "\""

    val rightQuote: String = "\""

    val args: ArrayBuffer[Any] = ArrayBuffer()

    var spaceNum = 0

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
        printSpace()
        sqlBuilder.append("SELECT ")

        select.param.foreach(p => sqlBuilder.append(p.param + " "))

        if select.select.isEmpty then sqlBuilder.append("*")
        else
            sqlBuilder.append("\n")
            printList(select.select, ",\n")(printSelectItem |> printWithSpace)

        if select.from.nonEmpty then
            sqlBuilder.append("\n")
            printSpace()
            sqlBuilder.append("FROM\n")
            printList(select.from, ",\n")(printTable |> printWithSpace)

        for w <- select.where do
            sqlBuilder.append("\n")
            printSpace()
            sqlBuilder.append("WHERE\n")
            w |> printWithSpace(printExpr)

        if select.groupBy.nonEmpty then
            sqlBuilder.append("\n")
            printSpace()
            sqlBuilder.append("GROUP BY\n")
            printList(select.groupBy, ",\n")(printGroupItem |> printWithSpace)

        for h <- select.having do
            sqlBuilder.append("\n")
            printSpace()
            sqlBuilder.append("HAVING\n")
            h |> printWithSpace(printExpr)

        if select.orderBy.nonEmpty then
            sqlBuilder.append("\n")
            printSpace()
            sqlBuilder.append("ORDER BY\n")
            printList(select.orderBy, ",\n")(printOrderBy |> printWithSpace)

        for l <- select.limit do
            sqlBuilder.append("\n")
            printSpace()
            printLimit(l)

    def printUnion(union: SqlQuery.Union): Unit =
        printSpace()
        sqlBuilder.append("(\n")
        push()
        printQuery(union.left)
        pull()
        sqlBuilder.append("\n")
        printSpace()
        sqlBuilder.append(")")
        sqlBuilder.append("\n")

        printSpace()
        sqlBuilder.append(union.unionType.unionType)
        sqlBuilder.append("\n")

        printSpace()
        sqlBuilder.append("(\n")
        push()
        printQuery(union.right)
        pull()
        sqlBuilder.append("\n")
        printSpace()
        sqlBuilder.append(")")

        if union.orderBy.nonEmpty then
            sqlBuilder.append("\n")
            printSpace()
            sqlBuilder.append("ORDER BY\n")
            printList(union.orderBy, ",\n")(printOrderBy |> printWithSpace)

        for l <- union.limit do
            sqlBuilder.append("\n")
            printSpace()
            printLimit(l)

    def printValues(values: SqlQuery.Values): Unit =
        printSpace()
        sqlBuilder.append("VALUES ")
        printList(values.values.map(SqlExpr.Vector(_)))(printExpr)

    def printCte(cte: SqlQuery.Cte): Unit =
        printSpace()
        sqlBuilder.append("WITH")
        if cte.recursive then printCteRecursive()
        sqlBuilder.append("\n")

        def printWithItem(item: SqlWithItem): Unit =
            printSpace()
            sqlBuilder.append(s"$leftQuote${item.name}$rightQuote")
            if item.columns.nonEmpty then
                sqlBuilder.append("(")
                printList(item.columns)(c => sqlBuilder.append(s"$leftQuote$c$rightQuote"))
                sqlBuilder.append(")")
            sqlBuilder.append(" AS ")
            push()
            sqlBuilder.append("(")
            sqlBuilder.append("\n")
            printQuery(item.query)
            sqlBuilder.append("\n")
            pull()
            printSpace()
            sqlBuilder.append(")")

        printList(cte.queryItems, ",\n")(printWithItem)
        sqlBuilder.append("\n")
        printQuery(cte.query)

    def printExpr(expr: SqlExpr): Unit = expr match
        case c: SqlExpr.Column => printColumnExpr(c)
        case SqlExpr.Null => printNullExpr()
        case s: SqlExpr.StringLiteral => printStringLiteralExpr(s)
        case n: SqlExpr.NumberLiteral => printNumberLiteralExpr(n)
        case b: SqlExpr.BooleanLiteral => printBooleanLiteralExpr(b)
        case v: SqlExpr.Vector => printVectorExpr(v)
        case u: SqlExpr.Unary => printUnaryExpr(u)
        case b: SqlExpr.Binary => printBinaryExpr(b)
        case n: SqlExpr.NullTest => printNullTestExpr(n)
        case f: SqlExpr.Func => printFuncExpr(f)
        case b: SqlExpr.Between => printBetweenExpr(b)
        case c: SqlExpr.Case => printCaseExpr(c)
        case c: SqlExpr.Cast => printCastExpr(c)
        case w: SqlExpr.Window => printWindowExpr(w)
        case q: SqlExpr.SubQuery => printSubQueryExpr(q)
        case q: SqlExpr.SubLink => printSubLinkExpr(q)
        case i: SqlExpr.Interval => printIntervalExpr(i)
        case e: SqlExpr.Extract => printExtractExpr(e)
        case g: SqlExpr.Grouping => printGroupingExpr(g)

    def printColumnExpr(expr: SqlExpr.Column): Unit =
        expr.tableName.foreach(n => sqlBuilder.append(s"$leftQuote$n$rightQuote."))
        sqlBuilder.append(s"$leftQuote${expr.columnName}$rightQuote")

    def printNullExpr(): Unit = sqlBuilder.append("NULL")

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
        expr.op match
            case SqlUnaryOperator.Not =>
                sqlBuilder.append(expr.op.operator)
                sqlBuilder.append(" ")
                printExpr(expr.expr)
            case _ =>
                val hasBrackets = expr.expr match
                    case SqlExpr.Null => false
                    case SqlExpr.Column(_, _) => false
                    case SqlExpr.NumberLiteral(_) => false
                    case _ => true
                sqlBuilder.append(expr.op.operator)
                if hasBrackets then
                    sqlBuilder.append("(")
                    printExpr(expr.expr)
                    sqlBuilder.append(")")
                else
                    printExpr(expr.expr)

    def printBinaryExpr(expr: SqlExpr.Binary): Unit =
        def hasBracketsLeft(parent: SqlExpr.Binary, child: SqlExpr): Boolean =
            child match
                case SqlExpr.Binary(_, op, _) if op.priority < parent.op.priority => true
                case _ => false

        def hasBracketsRight(parent: SqlExpr.Binary, child: SqlExpr): Boolean =
            (parent.op, child) match
                case (parentOp, SqlExpr.Binary(_, op, _))
                    if op.priority <= parentOp.priority => true
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
            
    def printNullTestExpr(expr: SqlExpr.NullTest): Unit =
        printExpr(expr.expr)
        if expr.not then sqlBuilder.append(" IS NOT NULL")
        else sqlBuilder.append(" IS NULL")

    def printFuncExpr(expr: SqlExpr.Func): Unit =
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
        if expr.withinGroupOrderBy.nonEmpty then
            sqlBuilder.append(" WITHIN GROUP(ORDER BY ")
            printList(expr.withinGroupOrderBy)(printOrderBy)
            sqlBuilder.append(")")
        if expr.filter.nonEmpty then
            sqlBuilder.append(" FILTER(WHERE ")
            printExpr(expr.filter.get)
            sqlBuilder.append(")")

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
        printExpr(expr.expr)
        sqlBuilder.append(s" AS ${expr.castType})")

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
                sqlBuilder.append(" AND ")
                sqlBuilder.append(end.showString)
            case Some(SqlWindowFrame.Range(start, end)) =>
                sqlBuilder.append(" RANGE BETWEEN ")
                sqlBuilder.append(start.showString)
                sqlBuilder.append(" AND ")
                sqlBuilder.append(end.showString)
            case Some(SqlWindowFrame.Groups(start, end)) =>
                sqlBuilder.append(" GROUPS BETWEEN ")
                sqlBuilder.append(start.showString)
                sqlBuilder.append(" AND ")
                sqlBuilder.append(end.showString)
            case None =>
        sqlBuilder.append(")")

    def printSubQueryExpr(expr: SqlExpr.SubQuery): Unit =
        push()
        sqlBuilder.append("(\n")
        printQuery(expr.query)
        pull()
        sqlBuilder.append("\n")
        printSpace()
        sqlBuilder.append(")")

    def printSubLinkExpr(expr: SqlExpr.SubLink): Unit =
        push()
        sqlBuilder.append(expr.linkType.linkType)
        sqlBuilder.append("(\n")
        printQuery(expr.query)
        pull()
        sqlBuilder.append("\n")
        printSpace()
        sqlBuilder.append(")")

    def printIntervalExpr(expr: SqlExpr.Interval): Unit

    def printExtractExpr(expr: SqlExpr.Extract): Unit =
        sqlBuilder.append("EXTRACT(")
        sqlBuilder.append(expr.unit.unit)
        sqlBuilder.append(" FROM ")
        printExpr(expr.expr)
        sqlBuilder.append(")")

    def printGroupingExpr(expr: SqlExpr.Grouping): Unit =
        sqlBuilder.append("GROUPING(")
        printList(expr.items)(printExpr)
        sqlBuilder.append(")")

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
            sqlBuilder.append("(\n")
            push()
            printQuery(query)
            pull()
            sqlBuilder.append("\n")
            printSpace()
            sqlBuilder.append(")")
            printTableAlias(alias)
        case SqlTable.JoinTable(left, joinType, right, condition) =>
            printTable(left)
            sqlBuilder.append("\n")
            printSpace()
            sqlBuilder.append(s"${joinType.joinType} ")
            right match
                case _: SqlTable.JoinTable =>
                    sqlBuilder.append("(")
                    sqlBuilder.append("\n")
                    push()
                    printSpace()
                    printTable(right)
                    printSpace()
                    pull()
                    sqlBuilder.append("\n")
                    printSpace()
                    sqlBuilder.append(")")
                case _ =>
                    printTable(right)
            for c <- condition do
                c match
                    case SqlJoinCondition.On(onCondition) =>
                        sqlBuilder.append(" ON ")
                        printExpr(onCondition)
                    case SqlJoinCondition.Using(usingCondition) =>
                        sqlBuilder.append(" USING (")
                        printList(usingCondition)(printExpr)
                        sqlBuilder.append(")")

    def printSelectItem(item: SqlSelectItem): Unit = item match
        case SqlSelectItem.Wildcard(table) =>
            table.foreach(n => sqlBuilder.append(s"$leftQuote$n$rightQuote."))
            sqlBuilder.append("*")
        case SqlSelectItem.Item(expr, alias) =>
            printExpr(expr)
            alias.foreach(a => sqlBuilder.append(s" AS $leftQuote$a$rightQuote"))

    def printGroupItem(item: SqlGroupItem): Unit = item match
        case SqlGroupItem.Singleton(item) => printExpr(item)
        case SqlGroupItem.Cube(items) =>
            sqlBuilder.append("CUBE(")
            printList(items)(printExpr)
            sqlBuilder.append(")")
        case SqlGroupItem.Rollup(items) =>
            sqlBuilder.append("ROLLUP(")
            printList(items)(printExpr)
            sqlBuilder.append(")")
        case SqlGroupItem.GroupingSets(items) =>
            sqlBuilder.append("GROUPING SETS(")
            printList(items)(printExpr)
            sqlBuilder.append(")")

    def printOrderBy(orderBy: SqlOrderBy): Unit =
        printExpr(orderBy.expr)
        val order = orderBy.order match
            case None | Some(SqlOrderByOption.Asc) => SqlOrderByOption.Asc
            case _ => SqlOrderByOption.Desc
        sqlBuilder.append(s" ${order.order}")
        for o <- orderBy.nullsOrder do
            sqlBuilder.append(s" ${o.order}")

    def printLimit(limit: SqlLimit): Unit =
        sqlBuilder.append("LIMIT ")
        printExpr(limit.limit)
        sqlBuilder.append(" OFFSET ")
        printExpr(limit.offset)

    def printCteRecursive(): Unit = sqlBuilder.append(" RECURSIVE")

    def printList[T](list: List[T], separator: String = ", ")(printer: T => Unit): Unit =
        for i <- list.indices do
            printer(list(i))
            if i < list.size - 1 then
                sqlBuilder.append(separator)

    def printSpace(): Unit =
        val spaceString = (0 until spaceNum).map(_ => " ").mkString
        sqlBuilder.append(spaceString)

    def push(): Unit =
        spaceNum += indent

    def pull(): Unit =
        spaceNum -= indent

    def printWithSpace[T](f: T => Unit)(x: T): Unit =
        push()
        printSpace()
        f(x)
        pull()

    extension [A, B](a: A)
        private[sqala] def |>(f: A => B): B = f(a)