package sqala.printer

import sqala.ast.expr.*
import sqala.ast.group.SqlGroupingItem
import sqala.ast.limit.SqlLimit
import sqala.ast.order.{SqlOrdering, SqlOrderingItem}
import sqala.ast.statement.{SqlQuery, SqlSelectItem, SqlStatement, SqlWithItem}
import sqala.ast.table.{SqlJoinCondition, SqlTable, SqlTableAlias}
import sqala.util.|>

import scala.collection.mutable.ArrayBuffer

abstract class SqlPrinter(val enableJdbcPrepare: Boolean):
    val sqlBuilder: StringBuilder = StringBuilder()

    val leftQuote: String = "\""

    val rightQuote: String = "\""

    val args: ArrayBuffer[Any] = ArrayBuffer()

    val indent: Int = 4

    var spaceNum: Int = 0

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
            printList(insert.values.map(SqlExpr.Tuple(_)))(printExpr)

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
        case set: SqlQuery.Set => printSet(set)
        case values: SqlQuery.Values => printValues(values)
        case cte: SqlQuery.Cte => printCte(cte)

    def printSelect(select: SqlQuery.Select): Unit =
        printSpace()
        sqlBuilder.append("SELECT")

        select.quantifier.foreach(q => sqlBuilder.append(" " + q.quantifier))

        if select.select.nonEmpty then
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

        for g <- select.groupBy do
            sqlBuilder.append("\n")
            printSpace()
            sqlBuilder.append("GROUP BY")
            for q <- g.quantifier do
                sqlBuilder.append(" ")
                sqlBuilder.append(q.quantifier)
            sqlBuilder.append("\n")
            printList(g.items, ",\n")(printGroupingItem |> printWithSpace)

        for h <- select.having do
            sqlBuilder.append("\n")
            printSpace()
            sqlBuilder.append("HAVING\n")
            h |> printWithSpace(printExpr)

        if select.orderBy.nonEmpty then
            sqlBuilder.append("\n")
            printSpace()
            sqlBuilder.append("ORDER BY\n")
            printList(select.orderBy, ",\n")(printOrderingItem |> printWithSpace)

        for l <- select.limit do
            sqlBuilder.append("\n")
            printSpace()
            printLimit(l)

    def printSet(set: SqlQuery.Set): Unit =
        printSpace()
        sqlBuilder.append("(\n")
        push()
        printQuery(set.left)
        pull()
        sqlBuilder.append("\n")
        printSpace()
        sqlBuilder.append(")")
        sqlBuilder.append("\n")

        printSpace()
        sqlBuilder.append(set.operator.operator)
        for q <- set.operator.quantifier do
            sqlBuilder.append(" ")
            sqlBuilder.append(q.quantifier)
        sqlBuilder.append("\n")

        printSpace()
        sqlBuilder.append("(\n")
        push()
        printQuery(set.right)
        pull()
        sqlBuilder.append("\n")
        printSpace()
        sqlBuilder.append(")")

        if set.orderBy.nonEmpty then
            sqlBuilder.append("\n")
            printSpace()
            sqlBuilder.append("ORDER BY\n")
            printList(set.orderBy, ",\n")(printOrderingItem |> printWithSpace)

        for l <- set.limit do
            sqlBuilder.append("\n")
            printSpace()
            printLimit(l)

    def printValues(values: SqlQuery.Values): Unit =
        printSpace()
        sqlBuilder.append("VALUES ")
        printList(values.values.map(SqlExpr.Tuple(_)))(printExpr)

    def printCte(cte: SqlQuery.Cte): Unit =
        printSpace()
        sqlBuilder.append("WITH")
        if cte.recursive then printCteRecursive()
        sqlBuilder.append("\n")

        def printWithItem(item: SqlWithItem): Unit =
            printSpace()
            sqlBuilder.append(s"$leftQuote${item.name}$rightQuote")
            if item.columnNames.nonEmpty then
                sqlBuilder.append("(")
                printList(item.columnNames)(c => sqlBuilder.append(s"$leftQuote$c$rightQuote"))
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
        case SqlExpr.NullLiteral => printNullLiteralExpr()
        case s: SqlExpr.StringLiteral => printStringLiteralExpr(s)
        case n: SqlExpr.NumberLiteral[_] => printNumberLiteralExpr(n)
        case b: SqlExpr.BooleanLiteral => printBooleanLiteralExpr(b)
        case t: SqlExpr.TimeLiteral => printTimeLiteralExpr(t)
        case v: SqlExpr.Tuple => printTupleExpr(v)
        case a: SqlExpr.Array => printArrayExpr(a)
        case u: SqlExpr.Unary => printUnaryExpr(u)
        case b: SqlExpr.Binary => printBinaryExpr(b)
        case n: SqlExpr.NullTest => printNullTestExpr(n)
        case f: SqlExpr.Func => printFuncExpr(f)
        case b: SqlExpr.Between => printBetweenExpr(b)
        case c: SqlExpr.Case => printCaseExpr(c)
        case m: SqlExpr.Match => printMatchExpr(m)
        case c: SqlExpr.Cast => printCastExpr(c)
        case w: SqlExpr.Window => printWindowExpr(w)
        case q: SqlExpr.SubQuery => printSubQueryExpr(q)
        case q: SqlExpr.SubLink => printSubLinkExpr(q)
        case i: SqlExpr.Interval => printIntervalExpr(i)
        case e: SqlExpr.Extract => printExtractExpr(e)
        case c: SqlExpr.Custom => printCustomExpr(c)

    def printColumnExpr(expr: SqlExpr.Column): Unit =
        expr.tableName.foreach(n => sqlBuilder.append(s"$leftQuote$n$rightQuote."))
        sqlBuilder.append(s"$leftQuote${expr.columnName}$rightQuote")

    def printNullLiteralExpr(): Unit = sqlBuilder.append("NULL")

    def printStringLiteralExpr(expr: SqlExpr.StringLiteral): Unit =
        if enableJdbcPrepare then
            sqlBuilder.append("?")
            args.addOne(expr.string)
        else
            sqlBuilder.append(s"'${expr.string}'")

    def printNumberLiteralExpr(expr: SqlExpr.NumberLiteral[?]): Unit =
        if enableJdbcPrepare then
            sqlBuilder.append("?")
            args.addOne(expr.number)
        else
            sqlBuilder.append(expr.number)

    def printBooleanLiteralExpr(expr: SqlExpr.BooleanLiteral): Unit =
        if enableJdbcPrepare then
            sqlBuilder.append("?")
            args.addOne(expr.boolean)
        else
            if expr.boolean then
                sqlBuilder.append("TRUE")
            else
                sqlBuilder.append("FALSE")

    def printTimeLiteralExpr(expr: SqlExpr.TimeLiteral): Unit =
        if enableJdbcPrepare then
            sqlBuilder.append("?")
            args.addOne(expr.time)
        else
            sqlBuilder.append(expr.unit.unit)
            sqlBuilder.append(" ")
            sqlBuilder.append(s"'${expr.time}'")

    def printTupleExpr(expr: SqlExpr.Tuple): Unit =
        sqlBuilder.append("(")
        printList(expr.items)(printExpr)
        sqlBuilder.append(")")

    def printArrayExpr(expr: SqlExpr.Array): Unit =
        sqlBuilder.append("ARRAY[")
        printList(expr.items)(printExpr)
        sqlBuilder.append("]")

    def printUnaryExpr(expr: SqlExpr.Unary): Unit =
        expr.operator match
            case SqlUnaryOperator.Not =>
                sqlBuilder.append(expr.operator.operator)
                sqlBuilder.append(" ")
                printExpr(expr.expr)
            case _ =>
                val hasBrackets = expr.expr match
                    case SqlExpr.NullLiteral => false
                    case SqlExpr.Column(_, _) => false
                    case SqlExpr.NumberLiteral(_) => false
                    case _ => true
                sqlBuilder.append(expr.operator.operator)
                if hasBrackets then
                    sqlBuilder.append("(")
                    printExpr(expr.expr)
                    sqlBuilder.append(")")
                else
                    printExpr(expr.expr)

    def printBinaryExpr(expr: SqlExpr.Binary): Unit =
        def hasBracketsLeft(parent: SqlExpr.Binary, child: SqlExpr): Boolean =
            child match
                case SqlExpr.Binary(_, op, _)
                    if op.precedence < parent.operator.precedence || op.precedence == 0 => true
                case _ => false

        def hasBracketsRight(parent: SqlExpr.Binary, child: SqlExpr): Boolean =
            child match
                case SqlExpr.Binary(_, op, _)
                    if op.precedence <= parent.operator.precedence => true
                case _ => false

        if hasBracketsLeft(expr, expr.left) then
            sqlBuilder.append("(")
            printExpr(expr.left)
            sqlBuilder.append(")")
        else
            printExpr(expr.left)

        sqlBuilder.append(s" ${expr.operator.operator} ")

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
        expr.quantifier.foreach: q => 
            sqlBuilder.append(q.quantifier)
            sqlBuilder.append(" ")
        if expr.name.equalsIgnoreCase("COUNT") && expr.args.isEmpty then sqlBuilder.append("*")
        else printList(expr.args)(printExpr)
        if expr.orderBy.nonEmpty then
            sqlBuilder.append(" ORDER BY ")
            printList(expr.orderBy)(printOrderingItem)
        sqlBuilder.append(")")
        if expr.withinGroup.nonEmpty then
            sqlBuilder.append(" WITHIN GROUP (ORDER BY ")
            printList(expr.withinGroup)(printOrderingItem)
            sqlBuilder.append(")")
        if expr.filter.nonEmpty then
            sqlBuilder.append(" FILTER (WHERE ")
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
            printExpr(branch.when)
            sqlBuilder.append(" THEN ")
            printExpr(branch.`then`)
        for d <- expr.default do
            sqlBuilder.append(" ELSE ")
            printExpr(d)
        sqlBuilder.append(" END")

    def printMatchExpr(expr: SqlExpr.Match): Unit =
        sqlBuilder.append("CASE ")
        printExpr(expr.expr)
        for branch <- expr.branches do
            sqlBuilder.append(" WHEN ")
            printExpr(branch.when)
            sqlBuilder.append(" THEN ")
            printExpr(branch.`then`)
        for d <- expr.default do
            sqlBuilder.append(" ELSE ")
            printExpr(d)
        sqlBuilder.append(" END")

    def printCastExpr(expr: SqlExpr.Cast): Unit =
        sqlBuilder.append("CAST(")
        printExpr(expr.expr)
        sqlBuilder.append(" AS ")
        printCastType(expr.castType)
        sqlBuilder.append(")")

    def printCastType(castType: SqlCastType): Unit

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
            printList(expr.orderBy)(printOrderingItem)
        for f <- expr.frame do
            f match
                case SqlWindowFrame.Start(unit, start) =>
                    sqlBuilder.append(" ")
                    sqlBuilder.append(unit.unit)
                    sqlBuilder.append(" ")
                    sqlBuilder.append(start.bound)
                case SqlWindowFrame.Between(unit, start, end) =>
                    sqlBuilder.append(" ")
                    sqlBuilder.append(unit.unit)
                    sqlBuilder.append(" BETWEEN ")
                    sqlBuilder.append(start.bound)
                    sqlBuilder.append(" AND ")
                    sqlBuilder.append(end.bound)
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
        sqlBuilder.append(expr.quantifier.quantifier)
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

    def printCustomExpr(expr: SqlExpr.Custom): Unit =
        sqlBuilder.append(expr.snippet)

    def printTableAlias(alias: SqlTableAlias): Unit =
        sqlBuilder.append(s" AS $leftQuote${alias.tableAlias}$rightQuote")
        if alias.columnAlias.nonEmpty then
            sqlBuilder.append("(")
            printList(alias.columnAlias)(i => sqlBuilder.append(s"$leftQuote$i$rightQuote"))
            sqlBuilder.append(")")

    def printTable(table: SqlTable): Unit = 
        table match
            case SqlTable.Range(tableName, _) =>
                sqlBuilder.append(s"$leftQuote$tableName$rightQuote")
            case SqlTable.Func(functionName, args, _) =>
                sqlBuilder.append(functionName)
                sqlBuilder.append("(")
                printList(args)(printExpr)
                sqlBuilder.append(")")
            case SqlTable.SubQuery(query, lateral, _) =>
                if lateral then sqlBuilder.append("LATERAL ")
                sqlBuilder.append("(\n")
                push()
                printQuery(query)
                pull()
                sqlBuilder.append("\n")
                printSpace()
                sqlBuilder.append(")")
            case SqlTable.Join(left, joinType, right, condition, _) =>
                printTable(left)
                sqlBuilder.append("\n")
                printSpace()
                sqlBuilder.append(s"${joinType.joinType} ")
                right match
                    case _: SqlTable.Join =>
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
        for a <- table.alias do
            printTableAlias(a)

    def printSelectItem(item: SqlSelectItem): Unit = item match
        case SqlSelectItem.Wildcard(table) =>
            table.foreach(n => sqlBuilder.append(s"$leftQuote$n$rightQuote."))
            sqlBuilder.append("*")
        case SqlSelectItem.Expr(expr, alias) =>
            printExpr(expr)
            alias.foreach(a => sqlBuilder.append(s" AS $leftQuote$a$rightQuote"))

    def printGroupingItem(item: SqlGroupingItem): Unit = item match
        case SqlGroupingItem.Expr(item) => printExpr(item)
        case SqlGroupingItem.Cube(items) =>
            sqlBuilder.append("CUBE(")
            printList(items)(printExpr)
            sqlBuilder.append(")")
        case SqlGroupingItem.Rollup(items) =>
            sqlBuilder.append("ROLLUP(")
            printList(items)(printExpr)
            sqlBuilder.append(")")
        case SqlGroupingItem.GroupingSets(items) =>
            sqlBuilder.append("GROUPING SETS(")
            printList(items)(printExpr)
            sqlBuilder.append(")")

    def printOrderingItem(item: SqlOrderingItem): Unit =
        printExpr(item.expr)
        val order = item.ordering match
            case None | Some(SqlOrdering.Asc) => SqlOrdering.Asc
            case _ => SqlOrdering.Desc
        sqlBuilder.append(s" ${order.order}")
        for o <- item.nullsOrdering do
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