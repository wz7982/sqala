package sqala.printer

import sqala.ast.expr.*
import sqala.ast.group.SqlGroupingItem
import sqala.ast.limit.{SqlFetchUnit, SqlLimit}
import sqala.ast.order.{SqlOrdering, SqlOrderingItem}
import sqala.ast.statement.*
import sqala.ast.table.*
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

    def printQuery(query: SqlQuery): Unit = 
        query match
            case select: SqlQuery.Select => printSelect(select)
            case set: SqlQuery.Set => printSet(set)
            case values: SqlQuery.Values => printValues(values)
            case cte: SqlQuery.Cte => printCte(cte)

        for l <- query.lock do
            sqlBuilder.append("\n")
            printSpace()
            printLock(l)

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

        if select.window.nonEmpty then
            sqlBuilder.append("\n")
            printSpace()
            sqlBuilder.append("WINDOW\n")
            printList(select.window, ",\n")(printWindowItem |> printWithSpace)

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
        case i: SqlExpr.IntervalLiteral => printIntervalLiteralExpr(i)
        case v: SqlExpr.Tuple => printTupleExpr(v)
        case a: SqlExpr.Array => printArrayExpr(a)
        case v: SqlExpr.Vector => printVectorExpr(v)
        case u: SqlExpr.Unary => printUnaryExpr(u)
        case b: SqlExpr.Binary => printBinaryExpr(b)
        case n: SqlExpr.NullTest => printNullTestExpr(n)
        case j: SqlExpr.JsonTest => printJsonTestExpr(j)
        case b: SqlExpr.Between => printBetweenExpr(b)
        case c: SqlExpr.Case => printCaseExpr(c)
        case s: SqlExpr.SimpleCase => printSimpleCaseExpr(s)
        case c: SqlExpr.Coalesce => printCoalesceExpr(c)
        case n: SqlExpr.NullIf => printNullIfExpr(n)
        case c: SqlExpr.Cast => printCastExpr(c)
        case w: SqlExpr.Window => printWindowExpr(w)
        case q: SqlExpr.SubQuery => printSubQueryExpr(q)
        case q: SqlExpr.SubLink => printSubLinkExpr(q)
        case g: SqlExpr.Grouping => printGroupingExpr(g)
        case i: SqlExpr.IdentFunc => printIdentFuncExpr(i)
        case s: SqlExpr.SubstringFunc => printSubstringFuncExpr(s)
        case t: SqlExpr.TrimFunc => printTrimFuncExpr(t)
        case o: SqlExpr.OverlayFunc => printOverlayFuncExpr(o)
        case p: SqlExpr.PositionFunc => printPositionFuncExpr(p)
        case e: SqlExpr.ExtractFunc => printExtractFuncExpr(e)
        case v: SqlExpr.VectorDistanceFunc => printVectorDistanceFuncExpr(v)
        case j: SqlExpr.JsonSerializeFunc => printJsonSerializeFuncExpr(j)
        case j: SqlExpr.JsonParseFunc => printJsonParseFuncExpr(j)
        case j: SqlExpr.JsonQueryFunc => printJsonQueryFuncExpr(j)
        case j: SqlExpr.JsonValueFunc => printJsonValueFuncExpr(j)
        case j: SqlExpr.JsonObjectFunc => printJsonObjectFuncExpr(j)
        case j: SqlExpr.JsonArrayFunc => printJsonArrayFuncExpr(j)
        case j: SqlExpr.JsonExistsFunc => printJsonExistsFuncExpr(j)
        case c: SqlExpr.CountAsteriskFunc => printCountAsteriskFuncExpr(c)
        case l: SqlExpr.ListAggFunc => printListAggFuncExpr(l)
        case j: SqlExpr.JsonObjectAggFunc => printJsonObjectAggFuncExpr(j)
        case j: SqlExpr.JsonArrayAggFunc => printJsonArrayAggFuncExpr(j)
        case n: SqlExpr.NullsTreatmentFunc => printNullsTreatmentFuncExpr(n)
        case n: SqlExpr.NthValueFunc => printNthValueFuncExpr(n)
        case s: SqlExpr.StandardFunc => printStandardFuncExpr(s)
        case m: SqlExpr.MatchPhase => printMatchPhaseExpr(m)
        case c: SqlExpr.Custom => printCustomExpr(c)

    def printColumnExpr(expr: SqlExpr.Column): Unit =
        for n <- expr.tableName do
            sqlBuilder.append(s"$leftQuote$n$rightQuote.")
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

    def printIntervalLiteralExpr(expr: SqlExpr.IntervalLiteral): Unit =
        sqlBuilder.append("INTERVAL '")
        sqlBuilder.append(expr.value)
        sqlBuilder.append("' ")
        expr.field match
            case SqlIntervalField.To(s, e) =>
                sqlBuilder.append(s.unit)
                sqlBuilder.append(" TO ")
                sqlBuilder.append(e.unit)
            case SqlIntervalField.Single(u) =>
                sqlBuilder.append(u.unit)

    def printTupleExpr(expr: SqlExpr.Tuple): Unit =
        sqlBuilder.append("(")
        printList(expr.items)(printExpr)
        sqlBuilder.append(")")

    def printArrayExpr(expr: SqlExpr.Array): Unit =
        sqlBuilder.append("ARRAY[")
        printList(expr.items)(printExpr)
        sqlBuilder.append("]")

    def printVectorExpr(expr: SqlExpr.Vector): Unit =
        printExpr(expr.expr)

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

    def printJsonTestExpr(expr: SqlExpr.JsonTest): Unit =
        printExpr(expr.expr)
        if expr.not then sqlBuilder.append(" IS NOT JSON")
        else sqlBuilder.append(" IS JSON")
        for t <- expr.nodeType do
            sqlBuilder.append(" ")
            sqlBuilder.append(t.`type`)
        for u <- expr.uniqueness do
            sqlBuilder.append(" ")
            printJsonUniqueness(u)

    def printBetweenExpr(expr: SqlExpr.Between): Unit =
        def hasBrackets(expr: SqlExpr): Boolean = 
            expr match
                case SqlExpr.Binary(l, SqlBinaryOperator.And, r) =>
                    true
                case SqlExpr.Binary(l, o, r) =>
                    hasBrackets(l) || hasBrackets(r)
                case _ => 
                    false

        printExpr(expr.expr)
        if expr.not then sqlBuilder.append(" NOT")
        sqlBuilder.append(" BETWEEN ")
        if hasBrackets(expr.start) then
            sqlBuilder.append("(")
            printExpr(expr.start)
            sqlBuilder.append(")")
        else
            printExpr(expr.start)
        sqlBuilder.append(" AND ")
        if hasBrackets(expr.end) then
            sqlBuilder.append("(")
            printExpr(expr.end)
            sqlBuilder.append(")")
        else
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

    def printSimpleCaseExpr(expr: SqlExpr.SimpleCase): Unit =
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

    def printCoalesceExpr(expr: SqlExpr.Coalesce): Unit =
        sqlBuilder.append("COALESCE(")
        printList(expr.items)(printExpr)
        sqlBuilder.append(")")

    def printNullIfExpr(expr: SqlExpr.NullIf): Unit =
        sqlBuilder.append("NULLIF(")
        printExpr(expr.expr)
        sqlBuilder.append(", ")
        printExpr(expr.`then`)
        sqlBuilder.append(")")

    def printCastExpr(expr: SqlExpr.Cast): Unit =
        sqlBuilder.append("CAST(")
        printExpr(expr.expr)
        sqlBuilder.append(" AS ")
        printType(expr.castType)
        sqlBuilder.append(")")

    def printType(`type`: SqlType): Unit =
        `type` match
            case SqlType.Varchar(maxLength) =>
                sqlBuilder.append(s"VARCHAR${maxLength.map(l => s"($l)").getOrElse("")}")
            case SqlType.Int =>
                sqlBuilder.append("INTEGER")
            case SqlType.Long =>
                sqlBuilder.append("BIGINT")
            case SqlType.Float =>
                sqlBuilder.append("REAL")
            case SqlType.Double =>
                sqlBuilder.append("DOUBLE PRECISION")
            case SqlType.Decimal(precision) =>
                sqlBuilder.append(s"DECIMAL${precision.map((p, s) => s"($p, $s)").getOrElse("")}")
            case SqlType.Date =>
                sqlBuilder.append("DATE")
            case SqlType.Timestamp(mode) =>
                sqlBuilder.append(s"TIMESTAMP${mode.map(m => s" ${m.mode}").getOrElse("")}")
            case SqlType.Time(mode) =>
                sqlBuilder.append(s"TIME${mode.map(m => s" ${m.mode}").getOrElse("")}")
            case SqlType.Json =>
                sqlBuilder.append("JSON")
            case SqlType.Boolean =>
                sqlBuilder.append("BOOLEAN")
            case SqlType.Vector =>
                sqlBuilder.append("VECTOR")
            case SqlType.Array(t) =>
                printType(t)
                sqlBuilder.append("[]")
            case SqlType.Custom(t) =>
                sqlBuilder.append(t)

    def printWindowExpr(expr: SqlExpr.Window): Unit =
        printExpr(expr.expr)
        sqlBuilder.append(" OVER ")
        printWindow(expr.window)

    def printWindow(window: SqlWindow): Unit =
        sqlBuilder.append("(")
        if window.partitionBy.nonEmpty then
            sqlBuilder.append("PARTITION BY ")
            printList(window.partitionBy)(printExpr)
        if window.orderBy.nonEmpty then
            if window.partitionBy.nonEmpty then
                sqlBuilder.append(" ")
            sqlBuilder.append("ORDER BY ")
            printList(window.orderBy)(printOrderingItem)
        for f <- window.frame do
            f match
                case SqlWindowFrame.Start(unit, start, exclude) =>
                    sqlBuilder.append(" ")
                    sqlBuilder.append(unit.unit)
                    sqlBuilder.append(" ")
                    sqlBuilder.append(start.bound)
                    for e <- exclude do
                        sqlBuilder.append(" ")
                        sqlBuilder.append(e.mode)
                case SqlWindowFrame.Between(unit, start, end, exclude) =>
                    sqlBuilder.append(" ")
                    sqlBuilder.append(unit.unit)
                    sqlBuilder.append(" BETWEEN ")
                    sqlBuilder.append(start.bound)
                    sqlBuilder.append(" AND ")
                    sqlBuilder.append(end.bound)
                    for e <- exclude do
                        sqlBuilder.append(" ")
                        sqlBuilder.append(e.mode)
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

    def printGroupingExpr(expr: SqlExpr.Grouping): Unit =
        sqlBuilder.append("GROUPING(")
        printList(expr.items)(printExpr)
        sqlBuilder.append(")")

    def printIdentFuncExpr(expr: SqlExpr.IdentFunc): Unit =
        sqlBuilder.append(expr.name)

    def printSubstringFuncExpr(expr: SqlExpr.SubstringFunc): Unit =
        sqlBuilder.append("SUBSTRING(")
        printExpr(expr.expr)
        sqlBuilder.append(" FROM ")
        printExpr(expr.from)
        for f <- expr.`for` do
            sqlBuilder.append(" FOR ")
            printExpr(f)
        sqlBuilder.append(")")

    def printTrimFuncExpr(expr: SqlExpr.TrimFunc): Unit =
        sqlBuilder.append("TRIM(")
        for t <- expr.trim do
            for m <- t._1 do
                sqlBuilder.append(m.mode)
            for e <- t._2 do
                if t._1.nonEmpty then
                    sqlBuilder.append(" ")
                printExpr(e)
            sqlBuilder.append(" FROM ")
        printExpr(expr.expr)
        sqlBuilder.append(")")

    def printOverlayFuncExpr(expr: SqlExpr.OverlayFunc): Unit =
        sqlBuilder.append("OVERLAY(")
        printExpr(expr.expr)
        sqlBuilder.append(" PLACING ")
        printExpr(expr.placing)
        sqlBuilder.append(" FROM ")
        printExpr(expr.from)
        for f <- expr.`for` do
            sqlBuilder.append(" FOR ")
            printExpr(f)
        sqlBuilder.append(")")

    def printPositionFuncExpr(expr: SqlExpr.PositionFunc): Unit =
        sqlBuilder.append("POSITION(")
        printExpr(expr.expr)
        sqlBuilder.append(" IN ")
        printExpr(expr.in)
        sqlBuilder.append(")")

    def printExtractFuncExpr(expr: SqlExpr.ExtractFunc): Unit =
        sqlBuilder.append("EXTRACT(")
        sqlBuilder.append(expr.unit.unit)
        sqlBuilder.append(" FROM ")
        printExpr(expr.expr)
        sqlBuilder.append(")")

    def printVectorDistanceFuncExpr(expr: SqlExpr.VectorDistanceFunc): Unit

    def printJsonSerializeFuncExpr(expr: SqlExpr.JsonSerializeFunc): Unit =
        sqlBuilder.append("JSON_SERIALIZE(")
        printExpr(expr)
        for o <- expr.output do
            sqlBuilder.append(" ")
            printJsonOutput(o)
        sqlBuilder.append(")")

    def printJsonParseFuncExpr(expr: SqlExpr.JsonParseFunc): Unit =
        sqlBuilder.append("JSON(")
        printExpr(expr)
        for i <- expr.input do
            sqlBuilder.append(" ")
            printJsonInput(i)
        for u <- expr.uniqueness do
            sqlBuilder.append(" ")
            printJsonUniqueness(u)
        sqlBuilder.append(")")

    def printJsonQueryFuncExpr(expr: SqlExpr.JsonQueryFunc): Unit =
        sqlBuilder.append("JSON_QUERY(")
        printExpr(expr.expr)
        sqlBuilder.append(", ")
        printExpr(expr.path)
        if expr.passingItems.nonEmpty then
            sqlBuilder.append(" PASSING ")
            printList(expr.passingItems)(printJsonPassing)
        for o <- expr.output do
            sqlBuilder.append(" ")
            printJsonOutput(o)
        for w <- expr.wrapper do
            sqlBuilder.append(" ")
            printJsonQueryWrapperBehavior(w)
        for q <- expr.quotes do
            sqlBuilder.append(" ")
            printJsonQueryQuotesBehavior(q)
        for e <- expr.onEmpty do
            sqlBuilder.append(" ")
            printJsonQueryEmptyBehavior(e)
        for e <- expr.onError do
            sqlBuilder.append(" ")
            printJsonQueryErrorBehavior(e)
        sqlBuilder.append(")")

    def printJsonValueFuncExpr(expr: SqlExpr.JsonValueFunc): Unit =
        sqlBuilder.append("JSON_VALUE(")
        printExpr(expr.expr)
        sqlBuilder.append(", ")
        printExpr(expr.path)
        if expr.passingItems.nonEmpty then
            sqlBuilder.append(" PASSING ")
            printList(expr.passingItems)(printJsonPassing)
        for o <- expr.output do
            sqlBuilder.append(" ")
            printJsonOutput(o)
        for e <- expr.onEmpty do
            sqlBuilder.append(" ")
            printJsonValueEmptyBehavior(e)
        for e <- expr.onError do
            sqlBuilder.append(" ")
            printJsonValueErrorBehavior(e)
        sqlBuilder.append(")")

    def printJsonObjectFuncExpr(expr: SqlExpr.JsonObjectFunc): Unit =
        def printItem(item: (SqlExpr, SqlExpr)): Unit =
            printExpr(item._1)
            sqlBuilder.append(" VALUE ")
            printExpr(item._2)
        sqlBuilder.append("JSON_OBJECT(")
        printList(expr.items)(printItem)
        for n <- expr.nullConstructor do
            sqlBuilder.append(" ")
            printJsonNullConstructor(n)
        for u <- expr.uniqueness do
            sqlBuilder.append(" ")
            printJsonUniqueness(u)
        for o <- expr.output do
            sqlBuilder.append(" ")
            printJsonOutput(o)
        sqlBuilder.append(")")

    def printJsonArrayFuncExpr(expr: SqlExpr.JsonArrayFunc): Unit =
        def printItem(item: (SqlExpr, Option[SqlJsonInput])): Unit =
            printExpr(item._1)
            for i <- item._2 do
                sqlBuilder.append(" ")
                printJsonInput(i)
        sqlBuilder.append("JSON_ARRAY(")
        printList(expr.items)(printItem)
        for n <- expr.nullConstructor do
            sqlBuilder.append(" ")
            printJsonNullConstructor(n)
        for o <- expr.output do
            sqlBuilder.append(" ")
            printJsonOutput(o)
        sqlBuilder.append(")")

    def printJsonExistsFuncExpr(expr: SqlExpr.JsonExistsFunc): Unit =
        sqlBuilder.append("JSON_EXISTS(")
        printExpr(expr.expr)
        sqlBuilder.append(", ")
        printExpr(expr.path)
        if expr.passingItems.nonEmpty then
            sqlBuilder.append(" PASSING ")
            printList(expr.passingItems)(printJsonPassing)
        for e <- expr.onError do
            sqlBuilder.append(" ")
            printJsonExistsErrorBehavior(e)
        sqlBuilder.append(")")

    def printJsonEncoding(encoding: SqlJsonEncoding): Unit =
        sqlBuilder.append("ENCODING ")
        sqlBuilder.append(encoding.encoding)

    def printJsonInput(input: SqlJsonInput): Unit =
        sqlBuilder.append("FORMAT JSON")
        for e <- input.format do
            sqlBuilder.append(" ENCODING ")
            sqlBuilder.append(e.encoding)

    def printJsonOutput(output: SqlJsonOutput): Unit =
        sqlBuilder.append("RETURNING ")
        printType(output.`type`)
        for f <- output.format do
            sqlBuilder.append(" FORMAT JSON")
            for e <- f do
                sqlBuilder.append(" ENCODING ")
                sqlBuilder.append(e.encoding)

    def printJsonUniqueness(uniqueness: SqlJsonUniqueness): Unit =
        sqlBuilder.append(uniqueness.uniqueness)

    def printJsonPassing(passing: SqlJsonPassing): Unit =
        printExpr(passing.expr)
        sqlBuilder.append(s" AS $leftQuote${passing.alias}$rightQuote")

    def printJsonNullConstructor(cons: SqlJsonNullConstructor): Unit =
        sqlBuilder.append(cons.constructor)

    def printJsonQueryWrapperBehavior(behavior: SqlJsonQueryWrapperBehavior): Unit =
        behavior match
            case SqlJsonQueryWrapperBehavior.Without(a) =>
                sqlBuilder.append("WITHOUT")
                if a then
                    sqlBuilder.append(" ARRAY")
                sqlBuilder.append(" WRAPPER")
            case SqlJsonQueryWrapperBehavior.With(mode, a) =>
                sqlBuilder.append("WITH")
                for m <- mode do
                    sqlBuilder.append(" ")
                    sqlBuilder.append(m.mode)
                if a then
                    sqlBuilder.append(" ARRAY")
                sqlBuilder.append(" WRAPPER")

    def printJsonQueryQuotesBehavior(behavior: SqlJsonQueryQuotesBehavior): Unit =
        sqlBuilder.append(behavior.mode.mode)
        sqlBuilder.append(" QUOTES")
        if behavior.onScalarString then
            sqlBuilder.append(" ON SCALAR STRING")

    def printJsonQueryEmptyBehavior(behavior: SqlJsonQueryEmptyBehavior): Unit =
        behavior match
            case SqlJsonQueryEmptyBehavior.Error =>
                sqlBuilder.append("ERROR")
            case SqlJsonQueryEmptyBehavior.Null =>
                sqlBuilder.append("NULL")
            case SqlJsonQueryEmptyBehavior.EmptyObject =>
                sqlBuilder.append("EMPTY OBJECT")
            case SqlJsonQueryEmptyBehavior.EmptyArray =>
                sqlBuilder.append("EMPTY ARRAY")
            case SqlJsonQueryEmptyBehavior.Default(expr) =>
                sqlBuilder.append("DEFAULT ")
                printExpr(expr)
        sqlBuilder.append(" ON EMPTY")

    def printJsonQueryErrorBehavior(behavior: SqlJsonQueryErrorBehavior): Unit =
        behavior match
            case SqlJsonQueryErrorBehavior.Error =>
                sqlBuilder.append("ERROR")
            case SqlJsonQueryErrorBehavior.Null =>
                sqlBuilder.append("NULL")
            case SqlJsonQueryErrorBehavior.EmptyObject =>
                sqlBuilder.append("EMPTY OBJECT")
            case SqlJsonQueryErrorBehavior.EmptyArray =>
                sqlBuilder.append("EMPTY ARRAY")
            case SqlJsonQueryErrorBehavior.Default(expr) =>
                sqlBuilder.append("DEFAULT ")
                printExpr(expr)
        sqlBuilder.append(" ON ERROR")

    def printJsonValueEmptyBehavior(behavior: SqlJsonValueEmptyBehavior): Unit =
        behavior match
            case SqlJsonValueEmptyBehavior.Error =>
                sqlBuilder.append("ERROR")
            case SqlJsonValueEmptyBehavior.Null =>
                sqlBuilder.append("NULL")
            case SqlJsonValueEmptyBehavior.Default(expr) =>
                sqlBuilder.append("DEFAULT ")
                printExpr(expr)
        sqlBuilder.append(" ON EMPTY")

    def printJsonValueErrorBehavior(behavior: SqlJsonValueErrorBehavior): Unit =
        behavior match
            case SqlJsonValueErrorBehavior.Error =>
                sqlBuilder.append("ERROR")
            case SqlJsonValueErrorBehavior.Null =>
                sqlBuilder.append("NULL")
            case SqlJsonValueErrorBehavior.Default(expr) =>
                sqlBuilder.append("DEFAULT ")
                printExpr(expr)
        sqlBuilder.append(" ON ERROR")

    def printJsonExistsErrorBehavior(behavior: SqlJsonExistsErrorBehavior): Unit =
        sqlBuilder.append(behavior.mode)
        sqlBuilder.append(" ON ERROR")

    def printJsonTableErrorBehavior(behavior: SqlJsonTableErrorBehavior): Unit =
        behavior match
            case SqlJsonTableErrorBehavior.Error =>
                sqlBuilder.append("ERROR")
            case SqlJsonTableErrorBehavior.Empty =>
                sqlBuilder.append("EMPTY")
            case SqlJsonTableErrorBehavior.EmptyArray =>
                sqlBuilder.append("EMPTY ARRAY")
        sqlBuilder.append(" ON ERROR")

    def printCountAsteriskFuncExpr(expr: SqlExpr.CountAsteriskFunc): Unit =
        sqlBuilder.append("COUNT(")
        for n <- expr.tableName do
            sqlBuilder.append(s"$leftQuote$n$rightQuote.")
        sqlBuilder.append("*)")
        for f <- expr.filter do
            sqlBuilder.append(" FILTER (WHERE ")
            printExpr(f)
            sqlBuilder.append(")")

    def printListAggFuncExpr(expr: SqlExpr.ListAggFunc): Unit =
        sqlBuilder.append("LISTAGG(")
        expr.quantifier.foreach: q => 
            sqlBuilder.append(q.quantifier)
            sqlBuilder.append(" ")
        printExpr(expr.expr)
        sqlBuilder.append(", ")
        printExpr(expr.separator)
        for o <- expr.onOverflow do
            sqlBuilder.append(" ON OVERFLOW ")
            o.mode match
                case SqlListAggOnOverflowMode.Error =>
                    sqlBuilder.append("ERROR")
                case SqlListAggOnOverflowMode.Truncate(e) =>
                    sqlBuilder.append("TRUNCATE ")
                    printExpr(e)
            sqlBuilder.append(" ")
            sqlBuilder.append(o.countMode.mode)
        sqlBuilder.append(")")
        if expr.withinGroup.nonEmpty then
            sqlBuilder.append(" WITHIN GROUP (ORDER BY ")
            printList(expr.withinGroup)(printOrderingItem)
            sqlBuilder.append(")")
        for f <- expr.filter do
            sqlBuilder.append(" FILTER (WHERE ")
            printExpr(f)
            sqlBuilder.append(")")

    def printJsonObjectAggFuncExpr(expr: SqlExpr.JsonObjectAggFunc): Unit =
        sqlBuilder.append("JSON_OBJECTAGG(")
        printExpr(expr.item._1)
        sqlBuilder.append(" VALUE ")
        printExpr(expr.item._2)
        for n <- expr.nullConstructor do
            sqlBuilder.append(" ")
            printJsonNullConstructor(n)
        for u <- expr.uniqueness do
            sqlBuilder.append(" ")
            printJsonUniqueness(u)
        for o <- expr.output do
            sqlBuilder.append(" ")
            printJsonOutput(o)
        sqlBuilder.append(")")
        for f <- expr.filter do
            sqlBuilder.append(" FILTER (WHERE ")
            printExpr(f)
            sqlBuilder.append(")")

    def printJsonArrayAggFuncExpr(expr: SqlExpr.JsonArrayAggFunc): Unit =
        sqlBuilder.append("JSON_ARRAYAGG(")
        printExpr(expr.item._1)
        for i <- expr.item._2 do
            sqlBuilder.append(" ")
            printJsonInput(i)
        if expr.orderBy.nonEmpty then
            sqlBuilder.append(" ORDER BY ")
            printList(expr.orderBy)(printOrderingItem)
        for n <- expr.nullConstructor do
            sqlBuilder.append(" ")
            printJsonNullConstructor(n)
        for o <- expr.output do
            sqlBuilder.append(" ")
            printJsonOutput(o)
        sqlBuilder.append(")")
        for f <- expr.filter do
            sqlBuilder.append(" FILTER (WHERE ")
            printExpr(f)
            sqlBuilder.append(")")

    def printNullsTreatmentFuncExpr(expr: SqlExpr.NullsTreatmentFunc): Unit =
        sqlBuilder.append(expr.name)
        sqlBuilder.append("(")
        printList(expr.args)(printExpr)
        sqlBuilder.append(")")
        for m <- expr.nullsMode do
            sqlBuilder.append(" ")
            sqlBuilder.append(m.mode)

    def printNthValueFuncExpr(expr: SqlExpr.NthValueFunc): Unit =
        sqlBuilder.append("NTH_VALUE(")
        printList(List(expr.expr, expr.row))(printExpr)
        sqlBuilder.append(")")
        for m <- expr.fromMode do
            sqlBuilder.append(" ")
            sqlBuilder.append(m.mode)
        for m <- expr.nullsMode do
            sqlBuilder.append(" ")
            sqlBuilder.append(m.mode)

    def printStandardFuncExpr(expr: SqlExpr.StandardFunc): Unit =
        sqlBuilder.append(expr.name)
        sqlBuilder.append("(")
        expr.quantifier.foreach: q => 
            sqlBuilder.append(q.quantifier)
            sqlBuilder.append(" ")
        printList(expr.args)(printExpr)
        if expr.orderBy.nonEmpty then
            sqlBuilder.append(" ORDER BY ")
            printList(expr.orderBy)(printOrderingItem)
        sqlBuilder.append(")")
        if expr.withinGroup.nonEmpty then
            sqlBuilder.append(" WITHIN GROUP (ORDER BY ")
            printList(expr.withinGroup)(printOrderingItem)
            sqlBuilder.append(")")
        for f <- expr.filter do
            sqlBuilder.append(" FILTER (WHERE ")
            printExpr(f)
            sqlBuilder.append(")")

    def printMatchPhaseExpr(expr: SqlExpr.MatchPhase): Unit =
        sqlBuilder.append(expr.phase.phase)
        sqlBuilder.append(" ")
        printExpr(expr.expr)

    def printCustomExpr(expr: SqlExpr.Custom): Unit =
        sqlBuilder.append(expr.snippet)

    def printTableAlias(alias: SqlTableAlias): Unit =
        sqlBuilder.append(s" AS $leftQuote${alias.tableAlias}$rightQuote")
        if alias.columnAlias.nonEmpty then
            sqlBuilder.append("(")
            printList(alias.columnAlias)(i => sqlBuilder.append(s"$leftQuote$i$rightQuote"))
            sqlBuilder.append(")")

    def printStandardTable(table: SqlTable.Standard): Unit =
        sqlBuilder.append(s"$leftQuote${table.name}$rightQuote")
        for a <- table.alias do
            printTableAlias(a)
        for m <- table.matchRecognize do
            sqlBuilder.append(" ")
            printMatchRecognize(m)
        for s <- table.sample do
            sqlBuilder.append(" TABLESAMPLE ")
            sqlBuilder.append(s.mode.mode)
            sqlBuilder.append(" (")
            printExpr(s.percentage)
            sqlBuilder.append(")")
            for r <- s.repeatable do
                sqlBuilder.append(" REPEATABLE (")
                printExpr(r)
                sqlBuilder.append(")")

    def printFuncTable(table: SqlTable.Func): Unit =
        if table.lateral then sqlBuilder.append("LATERAL ")
        sqlBuilder.append(table.name)
        sqlBuilder.append("(")
        printList(table.args)(printExpr)
        sqlBuilder.append(")")
        if table.withOrd then
            sqlBuilder.append(" WITH ORDINALITY")
        for a <- table.alias do
            printTableAlias(a)
        for m <- table.matchRecognize do
            sqlBuilder.append(" ")
            printMatchRecognize(m)

    def printSubQueryTable(table: SqlTable.SubQuery): Unit =
        if table.lateral then sqlBuilder.append("LATERAL ")
        sqlBuilder.append("(\n")
        push()
        printQuery(table.query)
        pull()
        sqlBuilder.append("\n")
        printSpace()
        sqlBuilder.append(")")
        for a <- table.alias do
            printTableAlias(a)
        for m <- table.matchRecognize do
            sqlBuilder.append(" ")
            printMatchRecognize(m)

    def printJsonTable(table: SqlTable.Json): Unit =
        def printJsonTableColumn(column: SqlJsonTableColumn): Unit =
            column match
                case SqlJsonTableColumn.Ordinality(name) =>
                    sqlBuilder.append(s"$leftQuote$name$rightQuote FOR ORDINALITY")
                case c: SqlJsonTableColumn.Column =>
                    sqlBuilder.append(s"$leftQuote${c.name}$rightQuote")
                    sqlBuilder.append(" ")
                    printType(c.`type`)
                    for f <- c.format do
                        sqlBuilder.append(" FORMAT JSON")
                        for e <- f do
                            sqlBuilder.append(" ENCODING ")
                            sqlBuilder.append(e.encoding)
                    for p <- c.path do
                        sqlBuilder.append(" PATH ")
                        printExpr(p)
                    for w <- c.wrapper do
                        sqlBuilder.append(" ")
                        printJsonQueryWrapperBehavior(w)
                    for q <- c.quotes do
                        sqlBuilder.append(" ")
                        printJsonQueryQuotesBehavior(q)
                    for b <- c.onEmpty do
                        sqlBuilder.append(" ")
                        printJsonQueryEmptyBehavior(b)
                    for b <- c.onError do
                        sqlBuilder.append(" ")
                        printJsonQueryErrorBehavior(b)
                case e: SqlJsonTableColumn.Exists =>
                    sqlBuilder.append(s"$leftQuote${e.name}$rightQuote")
                    sqlBuilder.append(" ")
                    printType(e.`type`)
                    for p <- e.path do
                        sqlBuilder.append(" PATH ")
                        printExpr(p)
                    for b <- e.onError do
                        sqlBuilder.append(" ")
                        printJsonExistsErrorBehavior(b)
                case n: SqlJsonTableColumn.Nested =>
                    sqlBuilder.append("NESTED PATH ")
                    printExpr(n.path)
                    for a <- n.pathAlias do
                        sqlBuilder.append(s" AS $leftQuote$a$rightQuote")
                    sqlBuilder.append(" COLUMNS(\n")
                    push()
                    printList(n.columns, ",\n"): i =>
                        printSpace()
                        printJsonTableColumn(i)
                    pull()
                    sqlBuilder.append("\n")
                    printSpace()
                    sqlBuilder.append(")")

        if table.lateral then sqlBuilder.append("LATERAL ")
        sqlBuilder.append("JSON_TABLE(\n")
        push()
        printSpace()
        printExpr(table.expr)
        sqlBuilder.append(", ")
        printExpr(table.path)
        for a <- table.pathAlias do
            sqlBuilder.append(s" AS $leftQuote$a$rightQuote")
        if table.passingItems.nonEmpty then
            sqlBuilder.append(" PASSING ")
            printList(table.passingItems)(printJsonPassing)
        sqlBuilder.append(" COLUMNS(\n")
        push()
        printList(table.columns, ",\n"): i =>
            printSpace()
            printJsonTableColumn(i)
        pull()
        sqlBuilder.append("\n")
        printSpace()
        sqlBuilder.append(")")
        for b <- table.onError do
            sqlBuilder.append("\n")
            printSpace()
            printJsonTableErrorBehavior(b)
        pull()
        sqlBuilder.append("\n")
        printSpace()
        sqlBuilder.append(")")
        for a <- table.alias do
            printTableAlias(a)
        for m <- table.matchRecognize do
            sqlBuilder.append(" ")
            printMatchRecognize(m)

    def printJoinTable(table: SqlTable.Join): Unit =
        printTable(table.left)
        sqlBuilder.append("\n")
        printSpace()
        sqlBuilder.append(s"${table.joinType.`type`} ")
        table.right match
            case _: SqlTable.Join =>
                sqlBuilder.append("(")
                sqlBuilder.append("\n")
                push()
                printSpace()
                printTable(table.right)
                printSpace()
                pull()
                sqlBuilder.append("\n")
                printSpace()
                sqlBuilder.append(")")
            case _ =>
                printTable(table.right)
        for c <- table.condition do
            c match
                case SqlJoinCondition.On(onCondition) =>
                    sqlBuilder.append(" ON ")
                    printExpr(onCondition)
                case SqlJoinCondition.Using(usingCondition) =>
                    sqlBuilder.append(" USING (")
                    printList(usingCondition): n =>
                        sqlBuilder.append(s"$leftQuote$n$rightQuote")
                    sqlBuilder.append(")")

    def printTable(table: SqlTable): Unit = 
        table match
            case s: SqlTable.Standard => printStandardTable(s)
            case f: SqlTable.Func => printFuncTable(f)
            case s: SqlTable.SubQuery => printSubQueryTable(s)
            case j: SqlTable.Json => printJsonTable(j)
            case j: SqlTable.Join => printJoinTable(j)

    def printMatchRecognize(matchRecognize: SqlMatchRecognize): Unit =
        def printMeasure(measure: SqlMeasureItem): Unit =
            printExpr(measure.expr)
            sqlBuilder.append(s" AS $leftQuote${measure.alias}$rightQuote")

        sqlBuilder.append("\n")
        printSpace()
        sqlBuilder.append("MATCH_RECOGNIZE(")
        push()
        if matchRecognize.partitionBy.nonEmpty then
            sqlBuilder.append("\n")
            printSpace()
            sqlBuilder.append("PARTITION BY\n")
            printList(matchRecognize.partitionBy, ",\n")(printExpr |> printWithSpace)
        if matchRecognize.orderBy.nonEmpty then
            sqlBuilder.append("\n")
            printSpace()
            sqlBuilder.append("ORDER BY\n")
            printList(matchRecognize.orderBy, ",\n")(printOrderingItem |> printWithSpace)
        if matchRecognize.measures.nonEmpty then
            sqlBuilder.append("\n")
            printSpace()
            sqlBuilder.append("MEASURES\n")
            printList(matchRecognize.measures, ",\n")(printMeasure |> printWithSpace)
        for m <- matchRecognize.rowsPerMatch do
            sqlBuilder.append("\n")
            printSpace()
            m match
                case SqlPatternRowsPerMatchMode.OneRow =>
                    sqlBuilder.append("ONE ROW PER MATCH")
                case SqlPatternRowsPerMatchMode.AllRows(mode) =>
                    sqlBuilder.append("ALL ROWS PER MATCH")
                    for em <- mode do
                        sqlBuilder.append(" ")
                        sqlBuilder.append(em.mode)
        pull()
        sqlBuilder.append("\n")
        printRowPattern(matchRecognize.rowPattern)
        sqlBuilder.append("\n")
        printSpace()
        sqlBuilder.append(")")
        for a <- matchRecognize.alias do
            printTableAlias(a)

    def printSelectItem(item: SqlSelectItem): Unit = item match
        case SqlSelectItem.Asterisk(table) =>
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

    def printWindowItem(item: SqlWindowItem): Unit =
        sqlBuilder.append(s"$leftQuote${item.name}$rightQuote AS ")
        printWindow(item.window)

    def printOrderingItem(item: SqlOrderingItem): Unit =
        printExpr(item.expr)
        val order = item.ordering match
            case None | Some(SqlOrdering.Asc) => SqlOrdering.Asc
            case _ => SqlOrdering.Desc
        sqlBuilder.append(s" ${order.order}")
        for o <- item.nullsOrdering do
            sqlBuilder.append(s" ${o.order}")

    def printLimit(limit: SqlLimit): Unit =
        for o <- limit.offset do
            sqlBuilder.append("OFFSET ")
            printExpr(o)
            sqlBuilder.append(" ROWS")
        for f <- limit.fetch do
            if limit.offset.isDefined then
                sqlBuilder.append(" ")
            sqlBuilder.append("FETCH")
            if limit.offset.isDefined then
                sqlBuilder.append(" NEXT ")
            else
                sqlBuilder.append(" FIRST ")
            printExpr(f.limit)
            if f.unit == SqlFetchUnit.Percentage then
                sqlBuilder.append(" ")
            sqlBuilder.append(f.unit.unit)
            sqlBuilder.append(" ROWS ")
            sqlBuilder.append(f.mode.mode)

    def printLock(lock: SqlLock): Unit =
        sqlBuilder.append(lock.lockMode)
        for w <- lock.waitMode do
            sqlBuilder.append(" ")
            sqlBuilder.append(w.waitMode)

    def printCteRecursive(): Unit = sqlBuilder.append(" RECURSIVE")

    def printRowPattern(pattern: SqlRowPattern): Unit =
        def printQuantifier(quantifier: SqlRowPatternQuantifier): Unit =
            quantifier match
                case SqlRowPatternQuantifier.Asterisk(false) =>
                    sqlBuilder.append("*")
                case SqlRowPatternQuantifier.Asterisk(true) =>
                    sqlBuilder.append("*?")
                case SqlRowPatternQuantifier.Plus(false) =>
                    sqlBuilder.append("+")
                case SqlRowPatternQuantifier.Plus(true) =>
                    sqlBuilder.append("+?")
                case SqlRowPatternQuantifier.Question(false) =>
                    sqlBuilder.append("?")
                case SqlRowPatternQuantifier.Question(true) =>
                    sqlBuilder.append("??")
                case SqlRowPatternQuantifier.Between(start, end, q) =>
                    sqlBuilder.append("{")
                    for s <- start do
                        printExpr(s)
                    sqlBuilder.append(",")
                    for e <- end do
                        printExpr(e)
                    sqlBuilder.append("}")
                    if q then
                        sqlBuilder.append("?")
                case SqlRowPatternQuantifier.Quantity(q) =>
                    sqlBuilder.append("{")
                    printExpr(q)
                    sqlBuilder.append("}")

        def printPatternTerm(pattern: SqlRowPatternTerm): Unit =
            pattern match
                case SqlRowPatternTerm.Pattern(name, _) =>
                    sqlBuilder.append(s"$leftQuote$name$rightQuote")
                case SqlRowPatternTerm.Circumflex(_) =>
                    sqlBuilder.append("^")
                case SqlRowPatternTerm.Dollar(_) =>
                    sqlBuilder.append("$")
                case SqlRowPatternTerm.NonGreedy(term, _) =>
                    sqlBuilder.append("{-")
                    printPatternTerm(term)
                    sqlBuilder.append("-}")
                case SqlRowPatternTerm.Permute(terms, _) =>
                    sqlBuilder.append("PERMUTE(")
                    printList(terms)(printPatternTerm)
                case SqlRowPatternTerm.Then(left, right, quantifier) =>
                    if quantifier.isDefined then
                        sqlBuilder.append("(")
                    left match
                        case SqlRowPatternTerm.Or(_, _, _) =>
                            sqlBuilder.append("(")
                            printPatternTerm(left)
                            sqlBuilder.append(")")
                        case _ =>
                            printPatternTerm(left)
                    sqlBuilder.append(" ")
                    right match
                        case SqlRowPatternTerm.Or(_, _, _) =>
                            sqlBuilder.append("(")
                            printPatternTerm(right)
                            sqlBuilder.append(")")
                        case _ =>
                            printPatternTerm(right)
                    if quantifier.isDefined then
                        sqlBuilder.append(")")
                case SqlRowPatternTerm.Or(left, right, quantifier) =>
                    if quantifier.isDefined then
                        sqlBuilder.append("(")
                    printPatternTerm(left)
                    sqlBuilder.append(" | ")
                    printPatternTerm(right)
                    if quantifier.isDefined then
                        sqlBuilder.append(")")

            for q <- pattern.quantifier do
                printQuantifier(q)

        def printSubsetItem(subset: SqlRowPatternSubsetItem): Unit =
            sqlBuilder.append(s"$leftQuote${subset.name}$rightQuote = (")
            printList(subset.patternNames)(i => sqlBuilder.append(s"$leftQuote$i$rightQuote"))
            sqlBuilder.append(")")

        def printDefineItem(define: SqlRowPatternDefineItem): Unit =
            sqlBuilder.append(s"$leftQuote${define.name}$rightQuote AS ")
            printExpr(define.expr)
        
        push()
        for m <- pattern.afterMatch do
            printSpace()
            sqlBuilder.append("AFTER MATCH ")
            m match
                case SqlRowPatternSkipMode.SkipToNextRow =>
                    sqlBuilder.append("SKIP TO NEXT ROW")
                case SqlRowPatternSkipMode.SkipPastLastRow =>
                    sqlBuilder.append("SKIP PAST LAST ROW")
                case SqlRowPatternSkipMode.SkipToFirst(name) =>
                    sqlBuilder.append(s"SKIP TO FIRST $leftQuote$name$rightQuote")
                case SqlRowPatternSkipMode.SkipToLast(name) =>
                    sqlBuilder.append(s"SKIP TO LAST $leftQuote$name$rightQuote")
                case SqlRowPatternSkipMode.SkipTo(name) =>
                    sqlBuilder.append(s"SKIP TO $leftQuote$name$rightQuote")
        sqlBuilder.append("\n")
        printSpace()
        for s <- pattern.strategy do
            sqlBuilder.append(s.strategy)
            sqlBuilder.append(" ")
        sqlBuilder.append("PATTERN (")
        printPatternTerm(pattern.pattern)
        sqlBuilder.append(")")
        if pattern.subset.nonEmpty then
            sqlBuilder.append("\n")
            printSpace()
            sqlBuilder.append("SUBSET\n")
            printList(pattern.subset, ",\n")(printSubsetItem |> printWithSpace)
        if pattern.define.nonEmpty then
            sqlBuilder.append("\n")
            printSpace()
            sqlBuilder.append("DEFINE\n")
            printList(pattern.define, ",\n")(printDefineItem |> printWithSpace)
        pull()
    
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