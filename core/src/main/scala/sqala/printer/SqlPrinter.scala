package sqala.printer

import sqala.ast.expr.*
import sqala.ast.group.SqlGroupingItem
import sqala.ast.limit.{SqlFetchMode, SqlFetchUnit, SqlLimit}
import sqala.ast.order.{SqlNullsOrdering, SqlOrdering, SqlOrderingItem}
import sqala.ast.quantifier.SqlQuantifier
import sqala.ast.statement.*
import sqala.ast.table.*
import sqala.util.|>

abstract class SqlPrinter(val standardEscapeStrings: Boolean):
    val sqlBuilder: StringBuilder = StringBuilder()

    val leftQuote: Char = '"'

    val rightQuote: Char = '"'

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
            printExpr(i.column)
            sqlBuilder.append(" = ")
            printExpr(i.value)

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

    def printQuantifier(quantifier: SqlQuantifier): Unit =
        quantifier match
            case SqlQuantifier.All =>
                sqlBuilder.append("ALL")
            case SqlQuantifier.Distinct =>
                sqlBuilder.append("DISTINCT")
            case SqlQuantifier.Custom(q) =>
                sqlBuilder.append(q)

    def printSelect(select: SqlQuery.Select): Unit =
        printSpace()
        sqlBuilder.append("SELECT")

        select.quantifier.foreach: q => 
            sqlBuilder.append(" ")
            printQuantifier(q)

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
                printQuantifier(q)
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
        set.operator match
            case SqlSetOperator.Union(_) =>
                sqlBuilder.append("UNION")
            case SqlSetOperator.Except(_) =>
                sqlBuilder.append("EXCEPT")
            case SqlSetOperator.Intersect(_) =>
                sqlBuilder.append("INTERSECT")
        for q <- set.operator.quantifier do
            sqlBuilder.append(" ")
            printQuantifier(q)
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
            printIdent(item.name)
            if item.columnNames.nonEmpty then
                sqlBuilder.append("(")
                printList(item.columnNames)(c => printIdent(c))
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
        case u: SqlExpr.Unary => printUnaryExpr(u)
        case b: SqlExpr.Binary => printBinaryExpr(b)
        case n: SqlExpr.NullTest => printNullTestExpr(n)
        case j: SqlExpr.JsonTest => printJsonTestExpr(j)
        case b: SqlExpr.Between => printBetweenExpr(b)
        case l: SqlExpr.Like => printLikeExpr(l)
        case s: SqlExpr.SimilarTo => printSimilarToExpr(s)
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
        case g: SqlExpr.GeneralFunc => printGeneralFuncExpr(g)
        case m: SqlExpr.MatchPhase => printMatchPhaseExpr(m)
        case c: SqlExpr.Custom => printCustomExpr(c)

    def printColumnExpr(expr: SqlExpr.Column): Unit =
        for n <- expr.tableName do
            printIdent(n)
            sqlBuilder.append(".")
        printIdent(expr.columnName)

    def printNullLiteralExpr(): Unit = sqlBuilder.append("NULL")

    def printStringLiteralExpr(expr: SqlExpr.StringLiteral): Unit =
        printChars(expr.string)

    def printNumberLiteralExpr(expr: SqlExpr.NumberLiteral[?]): Unit =
        sqlBuilder.append(expr.number)

    def printBooleanLiteralExpr(expr: SqlExpr.BooleanLiteral): Unit =
        if expr.boolean then
            sqlBuilder.append("TRUE")
        else
            sqlBuilder.append("FALSE")

    def printTimeLiteralExpr(expr: SqlExpr.TimeLiteral): Unit =
        def printUnit(unit: SqlTimeLiteralUnit): Unit =
            unit match
                case SqlTimeLiteralUnit.Timestamp =>
                    sqlBuilder.append("TIMESTAMP")
                case SqlTimeLiteralUnit.Date =>
                    sqlBuilder.append("DATE")
                case SqlTimeLiteralUnit.Time =>
                    sqlBuilder.append("TIME")
                    
        printUnit(expr.unit)
        sqlBuilder.append(" ")
        printChars(expr.time)

    def printTimeUnit(unit: SqlTimeUnit): Unit =
        unit match
            case SqlTimeUnit.Year =>
                sqlBuilder.append("YEAR")
            case SqlTimeUnit.Month =>
                sqlBuilder.append("MONTH")
            case SqlTimeUnit.Day =>
                sqlBuilder.append("DAY")
            case SqlTimeUnit.Hour =>
                sqlBuilder.append("HOUR")
            case SqlTimeUnit.Minute =>
                sqlBuilder.append("MINUTE")
            case SqlTimeUnit.Second =>
                sqlBuilder.append("SECOND")
            case SqlTimeUnit.Custom(unit) =>
                sqlBuilder.append(unit)

    def printIntervalLiteralExpr(expr: SqlExpr.IntervalLiteral): Unit =
        sqlBuilder.append("INTERVAL ")
        printChars(expr.value)
        sqlBuilder.append(" ")
        expr.field match
            case SqlIntervalField.To(s, e) =>
                printTimeUnit(s)
                sqlBuilder.append(" TO ")
                printTimeUnit(e)
            case SqlIntervalField.Single(u) =>
                printTimeUnit(u)

    def printTupleExpr(expr: SqlExpr.Tuple): Unit =
        sqlBuilder.append("(")
        printList(expr.items)(printExpr)
        sqlBuilder.append(")")

    def printArrayExpr(expr: SqlExpr.Array): Unit =
        sqlBuilder.append("ARRAY[")
        printList(expr.items)(printExpr)
        sqlBuilder.append("]")

    def printUnaryOperator(operator: SqlUnaryOperator): Unit =
        operator match
            case SqlUnaryOperator.Positive =>
                sqlBuilder.append("+")
            case SqlUnaryOperator.Negative =>
                sqlBuilder.append("-")
            case SqlUnaryOperator.Not =>
                sqlBuilder.append("NOT")
            case SqlUnaryOperator.Custom(op) =>
                sqlBuilder.append(op)

    def printUnaryExpr(expr: SqlExpr.Unary): Unit =
        printUnaryOperator(expr.operator)
        sqlBuilder.append("(")
        printExpr(expr.expr)
        sqlBuilder.append(")")

    def printBinaryOperator(operator: SqlBinaryOperator): Unit =
        operator match
            case SqlBinaryOperator.Times =>
                sqlBuilder.append("*")
            case SqlBinaryOperator.Div =>
                sqlBuilder.append("/")
            case SqlBinaryOperator.Plus =>
                sqlBuilder.append("+")
            case SqlBinaryOperator.Minus =>
                sqlBuilder.append("-")
            case SqlBinaryOperator.Concat =>
                sqlBuilder.append("||")
            case SqlBinaryOperator.Equal =>
                sqlBuilder.append("=")
            case SqlBinaryOperator.NotEqual =>
                sqlBuilder.append("<>")
            case SqlBinaryOperator.IsDistinctFrom =>
                sqlBuilder.append("IS DISTINCT FROM")
            case SqlBinaryOperator.IsNotDistinctFrom =>
                sqlBuilder.append("IS NOT DISTINCT FROM")
            case SqlBinaryOperator.In =>
                sqlBuilder.append("IN")
            case SqlBinaryOperator.NotIn =>
                sqlBuilder.append("NOT IN")
            case SqlBinaryOperator.GreaterThan =>
                sqlBuilder.append(">")
            case SqlBinaryOperator.GreaterThanEqual =>
                sqlBuilder.append(">=")
            case SqlBinaryOperator.LessThan =>
                sqlBuilder.append("<")
            case SqlBinaryOperator.LessThanEqual =>
                sqlBuilder.append("<=")
            case SqlBinaryOperator.Overlaps =>
                sqlBuilder.append("OVERLAPS")
            case SqlBinaryOperator.And =>
                sqlBuilder.append("AND")
            case SqlBinaryOperator.Or =>
                sqlBuilder.append("OR")
            case SqlBinaryOperator.Custom(op) =>
                sqlBuilder.append(op)

    def printBinaryExpr(expr: SqlExpr.Binary): Unit =
        def hasBracketsLeft(parent: SqlExpr.Binary, child: SqlExpr): Boolean =
            child match
                case SqlExpr.Binary(_, op, _)
                    if op.precedence < parent.operator.precedence || op.precedence == 0 => true
                case SqlExpr.Like(_, _, _, _)
                    if SqlBinaryOperator.Equal.precedence < parent.operator.precedence => true
                case SqlExpr.SimilarTo(_, _, _, _)
                    if SqlBinaryOperator.Equal.precedence < parent.operator.precedence => true
                case _ => false

        def hasBracketsRight(parent: SqlExpr.Binary, child: SqlExpr): Boolean =
            child match
                case SqlExpr.Binary(_, op, _)
                    if op.precedence <= parent.operator.precedence => true
                case SqlExpr.Like(_, _, _, _)
                    if SqlBinaryOperator.Equal.precedence <= parent.operator.precedence => true
                case SqlExpr.SimilarTo(_, _, _, _)
                    if SqlBinaryOperator.Equal.precedence <= parent.operator.precedence => true
                case _ => false

        if hasBracketsLeft(expr, expr.left) then
            sqlBuilder.append("(")
            printExpr(expr.left)
            sqlBuilder.append(")")
        else
            printExpr(expr.left)

        sqlBuilder.append(" ")
        printBinaryOperator(expr.operator)
        sqlBuilder.append(" ")

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
        def printType(`type`: SqlJsonNodeType): Unit =
            `type` match
                case SqlJsonNodeType.Value =>
                    sqlBuilder.append("VALUE")
                case SqlJsonNodeType.Object =>
                    sqlBuilder.append("OBJECT")
                case SqlJsonNodeType.Array =>
                    sqlBuilder.append("ARRAY")
                case SqlJsonNodeType.Scalar =>
                    sqlBuilder.append("SCALAR")

        printExpr(expr.expr)
        if expr.not then sqlBuilder.append(" IS NOT JSON")
        else sqlBuilder.append(" IS JSON")
        for t <- expr.nodeType do
            sqlBuilder.append(" ")
            printType(t)
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

    def printLikeExpr(expr: SqlExpr.Like): Unit =
        val precedence = SqlBinaryOperator.Equal.precedence

        def hasBracketsLeft(child: SqlExpr): Boolean =
            child match
                case SqlExpr.Binary(_, op, _)
                    if op.precedence < precedence || op.precedence == 0 => true
                case _ => false

        def hasBracketsRight(child: SqlExpr): Boolean =
            child match
                case SqlExpr.Binary(_, op, _)
                    if op.precedence <= precedence => true
                case SqlExpr.Like(_, _, _, _) => true
                case SqlExpr.SimilarTo(_, _, _, _) => true
                case _ => false

        if hasBracketsLeft(expr.expr) then
            sqlBuilder.append("(")
            printExpr(expr.expr)
            sqlBuilder.append(")")
        else
            printExpr(expr.expr)

        sqlBuilder.append(" ")
        if expr.not then
            sqlBuilder.append("NOT ")
        sqlBuilder.append("LIKE")
        sqlBuilder.append(" ")

        if hasBracketsRight(expr.pattern) then
            sqlBuilder.append("(")
            printExpr(expr.pattern)
            sqlBuilder.append(")")
        else
            printExpr(expr.pattern)

        for e <- expr.escape do
            sqlBuilder.append(" ESCAPE ")
            printExpr(e)

    def printSimilarToExpr(expr: SqlExpr.SimilarTo): Unit =
        val precedence = SqlBinaryOperator.Equal.precedence

        def hasBracketsLeft(child: SqlExpr): Boolean =
            child match
                case SqlExpr.Binary(_, op, _)
                    if op.precedence < precedence || op.precedence == 0 => true
                case _ => false

        def hasBracketsRight(child: SqlExpr): Boolean =
            child match
                case SqlExpr.Binary(_, op, _)
                    if op.precedence <= precedence => true
                case SqlExpr.Like(_, _, _, _) => true
                case SqlExpr.SimilarTo(_, _, _, _) => true
                case _ => false

        if hasBracketsLeft(expr.expr) then
            sqlBuilder.append("(")
            printExpr(expr.expr)
            sqlBuilder.append(")")
        else
            printExpr(expr.expr)

        sqlBuilder.append(" ")
        if expr.not then
            sqlBuilder.append("NOT ")
        sqlBuilder.append("SIMILAR TO")
        sqlBuilder.append(" ")

        if hasBracketsRight(expr.pattern) then
            sqlBuilder.append("(")
            printExpr(expr.pattern)
            sqlBuilder.append(")")
        else
            printExpr(expr.pattern)

        for e <- expr.escape do
            sqlBuilder.append(" ESCAPE ")
            printExpr(e)

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
        printExpr(expr.test)
        sqlBuilder.append(")")

    def printCastExpr(expr: SqlExpr.Cast): Unit =
        sqlBuilder.append("CAST(")
        printExpr(expr.expr)
        sqlBuilder.append(" AS ")
        printType(expr.`type`)
        sqlBuilder.append(")")

    def printType(`type`: SqlType): Unit =
        def printTimeZoneMode(mode: SqlTimeZoneMode): Unit =
            mode match
                case SqlTimeZoneMode.With =>
                    sqlBuilder.append("WITH TIME ZONE")
                case SqlTimeZoneMode.Without =>
                    sqlBuilder.append("WITHOUT TIME ZONE")

        `type` match
            case SqlType.Varchar(maxLength) =>
                sqlBuilder.append("VARCHAR")
                for l <- maxLength do
                    sqlBuilder.append("(")
                    sqlBuilder.append(l)
                    sqlBuilder.append(")")
            case SqlType.Int =>
                sqlBuilder.append("INTEGER")
            case SqlType.Long =>
                sqlBuilder.append("BIGINT")
            case SqlType.Float =>
                sqlBuilder.append("REAL")
            case SqlType.Double =>
                sqlBuilder.append("DOUBLE PRECISION")
            case SqlType.Decimal(precision) =>
                sqlBuilder.append("DECIMAL")
                for (p, s) <- precision do
                    sqlBuilder.append("(")
                    sqlBuilder.append(p)
                    sqlBuilder.append(", ")
                    sqlBuilder.append(s)
                    sqlBuilder.append(")")
            case SqlType.Date =>
                sqlBuilder.append("DATE")
            case SqlType.Timestamp(mode) =>
                sqlBuilder.append("TIMESTAMP")
                for m <- mode do
                    sqlBuilder.append(" ")
                    printTimeZoneMode(m)
            case SqlType.Time(mode) =>
                sqlBuilder.append("TIME")
                for m <- mode do
                    sqlBuilder.append(" ")
                    printTimeZoneMode(m)
            case SqlType.Json =>
                sqlBuilder.append("JSON")
            case SqlType.Boolean =>
                sqlBuilder.append("BOOLEAN")
            case SqlType.Interval =>
                sqlBuilder.append("INTERVAL")
            case SqlType.Vector =>
                sqlBuilder.append("VECTOR")
            case SqlType.Geometry =>
                sqlBuilder.append("GEOMETRY")
            case SqlType.Point =>
                sqlBuilder.append("POINT")
            case SqlType.LineString =>
                sqlBuilder.append("LINESTRING")
            case SqlType.Polygon =>
                sqlBuilder.append("POLYGON")
            case SqlType.MultiPoint =>
                sqlBuilder.append("MULTIPOINT")
            case SqlType.MultiLineString =>
                sqlBuilder.append("MULTILINESTRING")
            case SqlType.MultiPolygon =>
                sqlBuilder.append("MULTIPOLYGON")
            case SqlType.GeometryCollection =>
                sqlBuilder.append("GEOMETRYCOLLECTION")
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
        def printBound(bound: SqlWindowFrameBound): Unit =
            bound match
                case SqlWindowFrameBound.CurrentRow =>
                    sqlBuilder.append("CURRENT ROW")
                case SqlWindowFrameBound.UnboundedFollowing =>
                    sqlBuilder.append("UNBOUNDED FOLLOWING")
                case SqlWindowFrameBound.UnboundedPreceding =>
                    sqlBuilder.append("UNBOUNDED PRECEDING")
                case SqlWindowFrameBound.Following(n) =>
                    printExpr(n)
                    sqlBuilder.append(" FOLLOWING")
                case SqlWindowFrameBound.Preceding(n) =>
                    printExpr(n)
                    sqlBuilder.append(" PRECEDING")

        def printUnit(unit: SqlWindowFrameUnit): Unit =
            unit match
                case SqlWindowFrameUnit.Rows =>
                    sqlBuilder.append("ROWS")
                case SqlWindowFrameUnit.Range =>
                    sqlBuilder.append("RANGE")
                case SqlWindowFrameUnit.Groups =>
                    sqlBuilder.append("GROUPS")

        def printExcludeMode(mode: SqlWindowFrameExcludeMode): Unit =
            sqlBuilder.append("EXCLUDE ")
            mode match
                case SqlWindowFrameExcludeMode.CurrentRow =>
                    sqlBuilder.append("CURRENT ROW")
                case SqlWindowFrameExcludeMode.Group =>
                    sqlBuilder.append("GROUP")
                case SqlWindowFrameExcludeMode.Ties =>
                    sqlBuilder.append("TIES")
                case SqlWindowFrameExcludeMode.NoOthers =>
                    sqlBuilder.append("NO OTHERS")

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
                    printUnit(unit)
                    sqlBuilder.append(" ")
                    printBound(start)
                    for e <- exclude do
                        sqlBuilder.append(" ")
                        printExcludeMode(e)
                case SqlWindowFrame.Between(unit, start, end, exclude) =>
                    sqlBuilder.append(" ")
                    printUnit(unit)
                    sqlBuilder.append(" BETWEEN ")
                    printBound(start)
                    sqlBuilder.append(" AND ")
                    printBound(end)
                    for e <- exclude do
                        sqlBuilder.append(" ")
                        printExcludeMode(e)
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
        def printQuantifier(quantifier: SqlSubLinkQuantifier): Unit =
            quantifier match
                case SqlSubLinkQuantifier.Any =>
                    sqlBuilder.append("ANY")
                case SqlSubLinkQuantifier.All =>
                    sqlBuilder.append("ALL")
                case SqlSubLinkQuantifier.Exists =>
                    sqlBuilder.append("EXISTS")

        push()
        printQuantifier(expr.quantifier)
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
        def printMode(mode: SqlTrimMode): Unit =
            mode match
                case SqlTrimMode.Both =>
                    sqlBuilder.append("BOTH")
                case SqlTrimMode.Leading =>
                    sqlBuilder.append("LEADING")
                case SqlTrimMode.Trailing =>
                    sqlBuilder.append("TRAILING")

        sqlBuilder.append("TRIM(")
        for t <- expr.trim do
            for m <- t.mode do
                printMode(m)
            for e <- t.value do
                if t.mode.nonEmpty then
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
        printTimeUnit(expr.unit)
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
        def printItem(item: SqlJsonObjectItem): Unit =
            printExpr(item.key)
            sqlBuilder.append(" VALUE ")
            printExpr(item.value)
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
        def printItem(item: SqlJsonArrayItem): Unit =
            printExpr(item.value)
            for i <- item.input do
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
        encoding match
            case SqlJsonEncoding.Utf8 =>
                sqlBuilder.append("UTF8")
            case SqlJsonEncoding.Utf16 =>
                sqlBuilder.append("UTF16")
            case SqlJsonEncoding.Utf32 =>
                sqlBuilder.append("UTF32")
            case SqlJsonEncoding.Custom(e) =>
                sqlBuilder.append(e)

    def printJsonInput(input: SqlJsonInput): Unit =
        sqlBuilder.append("FORMAT JSON")
        for e <- input.format do
            printJsonEncoding(e)

    def printJsonOutput(output: SqlJsonOutput): Unit =
        sqlBuilder.append("RETURNING ")
        printType(output.`type`)
        for f <- output.format do
            sqlBuilder.append(" FORMAT JSON")
            for e <- f do
                printJsonEncoding(e)

    def printJsonUniqueness(uniqueness: SqlJsonUniqueness): Unit =
        uniqueness match
            case SqlJsonUniqueness.With =>
                sqlBuilder.append("WITH UNIQUE KEYS")
            case SqlJsonUniqueness.Without =>
                sqlBuilder.append("WITHOUT UNIQUE KEYS")

    def printJsonPassing(passing: SqlJsonPassing): Unit =
        printExpr(passing.expr)
        sqlBuilder.append(" AS ")
        printIdent(passing.alias)

    def printJsonNullConstructor(cons: SqlJsonNullConstructor): Unit =
        cons match
            case SqlJsonNullConstructor.Null =>
                sqlBuilder.append("NULL ON NULL")
            case SqlJsonNullConstructor.Absent =>
                sqlBuilder.append("ABSENT ON NULL")

    def printJsonQueryWrapperBehavior(behavior: SqlJsonQueryWrapperBehavior): Unit =
        def printMode(mode: SqlJsonQueryWrapperBehaviorMode): Unit =
            mode match
                case SqlJsonQueryWrapperBehaviorMode.Conditional =>
                    sqlBuilder.append("CONDITIONAL")
                case SqlJsonQueryWrapperBehaviorMode.Unconditional =>
                    sqlBuilder.append("UNCONDITIONAL")

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
                    printMode(m)
                if a then
                    sqlBuilder.append(" ARRAY")
                sqlBuilder.append(" WRAPPER")

    def printJsonQueryQuotesBehavior(behavior: SqlJsonQueryQuotesBehavior): Unit =
        def printMode(mode: SqlJsonQueryQuotesBehaviorMode): Unit =
            mode match
                case SqlJsonQueryQuotesBehaviorMode.Keep =>
                    sqlBuilder.append("KEEP")
                case SqlJsonQueryQuotesBehaviorMode.Omit =>
                    sqlBuilder.append("OMIT")

        printMode(behavior.mode)
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
        behavior match
            case SqlJsonExistsErrorBehavior.Error =>
                sqlBuilder.append("ERROR")
            case SqlJsonExistsErrorBehavior.True =>
                sqlBuilder.append("TRUE")
            case SqlJsonExistsErrorBehavior.False =>
                sqlBuilder.append("FALSE")
            case SqlJsonExistsErrorBehavior.Unknown =>
                sqlBuilder.append("UNKNOWN")
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
            printIdent(n)
            sqlBuilder.append(".")
        sqlBuilder.append("*)")
        for f <- expr.filter do
            sqlBuilder.append(" FILTER (WHERE ")
            printExpr(f)
            sqlBuilder.append(")")

    def printListAggFuncExpr(expr: SqlExpr.ListAggFunc): Unit =
        def printCountMode(mode: SqlListAggCountMode): Unit =
            mode match
                case SqlListAggCountMode.With =>
                    sqlBuilder.append("WITH COUNT")
                case SqlListAggCountMode.Without =>
                    sqlBuilder.append("WITHOUT COUNT")

        sqlBuilder.append("LISTAGG(")
        expr.quantifier.foreach: q => 
            printQuantifier(q)
            sqlBuilder.append(" ")
        printExpr(expr.expr)
        sqlBuilder.append(", ")
        printExpr(expr.separator)
        for o <- expr.onOverflow do
            sqlBuilder.append(" ON OVERFLOW ")
            o match
                case SqlListAggOnOverflow.Error =>
                    sqlBuilder.append("ERROR")
                case SqlListAggOnOverflow.Truncate(e, c) =>
                    sqlBuilder.append("TRUNCATE ")
                    printExpr(e)
                    sqlBuilder.append(" ")
                    printCountMode(c)
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
        printExpr(expr.item.key)
        sqlBuilder.append(" VALUE ")
        printExpr(expr.item.value)
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
        printExpr(expr.item.value)
        for i <- expr.item.input do
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

    def printWindowNullsMode(mode: SqlWindowNullsMode): Unit =
        mode match
            case SqlWindowNullsMode.Respect =>
                sqlBuilder.append("RESPECT NULLS")
            case SqlWindowNullsMode.Ignore =>
                sqlBuilder.append("IGNORE NULLS")

    def printNullsTreatmentFuncExpr(expr: SqlExpr.NullsTreatmentFunc): Unit =
        sqlBuilder.append(expr.name)
        sqlBuilder.append("(")
        printList(expr.args)(printExpr)
        sqlBuilder.append(")")
        for m <- expr.nullsMode do
            sqlBuilder.append(" ")
            printWindowNullsMode(m)

    def printNthValueFuncExpr(expr: SqlExpr.NthValueFunc): Unit =
        def printFromMode(mode: SqlNthValueFromMode): Unit =
            mode match
                case SqlNthValueFromMode.First =>
                    sqlBuilder.append("FROM FIRST")
                case SqlNthValueFromMode.Last =>
                    sqlBuilder.append("FROM LAST")

        sqlBuilder.append("NTH_VALUE(")
        printList(List(expr.expr, expr.row))(printExpr)
        sqlBuilder.append(")")
        for m <- expr.fromMode do
            sqlBuilder.append(" ")
            printFromMode(m)
        for m <- expr.nullsMode do
            sqlBuilder.append(" ")
            printWindowNullsMode(m)

    def printGeneralFuncExpr(expr: SqlExpr.GeneralFunc): Unit =
        sqlBuilder.append(expr.name)
        sqlBuilder.append("(")
        expr.quantifier.foreach: q => 
            printQuantifier(q)
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
        expr.phase match
            case SqlMatchPhase.Final =>
                sqlBuilder.append("FINAL")
            case SqlMatchPhase.Running =>
                sqlBuilder.append("RUNNING")
        sqlBuilder.append(" ")
        printExpr(expr.expr)

    def printCustomExpr(expr: SqlExpr.Custom): Unit =
        sqlBuilder.append(expr.snippet)

    def printTableAlias(alias: SqlTableAlias): Unit =
        sqlBuilder.append(" AS ")
        printIdent(alias.alias)
        if alias.columnAliases.nonEmpty then
            sqlBuilder.append("(")
            printList(alias.columnAliases)(i => printIdent(i))
            sqlBuilder.append(")")

    def printIdentTable(table: SqlTable.Ident): Unit =
        def printPeriodBetweenMode(mode: SqlTablePeriodBetweenMode): Unit =
            mode match
                case SqlTablePeriodBetweenMode.Asymmetric =>
                    sqlBuilder.append("ASYMMETRIC")
                case SqlTablePeriodBetweenMode.Symmetric =>
                    sqlBuilder.append("SYMMETRIC")

        def printSimpleMode(mode: SqlTableSampleMode): Unit =
            mode match
                case SqlTableSampleMode.Bernoulli =>
                    sqlBuilder.append("BERNOULLI")
                case SqlTableSampleMode.System =>
                    sqlBuilder.append("SYSTEM")
                case SqlTableSampleMode.Custom(m) =>
                    sqlBuilder.append(m)

        printIdent(table.name)
        for p <- table.period do
            p match
                case SqlTablePeriodMode.ForSystemTimeAsOf(expr) =>
                    sqlBuilder.append(" FOR SYSTEM_TIME AS OF ")
                    printExpr(expr)
                case SqlTablePeriodMode.ForSystemTimeBetween(mode, start, end) =>
                    sqlBuilder.append(" FOR SYSTEM_TIME BETWEEN ")
                    for m <- mode do
                        printPeriodBetweenMode(m)
                        sqlBuilder.append(" ")
                    printExpr(start)
                    sqlBuilder.append(" AND ")
                    printExpr(end)
                case SqlTablePeriodMode.ForSystemTimeFrom(from, to) =>
                    sqlBuilder.append(" FOR SYSTEM_TIME FROM ")
                    printExpr(from)
                    sqlBuilder.append(" TO ")
                    printExpr(to)
        for a <- table.alias do
            printTableAlias(a)
        for m <- table.matchRecognize do
            sqlBuilder.append(" ")
            printMatchRecognize(m)
        for s <- table.sample do
            sqlBuilder.append(" TABLESAMPLE ")
            printSimpleMode(s.mode)
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
                    printIdent(name)
                    sqlBuilder.append(" FOR ORDINALITY")
                case c: SqlJsonTableColumn.Column =>
                    printIdent(c.name)
                    sqlBuilder.append(" ")
                    printType(c.`type`)
                    for f <- c.format do
                        sqlBuilder.append(" FORMAT JSON")
                        for e <- f do
                            printJsonEncoding(e)
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
                    printIdent(e.name)
                    sqlBuilder.append(" ")
                    printType(e.`type`)
                    sqlBuilder.append(" EXISTS")
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
                        printIdent(a)
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
            sqlBuilder.append(" AS ")
            printIdent(a)
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

    def printGraphTable(table: SqlTable.Graph): Unit =
        def printPattern(pattern: SqlGraphPattern): Unit =
            for n <- pattern.name do
                printIdent(n)
                sqlBuilder.append(" = ")
            printPatternTerm(pattern.term)

        def printSymbol(symbol: SqlGraphSymbol): Unit =
            symbol match
                case SqlGraphSymbol.Dash =>
                    sqlBuilder.append("-")
                case SqlGraphSymbol.Tilde =>
                    sqlBuilder.append("~")
                case SqlGraphSymbol.LeftArrow =>
                    sqlBuilder.append("<-")
                case SqlGraphSymbol.RightArrow =>
                    sqlBuilder.append("->")
                case SqlGraphSymbol.LeftTildeArrow =>
                    sqlBuilder.append("<~")
                case SqlGraphSymbol.RightTildeArrow =>
                    sqlBuilder.append("~>")

        def printRepeatableMode(mode: SqlGraphRepeatableMode): Unit =
            mode match
                case SqlGraphRepeatableMode.Element =>
                    sqlBuilder.append("ELEMENT")
                case SqlGraphRepeatableMode.ElementBindings =>
                    sqlBuilder.append("ELEMENT BINDINGS")
                case SqlGraphRepeatableMode.Elements =>
                    sqlBuilder.append("ELEMENTS")

        def printDifferentMode(mode: SqlGraphDifferentMode): Unit =
            mode match
                case SqlGraphDifferentMode.Edge =>
                    sqlBuilder.append("EDGE")
                case SqlGraphDifferentMode.EdgeBindings =>
                    sqlBuilder.append("EDGE BINDINGS")
                case SqlGraphDifferentMode.Edges =>
                    sqlBuilder.append("EDGES")

        def printPatternTerm(term: SqlGraphPatternTerm): Unit =
            term match
                case SqlGraphPatternTerm.Quantified(term, quantifier) =>
                    term match
                        case _: SqlGraphPatternTerm.And | SqlGraphPatternTerm.Or | SqlGraphPatternTerm.Alternation =>
                            sqlBuilder.append("(")
                            printPatternTerm(term)
                            sqlBuilder.append(")")
                        case _ =>
                            printPatternTerm(term)
                    printPatternQuantifier(quantifier)
                case SqlGraphPatternTerm.Vertex(name, label, where) =>
                    sqlBuilder.append("(")
                    for n <- name do
                        printIdent(n)
                    for l <- label do
                        if name.isDefined then
                            sqlBuilder.append(" ")
                        sqlBuilder.append("IS ")
                        printPatternLabel(l)
                    for w <- where do
                        if name.isDefined || label.isDefined then
                            sqlBuilder.append(" ")
                        sqlBuilder.append("WHERE ")
                        printExpr(w)
                    sqlBuilder.append(")")
                case SqlGraphPatternTerm.Edge(leftSymbol, name, label, where, rightSymbol) =>
                    printSymbol(leftSymbol)
                    sqlBuilder.append("[")
                    for n <- name do
                        printIdent(n)
                    for l <- label do
                        if name.isDefined then
                            sqlBuilder.append(" ")
                        sqlBuilder.append("IS ")
                        printPatternLabel(l)
                    for w <- where do
                        if name.isDefined || label.isDefined then
                            sqlBuilder.append(" ")
                        sqlBuilder.append("WHERE ")
                        printExpr(w)
                    sqlBuilder.append("]")
                    printSymbol(rightSymbol)
                case SqlGraphPatternTerm.And(left, right) =>
                    left match
                        case _: SqlGraphPatternTerm.Or | SqlGraphPatternTerm.Alternation =>
                            sqlBuilder.append("(")
                            printPatternTerm(left)
                            sqlBuilder.append(")")
                        case _ =>
                            printPatternTerm(left)
                    sqlBuilder.append(" ")
                    right match
                        case _: SqlGraphPatternTerm.Or | SqlGraphPatternTerm.Alternation =>
                            sqlBuilder.append("(")
                            printPatternTerm(right)
                            sqlBuilder.append(")")
                        case _ =>
                            printPatternTerm(right)
                case SqlGraphPatternTerm.Or(left, right) =>
                    left match
                        case _: SqlGraphPatternTerm.Alternation =>
                            sqlBuilder.append("(")
                            printPatternTerm(left)
                            sqlBuilder.append(")")
                        case _ =>
                            printPatternTerm(left)
                    sqlBuilder.append(" | ")
                    right match
                        case _: SqlGraphPatternTerm.Alternation =>
                            sqlBuilder.append("(")
                            printPatternTerm(right)
                            sqlBuilder.append(")")
                        case _ =>
                            printPatternTerm(right)
                case SqlGraphPatternTerm.Alternation(left, right) =>
                    left match
                        case _: SqlGraphPatternTerm.Or =>
                            sqlBuilder.append("(")
                            printPatternTerm(left)
                            sqlBuilder.append(")")
                        case _ =>
                            printPatternTerm(left)
                    sqlBuilder.append(" |+| ")
                    right match
                        case _: SqlGraphPatternTerm.Or =>
                            sqlBuilder.append("(")
                            printPatternTerm(right)
                            sqlBuilder.append(")")
                        case _ =>
                            printPatternTerm(right)
                    
        def printPatternQuantifier(quantifier: SqlGraphQuantifier): Unit =
            quantifier match
                case SqlGraphQuantifier.Asterisk =>
                    sqlBuilder.append("*")
                case SqlGraphQuantifier.Question =>
                    sqlBuilder.append("?")
                case SqlGraphQuantifier.Plus =>
                    sqlBuilder.append("+")
                case SqlGraphQuantifier.Between(start, end) =>
                    sqlBuilder.append("{")
                    for s <- start do
                        printExpr(s)
                    sqlBuilder.append(",")
                    for e <- end do
                        printExpr(e)
                    sqlBuilder.append("}")
                case SqlGraphQuantifier.Quantity(quantity) =>
                    sqlBuilder.append("{")
                    printExpr(quantity)
                    sqlBuilder.append("}")

        def printPatternLabel(label: SqlGraphLabel): Unit =
            label match
                case SqlGraphLabel.Label(name) =>
                    printIdent(name)
                case SqlGraphLabel.Percent =>
                    sqlBuilder.append("%")
                case SqlGraphLabel.Not(label) =>
                    sqlBuilder.append("!(")
                    printPatternLabel(label)
                    sqlBuilder.append(")")
                case SqlGraphLabel.And(left, right) =>
                    left match
                        case _: SqlGraphLabel.Or =>
                            sqlBuilder.append("(")
                            printPatternLabel(left)
                            sqlBuilder.append(")")
                        case _ =>
                            printPatternLabel(left)
                    sqlBuilder.append(" & ")
                    right match
                        case _: SqlGraphLabel.Or =>
                            sqlBuilder.append("(")
                            printPatternLabel(right)
                            sqlBuilder.append(")")
                        case _ =>
                            printPatternLabel(right)
                case SqlGraphLabel.Or(left, right) =>
                    printPatternLabel(left)
                    sqlBuilder.append(" | ")
                    printPatternLabel(right)

        if table.lateral then sqlBuilder.append("LATERAL ")
        sqlBuilder.append("GRAPH_TABLE(\n")
        push()
        printSpace()
        printIdent(table.name)
        sqlBuilder.append("\n")

        printSpace()
        sqlBuilder.append("MATCH")
        for m <- table.`match` do
            sqlBuilder.append(" ")
            m match
                case SqlGraphMatchMode.Repeatable(mode) =>
                    sqlBuilder.append("REPEATABLE ")
                    printRepeatableMode(mode)
                case SqlGraphMatchMode.Different(mode) =>
                    sqlBuilder.append("DIFFERENT ")
                    printDifferentMode(mode)
        sqlBuilder.append("\n")
        printList(table.patterns, ",\n")(printPattern |> printWithSpace)

        for w <- table.where do
            sqlBuilder.append("\n")
            printSpace()
            sqlBuilder.append("WHERE\n")
            w |> printWithSpace(printExpr)

        for r <- table.rows do
            sqlBuilder.append("\n")
            printSpace()
            r match
                case SqlGraphRowsMode.Match =>
                    sqlBuilder.append("ONE ROW PER MATCH")
                case SqlGraphRowsMode.Vertex(name, inPaths) =>
                    sqlBuilder.append("ONE ROW PER VERTEX (")
                    printIdent(name)
                    sqlBuilder.append(")")
                    if inPaths.nonEmpty then
                        sqlBuilder.append(" IN (")
                        printList(inPaths)(printIdent)
                        sqlBuilder.append(")")
                case SqlGraphRowsMode.Step(v1, e, v2, inPaths) =>
                    sqlBuilder.append("ONE ROW PER STEP (")
                    printIdent(v1)
                    sqlBuilder.append(", ")
                    printIdent(e)
                    sqlBuilder.append(", ")
                    printIdent(v2)
                    sqlBuilder.append(")")
                    if inPaths.nonEmpty then
                        sqlBuilder.append(" IN (")
                        printList(inPaths)(printIdent)
                        sqlBuilder.append(")")

        sqlBuilder.append("\n")
        printSpace()
        sqlBuilder.append("COLUMNS(\n")
        printList(table.columns, ",\n")(printSelectItem |> printWithSpace)
        sqlBuilder.append("\n")
        printSpace()
        sqlBuilder.append(")")

        for e <- table.`export` do
            sqlBuilder.append("\n")
            printSpace()
            e match
                case SqlGraphExportMode.AllSingletons(exceptPatterns) =>
                    sqlBuilder.append("EXPORT ALL SINGLETONS EXCEPT (")
                    printList(exceptPatterns)(printIdent)
                    sqlBuilder.append(")")
                case SqlGraphExportMode.Singletons(patterns) =>
                    sqlBuilder.append("EXPORT SINGLETONS (")
                    printList(patterns)(printIdent)
                    sqlBuilder.append(")")
                case SqlGraphExportMode.NoSingletons =>
                    sqlBuilder.append("EXPORT NO SINGLETONS")

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
        def printJoinType(`type`: SqlJoinType): Unit =
            `type` match
                case SqlJoinType.Inner =>
                    sqlBuilder.append("INNER JOIN")
                case SqlJoinType.Left =>
                    sqlBuilder.append("LEFT OUTER JOIN")
                case SqlJoinType.Right =>
                    sqlBuilder.append("RIGHT OUTER JOIN")
                case SqlJoinType.Full =>
                    sqlBuilder.append("FULL OUTER JOIN")
                case SqlJoinType.Cross =>
                    sqlBuilder.append("CROSS JOIN")
                case SqlJoinType.Custom(t) =>
                    sqlBuilder.append(t) 

        printTable(table.left)
        sqlBuilder.append("\n")
        printSpace()
        printJoinType(table.joinType)
        sqlBuilder.append(" ")
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
                        printIdent(n)
                    sqlBuilder.append(")")

    def printTable(table: SqlTable): Unit = 
        table match
            case i: SqlTable.Ident => printIdentTable(i)
            case f: SqlTable.Func => printFuncTable(f)
            case s: SqlTable.SubQuery => printSubQueryTable(s)
            case j: SqlTable.Json => printJsonTable(j)
            case g: SqlTable.Graph => printGraphTable(g)
            case j: SqlTable.Join => printJoinTable(j)

    def printMatchRecognize(matchRecognize: SqlMatchRecognize): Unit =
        def printMeasure(measure: SqlRecognizeMeasureItem): Unit =
            printExpr(measure.expr)
            sqlBuilder.append(" AS ")
            printIdent(measure.alias)

        def printEmptyMatchMode(mode: SqlRecognizePatternEmptyMatchMode): Unit =
            mode match
                case SqlRecognizePatternEmptyMatchMode.ShowEmptyMatches =>
                    sqlBuilder.append("SHOW EMPTY MATCHES")
                case SqlRecognizePatternEmptyMatchMode.OmitEmptyMatches =>
                    sqlBuilder.append("OMIT EMPTY MATCHES")
                case SqlRecognizePatternEmptyMatchMode.WithUnmatchedRows =>
                    sqlBuilder.append("WITH UNMATCHED ROWS")  

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
                case SqlRecognizePatternRowsPerMatchMode.OneRow =>
                    sqlBuilder.append("ONE ROW PER MATCH")
                case SqlRecognizePatternRowsPerMatchMode.AllRows(mode) =>
                    sqlBuilder.append("ALL ROWS PER MATCH")
                    for em <- mode do
                        sqlBuilder.append(" ")
                        printEmptyMatchMode(em)
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
            for n <- table do
                printIdent(n)
                sqlBuilder.append(".")
            sqlBuilder.append("*")
        case SqlSelectItem.Expr(expr, alias) =>
            printExpr(expr)
            for a <- alias do
                sqlBuilder.append(" AS ")
                printIdent(a)

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
        printIdent(item.name)
        sqlBuilder.append(" AS ")
        printWindow(item.window)

    def printOrderingItem(item: SqlOrderingItem): Unit =
        printExpr(item.expr)
        val order = item.ordering match
            case None | Some(SqlOrdering.Asc) => SqlOrdering.Asc
            case _ => SqlOrdering.Desc
        sqlBuilder.append(" ")
        order match
            case SqlOrdering.Asc => 
                sqlBuilder.append("ASC")
            case SqlOrdering.Desc =>
                sqlBuilder.append("DESC")
        for o <- item.nullsOrdering do
            sqlBuilder.append(" NULLS ")
            o match
                case SqlNullsOrdering.First =>
                    sqlBuilder.append("FIRST")
                case SqlNullsOrdering.Last =>
                    sqlBuilder.append("Last")

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
                sqlBuilder.append(" PERCENT")
            sqlBuilder.append(" ROWS ")
            f.mode match
                case SqlFetchMode.Only =>
                    sqlBuilder.append("ONLY")
                case SqlFetchMode.WithTies =>
                    sqlBuilder.append("WITH TIES")

    def printLock(lock: SqlLock): Unit =
        lock match
            case SqlLock.Update(_) =>
                sqlBuilder.append("FOR UPDATE")
            case SqlLock.Share(_) =>
                sqlBuilder.append("FOR SHARE")
        for w <- lock.waitMode do
            sqlBuilder.append(" ")
            w match
                case SqlLockWaitMode.NoWait =>
                    sqlBuilder.append("NOWAIT")
                case SqlLockWaitMode.SkipLocked =>
                    sqlBuilder.append("SKIP LOCKED")

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
                    printIdent(name)
                case SqlRowPatternTerm.Circumflex(_) =>
                    sqlBuilder.append("^")
                case SqlRowPatternTerm.Dollar(_) =>
                    sqlBuilder.append("$")
                case SqlRowPatternTerm.Exclusion(term, _) =>
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
            printIdent(subset.name)
            sqlBuilder.append(" = (")
            printList(subset.patternNames)(i => printIdent(i))
            sqlBuilder.append(")")

        def printDefineItem(define: SqlRowPatternDefineItem): Unit =
            printIdent(define.name)
            sqlBuilder.append(" AS ")
            printExpr(define.expr)

        def printStrategy(strategy: SqlRowPatternStrategy): Unit =
            strategy match
                case SqlRowPatternStrategy.Initial =>
                    sqlBuilder.append("INITIAL")
                case SqlRowPatternStrategy.Seek =>
                    sqlBuilder.append("SEEK")
        
        push()
        for m <- pattern.afterMatch do
            printSpace()
            sqlBuilder.append("AFTER MATCH ")
            m match
                case SqlRowPatternSkipMode.ToNextRow =>
                    sqlBuilder.append("SKIP TO NEXT ROW")
                case SqlRowPatternSkipMode.PastLastRow =>
                    sqlBuilder.append("SKIP PAST LAST ROW")
                case SqlRowPatternSkipMode.ToFirst(name) =>
                    sqlBuilder.append("SKIP TO FIRST ")
                    printIdent(name)
                case SqlRowPatternSkipMode.ToLast(name) =>
                    sqlBuilder.append("SKIP TO LAST ")
                    printIdent(name)
                case SqlRowPatternSkipMode.To(name) =>
                    sqlBuilder.append("SKIP TO ")
                    printIdent(name)
            sqlBuilder.append("\n")
        printSpace()
        for s <- pattern.strategy do
            printStrategy(s)
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

    def printIdent(name: String): Unit =
        val nameBuilder = new StringBuilder
        for c <- name do
            if c == rightQuote then
                nameBuilder.append(rightQuote)
            nameBuilder.append(c)
        sqlBuilder.append(leftQuote)
        sqlBuilder.append(nameBuilder.toString)
        sqlBuilder.append(rightQuote)

    def printChars(chars: String): Unit =
        val stringBuilder = new StringBuilder
        for c <- chars do
            if c == '\'' then
                stringBuilder.append('\'')
            if c == '\\' && !standardEscapeStrings then
                stringBuilder.append('\\')
            stringBuilder.append(c)
        sqlBuilder.append("'")
        sqlBuilder.append(stringBuilder.toString)
        sqlBuilder.append("'")
    
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