package sqala.parser

import sqala.ast.expr.*
import sqala.ast.group.*
import sqala.ast.limit.*
import sqala.ast.order.*
import sqala.ast.quantifier.SqlQuantifier
import sqala.ast.statement.*
import sqala.ast.table.*
import sqala.ast.window.*
import sqala.util.NonEmptyList

import scala.collection.mutable.ListBuffer

/**
 * A hand-written recursive-descent parser for standard SQL expressions.
 */
final class Parser(source: String, tokens: Vector[TokenPosition]):
    private var position = 0

    /**
     * Keywords that the grammar itself treats as syntax. They cannot be used
     * as unquoted identifiers in expression position; users must quote them.
     */
    private val reservedKeywords: Set[String] = Set(
        "ALL", "AND", "ANY", "ARRAY", "AS", "ASC",
        "BETWEEN", "BY",
        "CASE", "CAST", "CURRENT",
        "DATE", "DAY", "DESC", "DISTINCT",
        "ELSE", "END", "ERROR", "EXCEPT", "EXCLUDE", "EXISTS", "EXTRACT",
        "FALSE", "FETCH", "FILTER", "FOLLOWING", "FROM",
        "GROUP", "GROUPING", "GROUPS",
        "HAVING", "HOUR",
        "IN", "INTERSECT", "INTERVAL", "IS",
        "LIKE", "LIMIT", "LISTAGG",
        "MINUTE", "MONTH",
        "NOT", "NULL",
        "OFFSET", "ON", "OR", "ORDER", "OTHERS", "OVER", "OVERFLOW",
        "OVERLAPS", "OVERLAY",
        "PARTITION", "POSITION", "PRECEDING",
        "RANGE", "RECURSIVE", "ROW", "ROWS",
        "SECOND", "SELECT", "SIMILAR", "SOME", "SUBSTRING",
        "THEN", "TIES", "TIME", "TIMESTAMP", "TO", "TRIM", "TRUE",
        "TRUNCATE",
        "UNBOUNDED", "UNION",
        "VALUES",
        "WHEN", "WHERE", "WITH", "WITHIN", "WITHOUT",
        "YEAR", "ZONE",
        "CROSS", "CORRESPONDING", "FOR", "FULL",
        "INNER", "JOIN", "LATERAL", "LEFT", "MATCH_RECOGNIZE",
        "NATURAL", "OUTER",
        "QUALIFY", "RIGHT",
        "SYSTEM_TIME", "TABLESAMPLE", "UPDATE", "USING",
        "WINDOW"
    )

    /**
     * Parses a single expression and requires the input to be fully consumed.
     */
    def parseExpr(): SqlExpr =
        val expr = parseOr()
        if !isEnd then failExpected("end of input")
        expr

    /**
     * Parses a single query and requires the input to be fully consumed.
     */
    def parseQuery(): SqlQuery =
        val query = parseQueryInternal()
        if !isEnd then failExpected("end of input")
        query

    // ---------------------------------------------------------------- cursor

    private def currentPosition: TokenPosition =
        tokens(position)

    private def current: Token =
        currentPosition.token

    private def peekToken(n: Int): Token =
        val index = position + n
        if index < tokens.length then tokens(index).token else tokens.last.token

    private def isEnd: Boolean =
        current match
            case Token.EndOfInput => true
            case _ => false

    private def advance(): Token =
        val token = current
        if !isEnd then position += 1
        token

    private def describe(token: Token): String =
        token match
            case Token.Identifier(name, true) => s""""$name""""
            case Token.Identifier(name, false) =>
                if isReservedKeyword(name) then s"keyword '$name'" else s"'$name'"
            case Token.StringLiteral(value) => s"'$value'"
            case Token.NumberLiteral(value) => s"$value"
            case Token.Symbol(value) => s"'$value'"
            case Token.EndOfInput => "end of input"

    private def failExpected(expected: String): Nothing =
        failDetail(s"Expected $expected, found ${describe(current)}")

    private def failInvalid(what: String, reason: String): Nothing =
        failDetail(s"Invalid $what: $reason")

    private def failUnsupported(what: String): Nothing =
        failDetail(s"$what is not supported")

    private def failDetail(message: String): Nothing =
        val p = currentPosition
        throw ParseException.at(message, source, p.offset, p.line, p.column)

    private def errorAt(message: String, position: TokenPosition): ParseException =
        ParseException.at(message, source, position.offset, position.line, position.column)

    // --------------------------------------------------------------- symbols

    private def isSymbol(symbol: String): Boolean =
        current match
            case Token.Symbol(value) => value == symbol
            case _ => false

    private def isSymbolAt(n: Int, symbol: String): Boolean =
        peekToken(n) match
            case Token.Symbol(value) => value == symbol
            case _ => false

    private def consumeSymbol(symbol: String): Boolean =
        if isSymbol(symbol) then
            advance()
            true
        else
            false

    private def expectSymbol(symbol: String): Unit =
        if !consumeSymbol(symbol) then failExpected(s"'$symbol'")

    // -------------------------------------------------------------- keywords

    private def isKeyword(keyword: String): Boolean =
        current match
            case Token.Identifier(name, false) => name.equalsIgnoreCase(keyword)
            case _ => false

    private def isKeywordAt(n: Int, keyword: String): Boolean =
        peekToken(n) match
            case Token.Identifier(name, false) => name.equalsIgnoreCase(keyword)
            case _ => false

    private def consumeKeyword(keyword: String): Boolean =
        if isKeyword(keyword) then
            advance()
            true
        else
            false

    private def expectKeyword(keyword: String): Unit =
        if !consumeKeyword(keyword) then failExpected(s"keyword $keyword")

    private def expectIdentifier(): String =
        current match
            case Token.Identifier(name, _) =>
                advance()
                name
            case _ => failExpected("an identifier")

    private def expectName(): String =
        current match
            case Token.Identifier(name, true) =>
                advance()
                name
            case Token.Identifier(name, false) =>
                if isReservedKeyword(name) then failExpected("an identifier")
                advance()
                name
            case _ => failExpected("an identifier")

    private def isReservedKeyword(name: String): Boolean =
        reservedKeywords.contains(name.toUpperCase(java.util.Locale.ROOT))

    private def expectStringLiteral(): String =
        current match
            case Token.StringLiteral(value) =>
                advance()
                value
            case _ => failExpected("a string literal")

    private def isStringLiteralAt(n: Int): Boolean =
        peekToken(n) match
            case Token.StringLiteral(_) => true
            case _ => false

    // ------------------------------------------------------------ precedence

    private def parseOr(): SqlExpr =
        var left = parseAnd()
        while consumeKeyword("OR") do
            left = SqlExpr.Binary(left, SqlBinaryOperator.Or, parseAnd())
        left

    private def parseAnd(): SqlExpr =
        var left = parseNot()
        while consumeKeyword("AND") do
            left = SqlExpr.Binary(left, SqlBinaryOperator.And, parseNot())
        left

    private def parseNot(): SqlExpr =
        if consumeKeyword("NOT") then
            SqlExpr.Unary(SqlUnaryOperator.Not, parseNot())
        else
            parseComparison()

    private def parseComparison(): SqlExpr =
        val left = parseConcat()
        if isKeyword("IS") then
            parseIsPredicate(left)
        else
            val withNot =
                if isKeyword("NOT") && isPredicateStart(1) then
                    advance()
                    true
                else
                    false
            if isKeyword("BETWEEN") then
                parseBetween(left, withNot)
            else if isKeyword("IN") then
                parseIn(left, withNot)
            else if isKeyword("LIKE") then
                parseLike(left, withNot)
            else if isKeyword("SIMILAR") && isKeywordAt(1, "TO") then
                parseSimilarTo(left, withNot)
            else if isKeyword("OVERLAPS") then
                parseOverlaps(left)
            else if withNot then
                failExpected("BETWEEN, IN, LIKE, or SIMILAR TO after NOT")
            else
                parseSymbolComparison(left)

    private def isPredicateStart(n: Int): Boolean =
        isKeywordAt(n, "BETWEEN") ||
        isKeywordAt(n, "IN") ||
        isKeywordAt(n, "LIKE") ||
        isKeywordAt(n, "SIMILAR")

    private def parseIsPredicate(left: SqlExpr): SqlExpr =
        expectKeyword("IS")
        val withNot = consumeKeyword("NOT")
        if consumeKeyword("NULL") then
            SqlExpr.Binary(left, SqlBinaryOperator.Is(withNot), SqlExpr.NullLiteral)
        else if consumeKeyword("TRUE") then
            SqlExpr.Binary(left, SqlBinaryOperator.Is(withNot), SqlExpr.BooleanLiteral(true))
        else if consumeKeyword("FALSE") then
            SqlExpr.Binary(left, SqlBinaryOperator.Is(withNot), SqlExpr.BooleanLiteral(false))
        else if consumeKeyword("DISTINCT") then
            expectKeyword("FROM")
            SqlExpr.Binary(left, SqlBinaryOperator.IsDistinctFrom(withNot), parseConcat())
        else if consumeKeyword("JSON") then
            parseJsonTest(left, withNot)
        else
            failExpected("NULL, TRUE, FALSE, DISTINCT FROM, or JSON after IS")

    private def parseJsonTest(left: SqlExpr, withNot: Boolean): SqlExpr =
        val nodeType =
            if consumeKeyword("VALUE") then Some(SqlJsonNodeType.Value)
            else if consumeKeyword("OBJECT") then Some(SqlJsonNodeType.Object)
            else if consumeKeyword("ARRAY") then Some(SqlJsonNodeType.Array)
            else if consumeKeyword("SCALAR") then Some(SqlJsonNodeType.Scalar)
            else None
        val uniquenessMode =
            if consumeKeyword("WITH") then
                expectKeyword("UNIQUE")
                expectKeyword("KEYS")
                Some(SqlJsonUniquenessMode.With)
            else if consumeKeyword("WITHOUT") then
                expectKeyword("UNIQUE")
                expectKeyword("KEYS")
                Some(SqlJsonUniquenessMode.Without)
            else
                None
        SqlExpr.JsonTest(left, nodeType, uniquenessMode, withNot)

    private def parseBetween(left: SqlExpr, withNot: Boolean): SqlExpr =
        expectKeyword("BETWEEN")
        val start = parseConcat()
        expectKeyword("AND")
        val end = parseConcat()
        SqlExpr.Between(left, start, end, withNot)

    private def parseIn(left: SqlExpr, withNot: Boolean): SqlExpr =
        expectKeyword("IN")
        expectSymbol("(")
        val in =
            if isQueryStart() then
                SqlInRightOperand.Subquery(parseSubquery())
            else
                if isSymbol(")") then failInvalid("IN list", "at least one value is required")
                SqlInRightOperand.Values(parseExprList())
        expectSymbol(")")
        SqlExpr.In(left, in, withNot)

    private def parseLike(left: SqlExpr, withNot: Boolean): SqlExpr =
        expectKeyword("LIKE")
        val pattern = parseConcat()
        val escape = if consumeKeyword("ESCAPE") then Some(parseConcat()) else None
        SqlExpr.Like(left, pattern, escape, withNot)

    private def parseSimilarTo(left: SqlExpr, withNot: Boolean): SqlExpr =
        expectKeyword("SIMILAR")
        expectKeyword("TO")
        val pattern = parseConcat()
        val escape = if consumeKeyword("ESCAPE") then Some(parseConcat()) else None
        SqlExpr.SimilarTo(left, pattern, escape, withNot)

    private def parseOverlaps(left: SqlExpr): SqlExpr =
        expectKeyword("OVERLAPS")
        SqlExpr.Binary(left, SqlBinaryOperator.Overlaps, parseConcat())

    private def parseSymbolComparison(left: SqlExpr): SqlExpr =
        if isComparisonSymbol then
            val symbol = currentSymbol
            val operator = binaryOperatorOf(symbol)
            advance()
            if (isKeyword("ANY") || isKeyword("ALL") || isKeyword("SOME")) && isSymbolAt(1, "(") then
                val quantifier =
                    if consumeKeyword("ALL") then SqlSubqueryQuantifier.All
                    else
                        if consumeKeyword("SOME") || consumeKeyword("ANY") then SqlSubqueryQuantifier.Any
                        else failExpected("ANY, ALL, or SOME")
                expectSymbol("(")
                val query = parseSubquery()
                expectSymbol(")")
                SqlExpr.QuantifiedComparisonPredicate(left, quantifiedOperatorOf(operator), quantifier, query)
            else
                SqlExpr.Binary(left, operator, parseConcat())
        else
            left

    private def isComparisonSymbol: Boolean =
        isSymbol("=") || isSymbol("<>") || isSymbol("!=") || isSymbol(">") ||
        isSymbol(">=") || isSymbol("<") || isSymbol("<=")

    private def currentSymbol: String =
        current match
            case Token.Symbol(value) => value
            case _ => failExpected("a comparison operator")

    private def binaryOperatorOf(symbol: String): SqlBinaryOperator =
        symbol match
            case "=" => SqlBinaryOperator.Equal
            case "<>" | "!=" => SqlBinaryOperator.NotEqual
            case ">" => SqlBinaryOperator.GreaterThan
            case ">=" => SqlBinaryOperator.GreaterThanEqual
            case "<" => SqlBinaryOperator.LessThan
            case "<=" => SqlBinaryOperator.LessThanEqual
            case _ => failInvalid("comparison operator", symbol)

    private def quantifiedOperatorOf(operator: SqlBinaryOperator): SqlQuantifiedComparisonOperator =
        operator match
            case SqlBinaryOperator.Equal => SqlQuantifiedComparisonOperator.Equal
            case SqlBinaryOperator.NotEqual => SqlQuantifiedComparisonOperator.NotEqual
            case SqlBinaryOperator.GreaterThan => SqlQuantifiedComparisonOperator.GreaterThan
            case SqlBinaryOperator.GreaterThanEqual => SqlQuantifiedComparisonOperator.GreaterThanEqual
            case SqlBinaryOperator.LessThan => SqlQuantifiedComparisonOperator.LessThan
            case SqlBinaryOperator.LessThanEqual => SqlQuantifiedComparisonOperator.LessThanEqual
            case _ => failInvalid("quantified comparison", "expected a comparison operator")

    private def parseConcat(): SqlExpr =
        var left = parseAdditive()
        while consumeSymbol("||") do
            left = SqlExpr.Binary(left, SqlBinaryOperator.Concat, parseAdditive())
        left

    private def parseAdditive(): SqlExpr =
        var left = parseMultiplicative()
        var continue = true
        while continue do
            if consumeSymbol("+") then
                left = SqlExpr.Binary(left, SqlBinaryOperator.Plus, parseMultiplicative())
            else if consumeSymbol("-") then
                left = SqlExpr.Binary(left, SqlBinaryOperator.Minus, parseMultiplicative())
            else
                continue = false
        left

    private def parseMultiplicative(): SqlExpr =
        var left = parseUnary()
        var continue = true
        while continue do
            if consumeSymbol("*") then
                left = SqlExpr.Binary(left, SqlBinaryOperator.Times, parseUnary())
            else if consumeSymbol("/") then
                left = SqlExpr.Binary(left, SqlBinaryOperator.Div, parseUnary())
            else if consumeSymbol("%") then
                val right = parseUnary()
                left = SqlExpr.GeneralFunc(None, "MOD", List(left, right), Nil, Nil, None)
            else
                continue = false
        left

    private def parseUnary(): SqlExpr =
        if consumeSymbol("+") then
            SqlExpr.Unary(SqlUnaryOperator.Positive, parseUnary())
        else if consumeSymbol("-") then
            SqlExpr.Unary(SqlUnaryOperator.Negative, parseUnary())
        else
            parsePostfix()

    private def parsePostfix(): SqlExpr =
        var expr = parsePrimary()
        while consumeKeyword("OVER") do
            expr = SqlExpr.Window(expr, parseWindow())
        expr

    // ---------------------------------------------------------------- primary

    private def parsePrimary(): SqlExpr =
        current match
            case Token.NumberLiteral(value) =>
                advance()
                SqlExpr.NumberLiteral(value)
            case Token.StringLiteral(value) =>
                advance()
                SqlExpr.StringLiteral(value)
            case Token.Identifier(name, quoted) =>
                parseIdentifierPrimary(name, quoted)
            case Token.Symbol("(") =>
                parseParen()
            case Token.Symbol("?") =>
                failUnsupported("Parameter placeholder")
            case Token.EndOfInput =>
                failExpected("an expression")
            case _ =>
                failExpected("an expression")

    private def parseIdentifierPrimary(name: String, quoted: Boolean): SqlExpr =
        val upper = if quoted then "" else name.toUpperCase(java.util.Locale.ROOT)
        if !quoted && upper == "TRUE" then
            advance()
            SqlExpr.BooleanLiteral(true)
        else if !quoted && upper == "FALSE" then
            advance()
            SqlExpr.BooleanLiteral(false)
        else if !quoted && upper == "NULL" then
            advance()
            SqlExpr.NullLiteral
        else if !quoted && upper == "CASE" then
            parseCase()
        else if !quoted && upper == "CAST" && isSymbolAt(1, "(") then
            parseCast()
        else if !quoted && upper == "EXISTS" && isSymbolAt(1, "(") then
            parseExists()
        else if !quoted && upper == "ARRAY" && isSymbolAt(1, "[") then
            parseArray()
        else if !quoted && (upper == "DATE" || upper == "TIME" || upper == "TIMESTAMP") && isTimeLiteralStart(upper) then
            parseTimeLiteral(upper)
        else if !quoted && upper == "INTERVAL" && isStringLiteralAt(1) then
            parseIntervalLiteral()
        else if !quoted && isNiladic(upper) then
            parseNiladic(upper)
        else if !quoted && isSpecialFunctionKeyword(upper) && isSymbolAt(1, "(") then
            advance()
            parseFunctionCall(name, upper)
        else if !quoted && isReservedKeyword(name) then
            failExpected("an expression")
        else if isSymbolAt(1, "(") then
            advance()
            parseFunctionCall(name, upper)
        else
            advance()
            parseColumnRest(name)

    private def parseNiladic(name: String): SqlExpr =
        advance()
        if isSymbol("(") then
            if isSymbolAt(1, ")") then
                advance()
                advance()
            else
                failInvalid(name, "does not take arguments")
        SqlExpr.IdentFunc(name)

    private def parseColumnRest(name: String): SqlExpr =
        if consumeSymbol(".") then
            val column = expectName()
            if isSymbol(".") then failInvalid("column reference", "too many qualifiers")
            SqlExpr.Column(Some(name), column)
        else
            SqlExpr.Column(None, name)

    private def parseParen(): SqlExpr =
        expectSymbol("(")
        if isQueryStart() then
            val query = parseSubquery()
            expectSymbol(")")
            SqlExpr.Subquery(query)
        else
            val head = parseExprInParens()
            val tail = ListBuffer[SqlExpr]()
            while consumeSymbol(",") do tail += parseExprInParens()
            expectSymbol(")")
            if tail.isEmpty then head else SqlExpr.Tuple(NonEmptyList(head, tail.toList))

    private def parseExprInParens(): SqlExpr =
        // A parenthesized expression is parsed at the same precedence level;
        // commas are handled by the caller.
        parseOr()

    private def parseCase(): SqlExpr =
        advance()
        if consumeKeyword("WHEN") then
            val branches = parseCaseBranchesAfterFirstWhen()
            val default = if consumeKeyword("ELSE") then Some(parseOr()) else None
            expectKeyword("END")
            SqlExpr.Case(branches, default)
        else
            val base = parseOr()
            expectKeyword("WHEN")
            val branches = parseCaseBranchesAfterFirstWhen()
            val default = if consumeKeyword("ELSE") then Some(parseOr()) else None
            expectKeyword("END")
            SqlExpr.SimpleCase(base, branches, default)

    private def parseCaseBranchesAfterFirstWhen(): NonEmptyList[SqlCaseBranch] =
        val head = parseCaseBranch()
        val tail = ListBuffer[SqlCaseBranch]()
        while consumeKeyword("WHEN") do tail += parseCaseBranch()
        NonEmptyList(head, tail.toList)

    private def parseCaseBranch(): SqlCaseBranch =
        val when = parseOr()
        expectKeyword("THEN")
        val thenExpr = parseOr()
        SqlCaseBranch(when, thenExpr)

    private def parseCast(): SqlExpr =
        advance()
        expectSymbol("(")
        val expr = parseOr()
        expectKeyword("AS")
        val target = parseType()
        expectSymbol(")")
        SqlExpr.Cast(expr, target)

    private def parseExists(): SqlExpr =
        advance()
        expectSymbol("(")
        if !isQueryStart() then failExpected("a subquery after EXISTS")
        val query = parseSubquery()
        expectSymbol(")")
        SqlExpr.ExistsPredicate(query)

    private def parseArray(): SqlExpr =
        advance()
        expectSymbol("[")
        val items = ListBuffer[SqlExpr]()
        if !isSymbol("]") then
            items += parseOr()
            while consumeSymbol(",") do items += parseOr()
        expectSymbol("]")
        SqlExpr.Array(items.toList)

    // --------------------------------------------------------------- functions

    private def parseFunctionCall(name: String, upper: String): SqlExpr =
        expectSymbol("(")
        if upper == "COUNT" && isCountAsteriskArg() then
            parseCountAsteriskBody()
        else if upper == "SUBSTRING" then
            parseSubstringBody()
        else if upper == "TRIM" then
            parseTrimBody()
        else if upper == "OVERLAY" then
            parseOverlayBody()
        else if upper == "POSITION" then
            parsePositionBody()
        else if upper == "EXTRACT" then
            parseExtractBody()
        else if upper == "GROUPING" then
            parseGroupingBody()
        else if upper == "LISTAGG" then
            parseListAggBody()
        else if upper == "JSON" then
            parseJsonParseBody()
        else if upper == "JSON_SERIALIZE" then
            parseJsonSerializeBody()
        else if upper == "JSON_QUERY" then
            parseJsonQueryBody()
        else if upper == "JSON_VALUE" then
            parseJsonValueBody()
        else if upper == "JSON_EXISTS" then
            parseJsonExistsBody()
        else if upper == "JSON_OBJECT" then
            parseJsonObjectBody()
        else if upper == "JSON_ARRAY" then
            parseJsonArrayBody()
        else if upper == "JSON_OBJECTAGG" then
            parseJsonObjectAggBody()
        else if upper == "JSON_ARRAYAGG" then
            parseJsonArrayAggBody()
        else
            parseGeneralBody(name)

    private def isCountAsteriskArg(): Boolean =
        isSymbol("*") || (current match
            case Token.Identifier(_, _) => isSymbolAt(1, ".") && isSymbolAt(2, "*")
            case _ => false)

    private def parseCountAsteriskBody(): SqlExpr =
        val tableName =
            if consumeSymbol("*") then
                None
            else
                val table = expectIdentifier()
                expectSymbol(".")
                expectSymbol("*")
                Some(table)
        val filter = parseFilterClause()
        expectSymbol(")")
        SqlExpr.CountAsteriskFunc(tableName, filter)

    private def parseSubstringBody(): SqlExpr =
        val expr = parseOr()
        if consumeKeyword("FROM") then
            val from = parseOr()
            val forExpr = if consumeKeyword("FOR") then Some(parseOr()) else None
            expectSymbol(")")
            SqlExpr.SubstringFunc(expr, from, forExpr)
        else if consumeSymbol(",") then
            val from = parseOr()
            val forExpr = if consumeSymbol(",") then Some(parseOr()) else None
            expectSymbol(")")
            SqlExpr.SubstringFunc(expr, from, forExpr)
        else
            failExpected("FROM or ',' in SUBSTRING")

    private def parseTrimBody(): SqlExpr =
        val mode =
            if consumeKeyword("BOTH") then Some(SqlTrimMode.Both)
            else if consumeKeyword("LEADING") then Some(SqlTrimMode.Leading)
            else if consumeKeyword("TRAILING") then Some(SqlTrimMode.Trailing)
            else None
        val (value, target) =
            if mode.isDefined then
                if consumeKeyword("FROM") then
                    (None, parseOr())
                else
                    val v = parseOr()
                    expectKeyword("FROM")
                    (Some(v), parseOr())
            else if consumeKeyword("FROM") then
                (None, parseOr())
            else
                val first = parseOr()
                if consumeKeyword("FROM") then
                    (Some(first), parseOr())
                else
                    (None, first)
        expectSymbol(")")
        val trim =
            if mode.isDefined || value.isDefined then Some(SqlTrim(mode, value))
            else None
        SqlExpr.TrimFunc(trim, target)

    private def parseOverlayBody(): SqlExpr =
        val target = parseOr()
        expectKeyword("PLACING")
        val placing = parseOr()
        expectKeyword("FROM")
        val from = parseOr()
        val forExpr = if consumeKeyword("FOR") then Some(parseOr()) else None
        expectSymbol(")")
        SqlExpr.OverlayFunc(target, placing, from, forExpr)

    private def parsePositionBody(): SqlExpr =
        val first = parseConcat()
        expectKeyword("IN")
        val second = parseConcat()
        expectSymbol(")")
        SqlExpr.PositionFunc(first, second)

    private def parseExtractBody(): SqlExpr =
        val unit = parseTimeUnit()
        expectKeyword("FROM")
        val expr = parseOr()
        expectSymbol(")")
        SqlExpr.ExtractFunc(unit, expr)

    private def parseGroupingBody(): SqlExpr =
        if isSymbol(")") then failInvalid("GROUPING", "at least one argument is required")
        val items = parseExprList()
        expectSymbol(")")
        SqlExpr.Grouping(items)

    private def parseListAggBody(): SqlExpr =
        val quantifier = parseQuantifier()
        val expr = parseOr()
        expectSymbol(",")
        val separator = parseOr()
        val onOverflow =
            if consumeKeyword("ON") then
                expectKeyword("OVERFLOW")
                if consumeKeyword("ERROR") then
                    Some(SqlListAggOnOverflow.Error)
                else if consumeKeyword("TRUNCATE") then
                    val filler = parseOr()
                    val countMode =
                        if consumeKeyword("WITH") then
                            expectKeyword("COUNT")
                            SqlListAggCountMode.With
                        else if consumeKeyword("WITHOUT") then
                            expectKeyword("COUNT")
                            SqlListAggCountMode.Without
                        else
                            SqlListAggCountMode.Without
                    Some(SqlListAggOnOverflow.Truncate(filler, countMode))
                else
                    failExpected("ERROR or TRUNCATE after ON OVERFLOW")
            else
                None
        expectSymbol(")")
        val withinGroup =
            if consumeKeyword("WITHIN") then
                expectKeyword("GROUP")
                expectSymbol("(")
                expectKeyword("ORDER")
                expectKeyword("BY")
                val items = parseOrderingList()
                expectSymbol(")")
                items
            else
                Nil
        val filter = parseFilterClause()
        SqlExpr.ListAggFunc(quantifier, expr, separator, onOverflow, withinGroup, filter)

    private def parseGeneralBody(name: String): SqlExpr =
        val quantifier = parseQuantifier()
        val args = if isSymbol(")") then Nil else parseExprList().toList
        val orderBy =
            if consumeKeyword("ORDER") then
                expectKeyword("BY")
                parseOrderingList()
            else
                Nil
        expectSymbol(")")
        val withinGroup =
            if consumeKeyword("WITHIN") then
                expectKeyword("GROUP")
                expectSymbol("(")
                expectKeyword("ORDER")
                expectKeyword("BY")
                val items = parseOrderingList()
                expectSymbol(")")
                items
            else
                Nil
        val filter = parseFilterClause()
        SqlExpr.GeneralFunc(quantifier, name, args, orderBy, withinGroup, filter)

    private def parseQuantifier(): Option[SqlQuantifier] =
        if consumeKeyword("DISTINCT") then Some(SqlQuantifier.Distinct)
        else if consumeKeyword("ALL") then Some(SqlQuantifier.All)
        else None

    private def parseFilterClause(): Option[SqlExpr] =
        if consumeKeyword("FILTER") then
            expectSymbol("(")
            expectKeyword("WHERE")
            val condition = parseOr()
            expectSymbol(")")
            Some(condition)
        else
            None

    private def parseOrderingList(): List[SqlOrderingItem] =
        val items = ListBuffer[SqlOrderingItem]()
        items += parseOrderingItem()
        while consumeSymbol(",") do items += parseOrderingItem()
        items.toList

    private def parseOrderingItem(): SqlOrderingItem =
        val expr = parseOr()
        val ordering =
            if consumeKeyword("ASC") then Some(SqlOrdering.Asc)
            else if consumeKeyword("DESC") then Some(SqlOrdering.Desc)
            else None
        val nullsOrdering =
            if consumeKeyword("NULLS") then
                if consumeKeyword("FIRST") then Some(SqlNullsOrdering.First)
                else if consumeKeyword("LAST") then Some(SqlNullsOrdering.Last)
                else failExpected("FIRST or LAST after NULLS")
            else
                None
        SqlOrderingItem(expr, ordering, nullsOrdering)

    private def parseExprList(): NonEmptyList[SqlExpr] =
        val head = parseOr()
        val tail = ListBuffer[SqlExpr]()
        while consumeSymbol(",") do tail += parseOr()
        NonEmptyList(head, tail.toList)

    private def isNiladic(name: String): Boolean =
        name == "CURRENT_DATE" ||
        name == "CURRENT_TIME" ||
        name == "CURRENT_TIMESTAMP" ||
        name == "LOCALTIME" ||
        name == "LOCALTIMESTAMP"

    private def isSpecialFunctionKeyword(name: String): Boolean =
        name == "SUBSTRING" ||
        name == "TRIM" ||
        name == "OVERLAY" ||
        name == "POSITION" ||
        name == "EXTRACT" ||
        name == "GROUPING" ||
        name == "LISTAGG"

    // ------------------------------------------------------------------- json

    private enum JsonBehaviorValue:
        case Error
        case Null
        case EmptyObject
        case EmptyArray
        case Default(expr: SqlExpr)

    private def parseJsonEncoding(): Option[SqlEncoding] =
        if consumeKeyword("ENCODING") then
            if consumeKeyword("UTF8") then Some(SqlEncoding.Utf8)
            else if consumeKeyword("UTF16") then Some(SqlEncoding.Utf16)
            else if consumeKeyword("UTF32") then Some(SqlEncoding.Utf32)
            else failExpected("UTF8, UTF16, or UTF32")
        else
            None

    private def parseJsonInput(): Option[SqlJsonInput] =
        if consumeKeyword("FORMAT") then
            expectKeyword("JSON")
            Some(SqlJsonInput(parseJsonEncoding()))
        else
            None

    private def parseJsonOutputFormat(): Option[SqlJsonOutputFormat] =
        if consumeKeyword("FORMAT") then
            expectKeyword("JSON")
            Some(SqlJsonOutputFormat(parseJsonEncoding()))
        else
            None

    private def parseJsonOutput(): Option[SqlJsonOutput] =
        if consumeKeyword("RETURNING") then
            val targetType = parseType()
            Some(SqlJsonOutput(targetType, parseJsonOutputFormat()))
        else
            None

    private def parseJsonUniquenessMode(): Option[SqlJsonUniquenessMode] =
        if consumeKeyword("WITH") then
            expectKeyword("UNIQUE")
            expectKeyword("KEYS")
            Some(SqlJsonUniquenessMode.With)
        else if consumeKeyword("WITHOUT") then
            expectKeyword("UNIQUE")
            expectKeyword("KEYS")
            Some(SqlJsonUniquenessMode.Without)
        else
            None

    private def parseJsonNullConstructor(): Option[SqlJsonNullConstructor] =
        if consumeKeyword("NULL") then
            expectKeyword("ON")
            expectKeyword("NULL")
            Some(SqlJsonNullConstructor.Null)
        else if consumeKeyword("ABSENT") then
            expectKeyword("ON")
            expectKeyword("NULL")
            Some(SqlJsonNullConstructor.Absent)
        else
            None

    private def parseJsonPassingItems(): List[SqlJsonPassingItem] =
        if consumeKeyword("PASSING") then
            val items = ListBuffer[SqlJsonPassingItem]()
            items += parseJsonPassingItem()
            while consumeSymbol(",") do items += parseJsonPassingItem()
            items.toList
        else
            Nil

    private def parseJsonPassingItem(): SqlJsonPassingItem =
        val expr = parseOr()
        expectKeyword("AS")
        SqlJsonPassingItem(expr, expectName())

    private def parseJsonQueryWrapper(): Option[SqlJsonQueryWrapperBehavior] =
        if consumeKeyword("WITH") then
            val mode =
                if consumeKeyword("CONDITIONAL") then Some(SqlJsonQueryWrapperBehaviorMode.Conditional)
                else if consumeKeyword("UNCONDITIONAL") then Some(SqlJsonQueryWrapperBehaviorMode.Unconditional)
                else None
            val withArray = consumeKeyword("ARRAY")
            expectKeyword("WRAPPER")
            Some(SqlJsonQueryWrapperBehavior.With(mode, withArray))
        else if consumeKeyword("WITHOUT") then
            val withArray = consumeKeyword("ARRAY")
            expectKeyword("WRAPPER")
            Some(SqlJsonQueryWrapperBehavior.Without(withArray))
        else
            None

    private def parseJsonQuotes(): Option[SqlJsonQueryQuotesBehavior] =
        if consumeKeyword("KEEP") then
            expectKeyword("QUOTES")
            val onScalar = parseJsonOnScalarString()
            Some(SqlJsonQueryQuotesBehavior(SqlJsonQueryQuotesBehaviorMode.Keep, onScalar))
        else if consumeKeyword("OMIT") then
            expectKeyword("QUOTES")
            val onScalar = parseJsonOnScalarString()
            Some(SqlJsonQueryQuotesBehavior(SqlJsonQueryQuotesBehaviorMode.Omit, onScalar))
        else
            None

    private def parseJsonOnScalarString(): Boolean =
        if consumeKeyword("ON") then
            expectKeyword("SCALAR")
            expectKeyword("STRING")
            true
        else
            false

    private def parseJsonBehaviorValue(): JsonBehaviorValue =
        if consumeKeyword("ERROR") then
            JsonBehaviorValue.Error
        else if consumeKeyword("NULL") then
            JsonBehaviorValue.Null
        else if consumeKeyword("EMPTY") then
            if consumeKeyword("OBJECT") then JsonBehaviorValue.EmptyObject
            else if consumeKeyword("ARRAY") then JsonBehaviorValue.EmptyArray
            else failExpected("OBJECT or ARRAY after EMPTY")
        else if consumeKeyword("DEFAULT") then
            JsonBehaviorValue.Default(parseOr())
        else
            failExpected("a JSON behavior")

    private def isJsonBehaviorStart(): Boolean =
        isKeyword("ERROR") || isKeyword("NULL") || isKeyword("EMPTY") || isKeyword("DEFAULT")

    private def parseJsonQueryBehaviors(): (Option[SqlJsonQueryEmptyBehavior], Option[SqlJsonQueryErrorBehavior]) =
        var onEmpty: Option[SqlJsonQueryEmptyBehavior] = None
        var onError: Option[SqlJsonQueryErrorBehavior] = None
        var continue = true
        while continue do
            if isJsonBehaviorStart() then
                val value = parseJsonBehaviorValue()
                expectKeyword("ON")
                if consumeKeyword("EMPTY") then
                    onEmpty = Some(jsonQueryEmptyBehavior(value))
                else if consumeKeyword("ERROR") then
                    onError = Some(jsonQueryErrorBehavior(value))
                else
                    failExpected("EMPTY or ERROR after ON")
            else
                continue = false
        (onEmpty, onError)

    private def jsonQueryEmptyBehavior(value: JsonBehaviorValue): SqlJsonQueryEmptyBehavior =
        value match
            case JsonBehaviorValue.Error => SqlJsonQueryEmptyBehavior.Error
            case JsonBehaviorValue.Null => SqlJsonQueryEmptyBehavior.Null
            case JsonBehaviorValue.EmptyObject => SqlJsonQueryEmptyBehavior.EmptyObject
            case JsonBehaviorValue.EmptyArray => SqlJsonQueryEmptyBehavior.EmptyArray
            case JsonBehaviorValue.Default(expr) => SqlJsonQueryEmptyBehavior.Default(expr)

    private def jsonQueryErrorBehavior(value: JsonBehaviorValue): SqlJsonQueryErrorBehavior =
        value match
            case JsonBehaviorValue.Error => SqlJsonQueryErrorBehavior.Error
            case JsonBehaviorValue.Null => SqlJsonQueryErrorBehavior.Null
            case JsonBehaviorValue.EmptyObject => SqlJsonQueryErrorBehavior.EmptyObject
            case JsonBehaviorValue.EmptyArray => SqlJsonQueryErrorBehavior.EmptyArray
            case JsonBehaviorValue.Default(expr) => SqlJsonQueryErrorBehavior.Default(expr)

    private def parseJsonValueBehaviors(): (Option[SqlJsonValueEmptyBehavior], Option[SqlJsonValueErrorBehavior]) =
        var onEmpty: Option[SqlJsonValueEmptyBehavior] = None
        var onError: Option[SqlJsonValueErrorBehavior] = None
        var continue = true
        while continue do
            if isJsonBehaviorStart() then
                val value = parseJsonBehaviorValue()
                expectKeyword("ON")
                if consumeKeyword("EMPTY") then
                    onEmpty = Some(jsonValueEmptyBehavior(value))
                else if consumeKeyword("ERROR") then
                    onError = Some(jsonValueErrorBehavior(value))
                else
                    failExpected("EMPTY or ERROR after ON")
            else
                continue = false
        (onEmpty, onError)

    private def jsonValueEmptyBehavior(value: JsonBehaviorValue): SqlJsonValueEmptyBehavior =
        value match
            case JsonBehaviorValue.Error => SqlJsonValueEmptyBehavior.Error
            case JsonBehaviorValue.Null => SqlJsonValueEmptyBehavior.Null
            case JsonBehaviorValue.Default(expr) => SqlJsonValueEmptyBehavior.Default(expr)
            case JsonBehaviorValue.EmptyObject | JsonBehaviorValue.EmptyArray =>
                failInvalid("JSON_VALUE behavior", "EMPTY OBJECT/ARRAY is not allowed")

    private def jsonValueErrorBehavior(value: JsonBehaviorValue): SqlJsonValueErrorBehavior =
        value match
            case JsonBehaviorValue.Error => SqlJsonValueErrorBehavior.Error
            case JsonBehaviorValue.Null => SqlJsonValueErrorBehavior.Null
            case JsonBehaviorValue.Default(expr) => SqlJsonValueErrorBehavior.Default(expr)
            case JsonBehaviorValue.EmptyObject | JsonBehaviorValue.EmptyArray =>
                failInvalid("JSON_VALUE behavior", "EMPTY OBJECT/ARRAY is not allowed")

    private def parseJsonExistsOnError(): Option[SqlJsonExistsErrorBehavior] =
        if isKeyword("ERROR") || isKeyword("TRUE") || isKeyword("FALSE") || isKeyword("UNKNOWN") then
            val value =
                if consumeKeyword("ERROR") then SqlJsonExistsErrorBehavior.Error
                else if consumeKeyword("TRUE") then SqlJsonExistsErrorBehavior.True
                else if consumeKeyword("FALSE") then SqlJsonExistsErrorBehavior.False
                else
                    consumeKeyword("UNKNOWN")
                    SqlJsonExistsErrorBehavior.Unknown
            expectKeyword("ON")
            expectKeyword("ERROR")
            Some(value)
        else
            None

    private def parseJsonParseBody(): SqlExpr =
        val expr = parseOr()
        val input = parseJsonInput()
        val uniquenessMode = parseJsonUniquenessMode()
        expectSymbol(")")
        SqlExpr.JsonParseFunc(expr, input, uniquenessMode)

    private def parseJsonSerializeBody(): SqlExpr =
        val expr = parseOr()
        val output = parseJsonOutput()
        expectSymbol(")")
        SqlExpr.JsonSerializeFunc(expr, output)

    private def parseJsonQueryBody(): SqlExpr =
        val expr = parseOr()
        expectSymbol(",")
        val path = parseOr()
        val passingItems = parseJsonPassingItems()
        val output = parseJsonOutput()
        val wrapper = parseJsonQueryWrapper()
        val quotes = parseJsonQuotes()
        val (onEmpty, onError) = parseJsonQueryBehaviors()
        expectSymbol(")")
        SqlExpr.JsonQueryFunc(expr, path, passingItems, output, wrapper, quotes, onEmpty, onError)

    private def parseJsonValueBody(): SqlExpr =
        val expr = parseOr()
        expectSymbol(",")
        val path = parseOr()
        val passingItems = parseJsonPassingItems()
        val output = parseJsonOutput()
        val (onEmpty, onError) = parseJsonValueBehaviors()
        expectSymbol(")")
        SqlExpr.JsonValueFunc(expr, path, passingItems, output, onEmpty, onError)

    private def parseJsonExistsBody(): SqlExpr =
        val expr = parseOr()
        expectSymbol(",")
        val path = parseOr()
        val passingItems = parseJsonPassingItems()
        val onError = parseJsonExistsOnError()
        expectSymbol(")")
        SqlExpr.JsonExistsFunc(expr, path, passingItems, onError)

    private def parseJsonObjectBody(): SqlExpr =
        val items = ListBuffer[SqlJsonObjectItem]()
        if !isSymbol(")") && !isJsonObjectClauseStart() then
            items += parseJsonObjectItem()
            while consumeSymbol(",") do items += parseJsonObjectItem()
        val nullConstructor = parseJsonNullConstructor()
        val uniquenessMode = parseJsonUniquenessMode()
        val output = parseJsonOutput()
        expectSymbol(")")
        SqlExpr.JsonObjectFunc(items.toList, nullConstructor, uniquenessMode, output)

    private def parseJsonObjectItem(): SqlJsonObjectItem =
        val key = parseOr()
        expectKeyword("VALUE")
        val value = parseOr()
        SqlJsonObjectItem(key, value)

    private def isJsonObjectClauseStart(): Boolean =
        isKeyword("ABSENT") ||
        (isKeyword("NULL") && isKeywordAt(1, "ON")) ||
        (isKeyword("WITH") && isKeywordAt(1, "UNIQUE")) ||
        (isKeyword("WITHOUT") && isKeywordAt(1, "UNIQUE")) ||
        isKeyword("RETURNING")

    private def parseJsonArrayBody(): SqlExpr =
        val items = ListBuffer[SqlJsonArrayItem]()
        if !isSymbol(")") && !isJsonArrayClauseStart() then
            items += parseJsonArrayItem()
            while consumeSymbol(",") do items += parseJsonArrayItem()
        val nullConstructor = parseJsonNullConstructor()
        val output = parseJsonOutput()
        expectSymbol(")")
        SqlExpr.JsonArrayFunc(items.toList, nullConstructor, output)

    private def parseJsonArrayItem(): SqlJsonArrayItem =
        val value = parseOr()
        val input = parseJsonInput()
        SqlJsonArrayItem(value, input)

    private def isJsonArrayClauseStart(): Boolean =
        isKeyword("ABSENT") ||
        (isKeyword("NULL") && isKeywordAt(1, "ON")) ||
        isKeyword("RETURNING")

    private def parseJsonObjectAggBody(): SqlExpr =
        val item = parseJsonObjectItem()
        val nullConstructor = parseJsonNullConstructor()
        val uniquenessMode = parseJsonUniquenessMode()
        val output = parseJsonOutput()
        expectSymbol(")")
        val filter = parseFilterClause()
        SqlExpr.JsonObjectAggFunc(item, nullConstructor, uniquenessMode, output, filter)

    private def parseJsonArrayAggBody(): SqlExpr =
        val item = parseJsonArrayItem()
        val orderBy =
            if consumeKeyword("ORDER") then
                expectKeyword("BY")
                parseOrderingList()
            else
                Nil
        val nullConstructor = parseJsonNullConstructor()
        val output = parseJsonOutput()
        expectSymbol(")")
        val filter = parseFilterClause()
        SqlExpr.JsonArrayAggFunc(item, orderBy, nullConstructor, output, filter)

    // ----------------------------------------------------------------- window

    private def parseWindow(): SqlWindow =
        if isSymbol("(") then parseInlinedWindow()
        else SqlWindow.Named(expectName())

    private def parseInlinedWindow(): SqlWindow.Inlined =
        expectSymbol("(")
        val existingWindowName =
            current match
                case Token.Identifier(name, quoted) if quoted || !isReservedKeyword(name) =>
                    advance()
                    Some(name)
                case _ =>
                    None
        val partitionBy =
            if consumeKeyword("PARTITION") then
                expectKeyword("BY")
                parseExprList().toList
            else
                Nil
        val orderBy =
            if consumeKeyword("ORDER") then
                expectKeyword("BY")
                parseOrderingList()
            else
                Nil
        val frame =
            if isKeyword("ROWS") || isKeyword("RANGE") || isKeyword("GROUPS") then Some(parseFrame())
            else None
        expectSymbol(")")
        SqlWindow.Inlined(existingWindowName, partitionBy, orderBy, frame)

    private def parseFrame(): SqlWindowFrame =
        val unit =
            if consumeKeyword("ROWS") then SqlWindowFrameUnit.Rows
            else if consumeKeyword("RANGE") then SqlWindowFrameUnit.Range
            else if consumeKeyword("GROUPS") then SqlWindowFrameUnit.Groups
            else failExpected("ROWS, RANGE, or GROUPS")
        if consumeKeyword("BETWEEN") then
            val start = parseFrameBound()
            expectKeyword("AND")
            val end = parseFrameBound()
            val exclude = parseExclude()
            SqlWindowFrame.Between(Nil, unit, start, end, exclude, None)
        else
            val start = parseFrameBound()
            val exclude = parseExclude()
            SqlWindowFrame.Start(Nil, unit, start, exclude, None)

    private def parseFrameBound(): SqlWindowFrameBound =
        if consumeKeyword("CURRENT") then
            expectKeyword("ROW")
            SqlWindowFrameBound.CurrentRow
        else if consumeKeyword("UNBOUNDED") then
            if consumeKeyword("PRECEDING") then SqlWindowFrameBound.UnboundedPreceding
            else if consumeKeyword("FOLLOWING") then SqlWindowFrameBound.UnboundedFollowing
            else failExpected("PRECEDING or FOLLOWING after UNBOUNDED")
        else
            val value = parseAdditive()
            if consumeKeyword("PRECEDING") then SqlWindowFrameBound.Preceding(value)
            else if consumeKeyword("FOLLOWING") then SqlWindowFrameBound.Following(value)
            else failExpected("PRECEDING or FOLLOWING after a frame bound")

    private def parseExclude(): Option[SqlWindowFrameExcludeMode] =
        if consumeKeyword("EXCLUDE") then
            if consumeKeyword("CURRENT") then
                expectKeyword("ROW")
                Some(SqlWindowFrameExcludeMode.CurrentRow)
            else if consumeKeyword("GROUP") then
                Some(SqlWindowFrameExcludeMode.Group)
            else if consumeKeyword("TIES") then
                Some(SqlWindowFrameExcludeMode.Ties)
            else if consumeKeyword("NO") then
                expectKeyword("OTHERS")
                Some(SqlWindowFrameExcludeMode.NoOthers)
            else
                failExpected("CURRENT ROW, GROUP, TIES, or NO OTHERS after EXCLUDE")
        else
            None

    // ------------------------------------------------------------------ types

    private def parseType(): SqlType =
        val name = expectIdentifier()
        val upper = name.toUpperCase(java.util.Locale.ROOT)
        val base =
            upper match
                case "VARCHAR" =>
                    SqlType.Varchar(parseOptionalLength())
                case "CHAR" | "CHARACTER" =>
                    consumeKeyword("VARYING")
                    SqlType.Varchar(parseOptionalLength())
                case "INT" | "INTEGER" =>
                    SqlType.Int
                case "BIGINT" =>
                    SqlType.Long
                case "REAL" =>
                    SqlType.Float
                case "DOUBLE" =>
                    consumeKeyword("PRECISION")
                    SqlType.Double
                case "FLOAT" =>
                    SqlType.Double
                case "DECIMAL" | "NUMERIC" =>
                    if consumeSymbol("(") then
                        val precision = parseInt()
                        val scale = if consumeSymbol(",") then parseInt() else 0
                        expectSymbol(")")
                        SqlType.Decimal(Some((precision, scale)))
                    else
                        SqlType.Decimal(None)
                case "DATE" =>
                    SqlType.Date
                case "TIMESTAMP" =>
                    SqlType.Timestamp(parseTimeZoneMode())
                case "TIME" =>
                    SqlType.Time(parseTimeZoneMode())
                case "JSON" =>
                    SqlType.Json
                case "BOOLEAN" | "BOOL" =>
                    SqlType.Boolean
                case "INTERVAL" =>
                    SqlType.Interval
                case "GEOMETRY" =>
                    SqlType.Geometry
                case "POINT" =>
                    SqlType.Point
                case "LINESTRING" =>
                    SqlType.LineString
                case "POLYGON" =>
                    SqlType.Polygon
                case "MULTIPOINT" =>
                    SqlType.MultiPoint
                case "MULTILINESTRING" =>
                    SqlType.MultiLineString
                case "MULTIPOLYGON" =>
                    SqlType.MultiPolygon
                case "GEOMETRYCOLLECTION" =>
                    SqlType.GeometryCollection
                case _ =>
                    failInvalid("type", name)
        parseArraySuffix(base)

    private def parseOptionalLength(): Option[Int] =
        if consumeSymbol("(") then
            val length = parseInt()
            expectSymbol(")")
            Some(length)
        else
            None

    private def parseInt(): Int =
        current match
            case Token.NumberLiteral(value) =>
                if !value.isWhole then failExpected("an integer")
                advance()
                value.toInt
            case _ =>
                failExpected("an integer")

    private def parseArraySuffix(base: SqlType): SqlType =
        var result = base
        while consumeSymbol("[") do
            expectSymbol("]")
            result = SqlType.Array(result)
        result

    private def parseTimeZoneMode(): Option[SqlTimeZoneMode] =
        if consumeKeyword("WITH") then
            expectKeyword("TIME")
            expectKeyword("ZONE")
            Some(SqlTimeZoneMode.With)
        else if consumeKeyword("WITHOUT") then
            expectKeyword("TIME")
            expectKeyword("ZONE")
            Some(SqlTimeZoneMode.Without)
        else
            None

    private def parseTimeLiteral(kind: String): SqlExpr =
        advance()
        val mode = parseTimeZoneMode()
        val literalPosition = currentPosition
        val value = expectStringLiteral()
        val timeType =
            kind match
                case "DATE" =>
                    if mode.isDefined then failInvalid("DATE literal", "time zone is not allowed")
                    SqlTimeType.Date
                case "TIME" =>
                    SqlTimeType.Time(mode)
                case "TIMESTAMP" =>
                    SqlTimeType.Timestamp(mode)
                case _ =>
                    failInvalid("time literal", kind)
        validateTimeLiteral(timeType, value, literalPosition)
        SqlExpr.TimeLiteral(timeType, value)

    private def validateTimeLiteral(
        timeType: SqlTimeType,
        value: String,
        position: TokenPosition
    ): Unit =
        try
            timeType match
                case SqlTimeType.Date =>
                    LiteralValidator.validateDate(value)
                case SqlTimeType.Time(withTimeZone) =>
                    LiteralValidator.validateTime(value, withTimeZone.isDefined)
                case SqlTimeType.Timestamp(withTimeZone) =>
                    LiteralValidator.validateTimestamp(value, withTimeZone.isDefined)
        catch
            case e: LiteralValidationException =>
                throw errorAt(s"Invalid ${timeTypeName(timeType)} literal: ${e.reason}", position)

    private def timeTypeName(timeType: SqlTimeType): String =
        timeType match
            case SqlTimeType.Date => "DATE"
            case SqlTimeType.Time(None) => "TIME"
            case SqlTimeType.Time(Some(_)) => "TIME WITH TIME ZONE"
            case SqlTimeType.Timestamp(None) => "TIMESTAMP"
            case SqlTimeType.Timestamp(Some(_)) => "TIMESTAMP WITH TIME ZONE"

    private def isTimeLiteralStart(kind: String): Boolean =
        if isStringLiteralAt(1) then
            true
        else
            kind != "DATE" &&
            (isKeywordAt(1, "WITH") || isKeywordAt(1, "WITHOUT")) &&
            isKeywordAt(2, "TIME") &&
            isKeywordAt(3, "ZONE") &&
            isStringLiteralAt(4)

    private def parseIntervalLiteral(): SqlExpr =
        advance()
        val literalPosition = currentPosition
        val value = expectStringLiteral()
        val start = parseTimeUnit()
        val field =
            if consumeKeyword("TO") then SqlIntervalField.To(start, parseTimeUnit())
            else SqlIntervalField.Single(start)
        try LiteralValidator.validateInterval(value, field)
        catch
            case e: LiteralValidationException =>
                throw errorAt(s"Invalid INTERVAL literal: ${e.reason}", literalPosition)
        SqlExpr.IntervalLiteral(value, field)

    private def parseTimeUnit(): SqlTimeUnit =
        val name = expectIdentifier().toUpperCase(java.util.Locale.ROOT)
        name match
            case "YEAR" => SqlTimeUnit.Year
            case "MONTH" => SqlTimeUnit.Month
            case "DAY" => SqlTimeUnit.Day
            case "HOUR" => SqlTimeUnit.Hour
            case "MINUTE" => SqlTimeUnit.Minute
            case "SECOND" => SqlTimeUnit.Second
            case _ => failInvalid("time unit", name)

    // ------------------------------------------------------------------ query

    private def parseQueryInternal(): SqlQuery =
        if isKeyword("WITH") then parseWithQuery()
        else parseQueryTail(parseSetExpression(0))

    private def parseWithQuery(): SqlQuery =
        expectKeyword("WITH")
        val recursive = consumeKeyword("RECURSIVE")
        val items = parseWithItems()
        val query = parseQueryInternal()
        SqlQuery.With(recursive, items, query)

    private def parseWithItems(): NonEmptyList[SqlWithItem] =
        val head = parseWithItem()
        val tail = ListBuffer[SqlWithItem]()
        while consumeSymbol(",") do tail += parseWithItem()
        NonEmptyList(head, tail.toList)

    private def parseWithItem(): SqlWithItem =
        val name = expectName()
        val columnNames =
            if consumeSymbol("(") then
                val names = parseColumnNameList()
                expectSymbol(")")
                names
            else
                Nil
        expectKeyword("AS")
        expectSymbol("(")
        val query = parseQueryInternal()
        expectSymbol(")")
        SqlWithItem(name, columnNames, query)

    private def parseSetExpression(minPrecedence: Int): SqlQuery =
        var left = parseQueryPrimary()
        var continue = true
        while continue do
            setOperatorKeyword() match
                case Some(keyword) if setOperatorPrecedence(keyword) >= minPrecedence =>
                    advance()
                    val quantifier = parseQuantifier()
                    val corresponding = parseSetCorresponding()
                    val right = parseSetExpression(setOperatorPrecedence(keyword) + 1)
                    left = SqlQuery.Set(
                        left,
                        setOperatorOf(keyword, quantifier),
                        corresponding,
                        right,
                        Nil,
                        None
                    )
                case _ =>
                    continue = false
        left

    private def setOperatorKeyword(): Option[String] =
        if isKeyword("UNION") then Some("UNION")
        else if isKeyword("EXCEPT") then Some("EXCEPT")
        else if isKeyword("INTERSECT") then Some("INTERSECT")
        else None

    private def setOperatorPrecedence(keyword: String): Int =
        if keyword == "INTERSECT" then SqlSetOperator.Intersect(None).precedence
        else SqlSetOperator.Union(None).precedence

    private def setOperatorOf(keyword: String, quantifier: Option[SqlQuantifier]): SqlSetOperator =
        keyword match
            case "UNION" => SqlSetOperator.Union(quantifier)
            case "EXCEPT" => SqlSetOperator.Except(quantifier)
            case "INTERSECT" => SqlSetOperator.Intersect(quantifier)
            case _ => failInvalid("set operator", keyword)

    private def parseSetCorresponding(): Option[SqlSetCorresponding] =
        if consumeKeyword("CORRESPONDING") then
            val columnNames =
                if consumeKeyword("BY") then
                    expectSymbol("(")
                    val names = parseColumnNameList()
                    expectSymbol(")")
                    names
                else
                    Nil
            Some(SqlSetCorresponding(columnNames))
        else
            None

    private def parseQueryPrimary(): SqlQuery =
        if isKeyword("SELECT") then
            parseSelectQuery()
        else if isKeyword("VALUES") then
            parseValuesQuery()
        else if isSymbol("(") then
            advance()
            val query = parseQueryInternal()
            expectSymbol(")")
            query
        else
            failExpected("a query")

    private def parseSelectQuery(): SqlQuery =
        expectKeyword("SELECT")
        val quantifier = parseQuantifier()
        val select = parseSelectItems()
        val from = if consumeKeyword("FROM") then parseTableList() else Nil
        val where = if consumeKeyword("WHERE") then Some(parseOr()) else None
        val groupBy =
            if consumeKeyword("GROUP") then
                expectKeyword("BY")
                Some(parseGroupClause())
            else
                None
        val having = if consumeKeyword("HAVING") then Some(parseOr()) else None
        val window = if consumeKeyword("WINDOW") then parseWindowItems() else Nil
        val qualify = if consumeKeyword("QUALIFY") then Some(parseOr()) else None
        SqlQuery.Select(
            quantifier,
            select,
            from,
            where,
            groupBy,
            having,
            window,
            qualify,
            Nil,
            None,
            None
        )

    private def parseSelectItems(): NonEmptyList[SqlSelectItem] =
        val head = parseSelectItem()
        val tail = ListBuffer[SqlSelectItem]()
        while consumeSymbol(",") do tail += parseSelectItem()
        NonEmptyList(head, tail.toList)

    private def parseSelectItem(): SqlSelectItem =
        if consumeSymbol("*") then
            SqlSelectItem.Asterisk(None)
        else if isQualifiedAsterisk() then
            val tableName = expectName()
            expectSymbol(".")
            expectSymbol("*")
            SqlSelectItem.Asterisk(Some(tableName))
        else
            val expr = parseOr()
            val alias =
                if consumeKeyword("AS") then Some(expectName())
                else
                    current match
                        case Token.Identifier(name, quoted) if quoted || !isReservedKeyword(name) =>
                            Some(expectName())
                        case _ =>
                            None
            SqlSelectItem.Expr(expr, alias)

    private def isQualifiedAsterisk(): Boolean =
        current match
            case Token.Identifier(_, _) => isSymbolAt(1, ".") && isSymbolAt(2, "*")
            case _ => false

    private def parseGroupClause(): SqlGroup =
        val quantifier = parseQuantifier()
        val items = parseGroupingItems()
        SqlGroup(quantifier, items)

    private def parseGroupingItems(): NonEmptyList[SqlGroupingItem] =
        val head = parseGroupingItem()
        val tail = ListBuffer[SqlGroupingItem]()
        while consumeSymbol(",") do tail += parseGroupingItem()
        NonEmptyList(head, tail.toList)

    private def parseGroupingItem(): SqlGroupingItem =
        if isKeyword("GROUPING") && isKeywordAt(1, "SETS") then
            advance()
            advance()
            expectSymbol("(")
            val items = parseGroupingItems()
            expectSymbol(")")
            SqlGroupingItem.GroupingSets(items)
        else if isKeyword("CUBE") then
            advance()
            expectSymbol("(")
            val items = parseExprList()
            expectSymbol(")")
            SqlGroupingItem.Cube(items)
        else if isKeyword("ROLLUP") then
            advance()
            expectSymbol("(")
            val items = parseExprList()
            expectSymbol(")")
            SqlGroupingItem.Rollup(items)
        else if isSymbol("(") && isSymbolAt(1, ")") then
            advance()
            advance()
            SqlGroupingItem.EmptyGroup
        else
            SqlGroupingItem.Expr(parseOr())

    private def parseWindowItems(): List[SqlWindowItem] =
        val items = ListBuffer[SqlWindowItem]()
        items += parseWindowItem()
        while consumeSymbol(",") do items += parseWindowItem()
        items.toList

    private def parseWindowItem(): SqlWindowItem =
        val name = expectName()
        expectKeyword("AS")
        val window = parseInlinedWindow()
        SqlWindowItem(name, window)

    private def parseQueryTail(query: SqlQuery): SqlQuery =
        val orderBy =
            if consumeKeyword("ORDER") then
                expectKeyword("BY")
                parseOrderingList()
            else
                Nil
        val limit = parseLimitClause()
        val lock = parseLockClause()
        query match
            case select: SqlQuery.Select =>
                select.copy(
                    orderBy = if orderBy.nonEmpty then orderBy else select.orderBy,
                    limit = limit.orElse(select.limit),
                    lock = lock.orElse(select.lock)
                )
            case set: SqlQuery.Set =>
                if lock.isDefined then
                    failInvalid("lock clause", "only allowed on a SELECT query")
                set.copy(
                    orderBy = if orderBy.nonEmpty then orderBy else set.orderBy,
                    limit = limit.orElse(set.limit)
                )
            case values: SqlQuery.Values =>
                if lock.isDefined then
                    failInvalid("lock clause", "only allowed on a SELECT query")
                values.copy(
                    orderBy = if orderBy.nonEmpty then orderBy else values.orderBy,
                    limit = limit.orElse(values.limit)
                )
            case _ =>
                if orderBy.nonEmpty || limit.isDefined || lock.isDefined then
                    failInvalid("clause", "cannot apply ORDER BY, LIMIT, or locking to this query")
                query

    private def parseLimitClause(): Option[SqlLimit] =
        var offset: Option[SqlExpr] = None
        var fetch: Option[SqlFetch] = None
        var continue = true
        while continue do
            if consumeKeyword("LIMIT") then
                if consumeKeyword("ALL") then
                    fetch = None
                else
                    val first = parseOr()
                    if consumeSymbol(",") then
                        offset = Some(first)
                        fetch = Some(SqlFetch(parseOr(), SqlFetchUnit.RowCount, SqlFetchMode.Only))
                    else
                        fetch = Some(SqlFetch(first, SqlFetchUnit.RowCount, SqlFetchMode.Only))
            else if consumeKeyword("OFFSET") then
                offset = Some(parseOr())
                if !consumeKeyword("ROW") then consumeKeyword("ROWS")
            else if consumeKeyword("FETCH") then
                if !consumeKeyword("FIRST") then expectKeyword("NEXT")
                val limit =
                    if isKeyword("ROW") || isKeyword("ROWS") || isKeyword("PERCENT") then
                        SqlExpr.NumberLiteral(1)
                    else
                        parseOr()
                val unit =
                    if consumeKeyword("PERCENT") then SqlFetchUnit.Percentage
                    else SqlFetchUnit.RowCount
                if !consumeKeyword("ROW") then consumeKeyword("ROWS")
                val mode =
                    if consumeKeyword("ONLY") then SqlFetchMode.Only
                    else if consumeKeyword("WITH") then
                        expectKeyword("TIES")
                        SqlFetchMode.WithTies
                    else
                        SqlFetchMode.Only
                fetch = Some(SqlFetch(limit, unit, mode))
            else
                continue = false
        if offset.isDefined || fetch.isDefined then Some(SqlLimit(offset, fetch)) else None

    private def parseLockClause(): Option[SqlLock] =
        if consumeKeyword("FOR") then
            val isUpdate =
                if consumeKeyword("UPDATE") then true
                else if consumeKeyword("SHARE") then false
                else failExpected("UPDATE or SHARE after FOR")
            val waitMode =
                if consumeKeyword("NOWAIT") then Some(SqlLockWaitMode.NoWait)
                else if consumeKeyword("SKIP") then
                    expectKeyword("LOCKED")
                    Some(SqlLockWaitMode.SkipLocked)
                else if consumeKeyword("WAIT") then
                    Some(SqlLockWaitMode.Wait(parseOr()))
                else
                    None
            if isUpdate then Some(SqlLock.Update(waitMode))
            else Some(SqlLock.Share(waitMode))
        else
            None

    private def parseValuesQuery(): SqlQuery =
        expectKeyword("VALUES")
        val head = parseValuesRow()
        val tail = ListBuffer[NonEmptyList[SqlExpr]]()
        while consumeSymbol(",") do tail += parseValuesRow()
        SqlQuery.Values(NonEmptyList(head, tail.toList), Nil, None)

    private def parseValuesRow(): NonEmptyList[SqlExpr] =
        expectSymbol("(")
        val values = parseExprList()
        expectSymbol(")")
        values

    private def parseTableList(): List[SqlTable] =
        val tables = ListBuffer[SqlTable]()
        tables += parseTable()
        while consumeSymbol(",") do tables += parseTable()
        tables.toList

    private def parseTable(): SqlTable =
        var table = parseTablePrimary()
        var continue = true
        while continue do
            if isKeyword("JOIN") then
                advance()
                table = parseJoinedTable(table, SqlJoinType.Inner)
            else if isKeyword("INNER") then
                advance()
                expectKeyword("JOIN")
                table = parseJoinedTable(table, SqlJoinType.Inner)
            else if isKeyword("LEFT") then
                advance()
                consumeKeyword("OUTER")
                expectKeyword("JOIN")
                table = parseJoinedTable(table, SqlJoinType.Left)
            else if isKeyword("RIGHT") then
                advance()
                consumeKeyword("OUTER")
                expectKeyword("JOIN")
                table = parseJoinedTable(table, SqlJoinType.Right)
            else if isKeyword("FULL") then
                advance()
                consumeKeyword("OUTER")
                expectKeyword("JOIN")
                table = parseJoinedTable(table, SqlJoinType.Full)
            else if isKeyword("CROSS") then
                advance()
                expectKeyword("JOIN")
                table = SqlTable.Join(table, SqlJoinType.Cross, parseTablePrimary(), None)
            else if isKeyword("NATURAL") then
                failUnsupported("NATURAL JOIN")
            else
                continue = false
        table

    private def parseJoinedTable(left: SqlTable, joinType: SqlJoinType): SqlTable =
        val right = parseTablePrimary()
        val condition =
            if consumeKeyword("ON") then
                Some(SqlJoinCondition.On(parseOr()))
            else if consumeKeyword("USING") then
                expectSymbol("(")
                val names = parseNameList()
                expectSymbol(")")
                Some(SqlJoinCondition.Using(names))
            else
                failExpected("ON or USING")
        SqlTable.Join(left, joinType, right, condition)

    private def parseTablePrimary(): SqlTable =
        val lateral = consumeKeyword("LATERAL")
        if isSymbol("(") then
            advance()
            if isQueryStart() then
                val query = parseQueryInternal()
                expectSymbol(")")
                val alias = parseOptionalTableAlias()
                val matchRecognize = parseOptionalMatchRecognize()
                SqlTable.Subquery(lateral, query, alias, matchRecognize)
            else
                val table = parseTable()
                expectSymbol(")")
                table
        else
            current match
                case Token.Identifier(name, quoted) =>
                    val upper = if quoted then "" else name.toUpperCase(java.util.Locale.ROOT)
                    if !quoted && upper == "JSON_TABLE" && isSymbolAt(1, "(") then
                        parseJsonTable(lateral)
                    else if !quoted && upper == "GRAPH_TABLE" && isSymbolAt(1, "(") then
                        parseGraphTable(lateral)
                    else if isSymbolAt(1, "(") then
                        if !quoted && isReservedKeyword(name) then
                            failExpected("a table reference")
                        advance()
                        expectSymbol("(")
                        val args = if isSymbol(")") then Nil else parseExprList().toList
                        expectSymbol(")")
                        val withOrdinality =
                            if consumeKeyword("WITH") then
                                expectKeyword("ORDINALITY")
                                true
                            else
                                false
                        val alias = parseOptionalTableAlias()
                        val matchRecognize = parseOptionalMatchRecognize()
                        SqlTable.Func(lateral, name, args, withOrdinality, alias, matchRecognize)
                    else
                        if !quoted && isReservedKeyword(name) then
                            failExpected("a table reference")
                        advance()
                        val period = parseOptionalTablePeriod()
                        val alias = parseOptionalTableAlias()
                        val matchRecognize = parseOptionalMatchRecognize()
                        val sample = parseOptionalTableSample()
                        SqlTable.Ident(name, alias, period, matchRecognize, sample)
                case _ =>
                    failExpected("a table reference")

    private def parseOptionalTablePeriod(): Option[SqlTablePeriodForMode] =
        if isKeyword("FOR") && isKeywordAt(1, "SYSTEM_TIME") then
            advance()
            advance()
            Some(parseTablePeriodMode())
        else
            None

    private def parseTablePeriodMode(): SqlTablePeriodForMode =
        if consumeKeyword("AS") then
            expectKeyword("OF")
            SqlTablePeriodForMode.SystemTimeAsOf(parseOr())
        else if consumeKeyword("BETWEEN") then
            val mode =
                if consumeKeyword("ASYMMETRIC") then Some(SqlTablePeriodBetweenMode.Asymmetric)
                else if consumeKeyword("SYMMETRIC") then Some(SqlTablePeriodBetweenMode.Symmetric)
                else None
            val start = parseOr()
            expectKeyword("AND")
            val end = parseOr()
            SqlTablePeriodForMode.SystemTimeBetween(mode, start, end)
        else if consumeKeyword("FROM") then
            val from = parseOr()
            expectKeyword("TO")
            val to = parseOr()
            SqlTablePeriodForMode.SystemTimeFrom(from, to)
        else
            failExpected("AS OF, BETWEEN, or FROM after FOR SYSTEM_TIME")

    private def parseOptionalTableSample(): Option[SqlTableSample] =
        if consumeKeyword("TABLESAMPLE") then
            val mode =
                if consumeKeyword("BERNOULLI") then SqlTableSampleMode.Bernoulli
                else if consumeKeyword("SYSTEM") then SqlTableSampleMode.System
                else failExpected("BERNOULLI or SYSTEM")
            expectSymbol("(")
            val percentage = parseOr()
            expectSymbol(")")
            val repeatable =
                if consumeKeyword("REPEATABLE") then
                    expectSymbol("(")
                    val value = parseOr()
                    expectSymbol(")")
                    Some(value)
                else
                    None
            Some(SqlTableSample(mode, percentage, repeatable))
        else
            None

    private def parseOptionalMatchRecognize(): Option[SqlMatchRecognize] =
        if consumeKeyword("MATCH_RECOGNIZE") then Some(parseMatchRecognizeBody())
        else None

    private def parseMatchRecognizeBody(): SqlMatchRecognize =
        expectSymbol("(")
        val partitionBy =
            if consumeKeyword("PARTITION") then
                expectKeyword("BY")
                parseExprList().toList
            else
                Nil
        val orderBy =
            if consumeKeyword("ORDER") then
                expectKeyword("BY")
                parseOrderingList()
            else
                Nil
        val measures = parseMatchMeasures()
        val rowsMode = parseMatchRowsMode()
        val afterMatchMode = parseMatchAfterMatch()
        val strategy =
            if consumeKeyword("INITIAL") then Some(SqlRowPatternStrategy.Initial)
            else if consumeKeyword("SEEK") then Some(SqlRowPatternStrategy.Seek)
            else None
        expectKeyword("PATTERN")
        expectSymbol("(")
        val pattern = parseRowPattern()
        expectSymbol(")")
        val subset =
            if consumeKeyword("SUBSET") then parseRowPatternSubsets()
            else Nil
        expectKeyword("DEFINE")
        val define = parseRowPatternDefines()
        expectSymbol(")")
        val alias = parseOptionalTableAlias()
        SqlMatchRecognize(
            partitionBy,
            orderBy,
            measures,
            rowsMode,
            SqlRowPattern(afterMatchMode, strategy, pattern, subset, define),
            alias
        )

    private def parseMatchMeasures(): NonEmptyList[SqlRowPatternMeasureItem] =
        expectKeyword("MEASURES")
        val head = parseMatchMeasure()
        val tail = ListBuffer[SqlRowPatternMeasureItem]()
        while consumeSymbol(",") do tail += parseMatchMeasure()
        NonEmptyList(head, tail.toList)

    private def parseMatchMeasure(): SqlRowPatternMeasureItem =
        val expr = parseRecognizeExpr()
        expectKeyword("AS")
        SqlRowPatternMeasureItem(expr, expectName())

    private def parseRecognizeExpr(): SqlExpr =
        if consumeKeyword("FINAL") then
            SqlExpr.MatchPhase(SqlMatchPhase.Final, parseOr())
        else if consumeKeyword("RUNNING") then
            SqlExpr.MatchPhase(SqlMatchPhase.Running, parseOr())
        else
            parseOr()

    private def parseMatchRowsMode(): Option[SqlRecognizePatternRowsMode] =
        if consumeKeyword("ONE") then
            expectKeyword("ROW")
            expectKeyword("PER")
            expectKeyword("MATCH")
            Some(SqlRecognizePatternRowsMode.OneRow)
        else if consumeKeyword("ALL") then
            expectKeyword("ROWS")
            expectKeyword("PER")
            expectKeyword("MATCH")
            val emptyMode =
                if consumeKeyword("SHOW") then
                    expectKeyword("EMPTY")
                    expectKeyword("MATCHES")
                    Some(SqlRecognizePatternEmptyMatchMode.ShowEmptyMatches)
                else if consumeKeyword("OMIT") then
                    expectKeyword("EMPTY")
                    expectKeyword("MATCHES")
                    Some(SqlRecognizePatternEmptyMatchMode.OmitEmptyMatches)
                else if consumeKeyword("WITH") then
                    expectKeyword("UNMATCHED")
                    expectKeyword("ROWS")
                    Some(SqlRecognizePatternEmptyMatchMode.WithUnmatchedRows)
                else
                    None
            Some(SqlRecognizePatternRowsMode.AllRows(emptyMode))
        else
            None

    private def parseMatchAfterMatch(): Option[SqlRowPatternSkipMode] =
        if consumeKeyword("AFTER") then
            expectKeyword("MATCH")
            expectKeyword("SKIP")
            if consumeKeyword("TO") then
                if consumeKeyword("NEXT") then
                    expectKeyword("ROW")
                    Some(SqlRowPatternSkipMode.ToNextRow)
                else if consumeKeyword("FIRST") then
                    Some(SqlRowPatternSkipMode.ToFirst(expectName()))
                else if consumeKeyword("LAST") then
                    Some(SqlRowPatternSkipMode.ToLast(expectName()))
                else
                    Some(SqlRowPatternSkipMode.To(expectName()))
            else if consumeKeyword("PAST") then
                expectKeyword("LAST")
                expectKeyword("ROW")
                Some(SqlRowPatternSkipMode.PastLastRow)
            else
                failExpected("TO or PAST after AFTER MATCH SKIP")
        else
            None

    private def parseRowPatternSubsets(): List[SqlRowPatternSubsetItem] =
        val items = ListBuffer[SqlRowPatternSubsetItem]()
        items += parseRowPatternSubset()
        while consumeSymbol(",") do items += parseRowPatternSubset()
        items.toList

    private def parseRowPatternSubset(): SqlRowPatternSubsetItem =
        val name = expectName()
        expectSymbol("=")
        expectSymbol("(")
        val names = parseNameList()
        expectSymbol(")")
        SqlRowPatternSubsetItem(name, names)

    private def parseRowPatternDefines(): NonEmptyList[SqlRowPatternDefineItem] =
        val head = parseRowPatternDefine()
        val tail = ListBuffer[SqlRowPatternDefineItem]()
        while consumeSymbol(",") do tail += parseRowPatternDefine()
        NonEmptyList(head, tail.toList)

    private def parseRowPatternDefine(): SqlRowPatternDefineItem =
        val name = expectName()
        expectKeyword("AS")
        SqlRowPatternDefineItem(name, parseRecognizeExpr())

    private def parseRowPattern(): SqlRowPatternTerm =
        parseRowPatternOr()

    private def parseRowPatternOr(): SqlRowPatternTerm =
        var left = parseRowPatternThen()
        while consumeSymbol("|") do
            left = SqlRowPatternTerm.Or(left, parseRowPatternThen(), None)
        left

    private def parseRowPatternThen(): SqlRowPatternTerm =
        var left = parseRowPatternTerm()
        while isRowPatternTermStart() do
            left = SqlRowPatternTerm.Then(left, parseRowPatternTerm(), None)
        left

    private def parseRowPatternTerm(): SqlRowPatternTerm =
        val term = parseRowPatternPrimary()
        parseRowPatternQuantifier() match
            case Some(quantifier) => attachRowPatternQuantifier(term, quantifier)
            case None => term

    private def isRowPatternTermStart(): Boolean =
        isSymbol("(") ||
        isSymbol("^") ||
        isSymbol("$") ||
        (isSymbol("{") && isSymbolAt(1, "-")) ||
        (current match
            case Token.Identifier(name, quoted) => quoted || !isReservedKeyword(name)
            case _ => false)

    private def parseRowPatternPrimary(): SqlRowPatternTerm =
        if consumeSymbol("(") then
            val term = parseRowPatternOr()
            expectSymbol(")")
            term
        else if consumeSymbol("^") then
            SqlRowPatternTerm.Circumflex(None)
        else if consumeSymbol("$") then
            SqlRowPatternTerm.Dollar(None)
        else if isSymbol("{") && isSymbolAt(1, "-") then
            advance()
            advance()
            val term = parseRowPatternOr()
            expectSymbol("-")
            expectSymbol("}")
            SqlRowPatternTerm.Exclusion(term, None)
        else if consumeKeyword("PERMUTE") then
            expectSymbol("(")
            val head = parseRowPatternTerm()
            val tail = ListBuffer[SqlRowPatternTerm]()
            while consumeSymbol(",") do tail += parseRowPatternTerm()
            expectSymbol(")")
            SqlRowPatternTerm.Permute(NonEmptyList(head, tail.toList), None)
        else
            SqlRowPatternTerm.Pattern(expectName(), None)

    private def attachRowPatternQuantifier(
        term: SqlRowPatternTerm,
        quantifier: SqlRowPatternQuantifier
    ): SqlRowPatternTerm =
        term match
            case SqlRowPatternTerm.Pattern(name, _) =>
                SqlRowPatternTerm.Pattern(name, Some(quantifier))
            case SqlRowPatternTerm.Circumflex(_) =>
                SqlRowPatternTerm.Circumflex(Some(quantifier))
            case SqlRowPatternTerm.Dollar(_) =>
                SqlRowPatternTerm.Dollar(Some(quantifier))
            case SqlRowPatternTerm.Exclusion(inner, _) =>
                SqlRowPatternTerm.Exclusion(inner, Some(quantifier))
            case SqlRowPatternTerm.Permute(terms, _) =>
                SqlRowPatternTerm.Permute(terms, Some(quantifier))
            case SqlRowPatternTerm.Then(left, right, _) =>
                SqlRowPatternTerm.Then(left, right, Some(quantifier))
            case SqlRowPatternTerm.Or(left, right, _) =>
                SqlRowPatternTerm.Or(left, right, Some(quantifier))

    private def parseRowPatternQuantifier(): Option[SqlRowPatternQuantifier] =
        if consumeSymbol("*") then
            Some(SqlRowPatternQuantifier.Asterisk(consumeSymbol("?")))
        else if consumeSymbol("+") then
            Some(SqlRowPatternQuantifier.Plus(consumeSymbol("?")))
        else if consumeSymbol("?") then
            Some(SqlRowPatternQuantifier.Question(consumeSymbol("?")))
        else if isSymbol("{") then
            advance()
            val start = if isSymbol(",") || isSymbol("}") then None else Some(parseOr())
            if consumeSymbol(",") then
                val end = if isSymbol("}") then None else Some(parseOr())
                expectSymbol("}")
                Some(SqlRowPatternQuantifier.Between(start, end, consumeSymbol("?")))
            else
                expectSymbol("}")
                start match
                    case Some(expr) => Some(SqlRowPatternQuantifier.Quantity(expr))
                    case None => failInvalid("row pattern quantifier", "expected a quantity")
        else
            None

    private def parseJsonTable(lateral: Boolean): SqlTable =
        advance()
        expectSymbol("(")
        val expr = parseOr()
        expectSymbol(",")
        val path = parseOr()
        val pathAlias =
            if consumeKeyword("AS") then Some(expectName())
            else None
        val passingItems = parseJsonPassingItems()
        expectKeyword("COLUMNS")
        expectSymbol("(")
        val columns = parseJsonColumns()
        expectSymbol(")")
        val onError = parseJsonTableOnError()
        expectSymbol(")")
        val alias = parseOptionalTableAlias()
        val matchRecognize = parseOptionalMatchRecognize()
        SqlTable.Json(lateral, expr, path, pathAlias, passingItems, columns, onError, alias, matchRecognize)

    private def parseJsonColumns(): NonEmptyList[SqlJsonColumn] =
        val head = parseJsonColumn()
        val tail = ListBuffer[SqlJsonColumn]()
        while consumeSymbol(",") do tail += parseJsonColumn()
        NonEmptyList(head, tail.toList)

    private def parseJsonColumn(): SqlJsonColumn =
        if isKeyword("NESTED") && isKeywordAt(1, "PATH") then
            advance()
            advance()
            val path = parseOr()
            val pathAlias =
                if consumeKeyword("AS") then
                    Some(expectName())
                else
                    current match
                        case Token.Identifier(name, quoted)
                            if (quoted || !isReservedKeyword(name)) && !isKeyword("COLUMNS") =>
                            Some(expectName())
                        case _ =>
                            None
            expectKeyword("COLUMNS")
            expectSymbol("(")
            val columns = parseJsonColumns()
            expectSymbol(")")
            SqlJsonColumn.Nested(path, pathAlias, columns)
        else
            val name = expectName()
            if consumeKeyword("FOR") then
                expectKeyword("ORDINALITY")
                SqlJsonColumn.Ordinality(name)
            else
                val targetType = parseType()
                if consumeKeyword("EXISTS") then
                    val path = if consumeKeyword("PATH") then Some(parseOr()) else None
                    val onError = parseJsonExistsOnError()
                    SqlJsonColumn.Exists(name, targetType, path, onError)
                else
                    val format = parseJsonOutputFormat()
                    val path = if consumeKeyword("PATH") then Some(parseOr()) else None
                    val wrapper = parseJsonQueryWrapper()
                    val quotes = parseJsonQuotes()
                    val (onEmpty, onError) = parseJsonQueryBehaviors()
                    SqlJsonColumn.Column(name, targetType, format, path, wrapper, quotes, onEmpty, onError)

    private def parseJsonTableOnError(): Option[SqlJsonErrorBehavior] =
        if consumeKeyword("ERROR") then
            expectKeyword("ON")
            expectKeyword("ERROR")
            Some(SqlJsonErrorBehavior.Error)
        else if consumeKeyword("EMPTY") then
            val behavior =
                if consumeKeyword("ARRAY") then SqlJsonErrorBehavior.EmptyArray
                else SqlJsonErrorBehavior.Empty
            expectKeyword("ON")
            expectKeyword("ERROR")
            Some(behavior)
        else
            None

    private def parseGraphTable(lateral: Boolean): SqlTable =
        advance()
        expectSymbol("(")
        val name = expectName()
        expectKeyword("MATCH")
        val matchMode = parseGraphMatchMode()
        val patterns = parseGraphPatterns()
        val where = if consumeKeyword("WHERE") then Some(parseOr()) else None
        val rowsMode = parseGraphRowsMode()
        expectKeyword("COLUMNS")
        expectSymbol("(")
        val columns = parseSelectItems()
        expectSymbol(")")
        val exportMode = parseGraphExportMode()
        expectSymbol(")")
        val alias = parseOptionalTableAlias()
        val matchRecognize = parseOptionalMatchRecognize()
        SqlTable.Graph(
            lateral,
            name,
            matchMode,
            patterns,
            where,
            rowsMode,
            columns,
            exportMode,
            alias,
            matchRecognize
        )

    private def parseGraphMatchMode(): Option[SqlGraphMatchMode] =
        if consumeKeyword("REPEATABLE") then
            val mode =
                if consumeKeyword("ELEMENT") then
                    if consumeKeyword("BINDINGS") then SqlGraphRepeatableMode.ElementBindings
                    else SqlGraphRepeatableMode.Element
                else if consumeKeyword("ELEMENTS") then
                    SqlGraphRepeatableMode.Elements
                else
                    failExpected("ELEMENT, ELEMENT BINDINGS, or ELEMENTS")
            Some(SqlGraphMatchMode.Repeatable(mode))
        else if consumeKeyword("DIFFERENT") then
            val mode =
                if consumeKeyword("EDGE") then
                    if consumeKeyword("BINDINGS") then SqlGraphDifferentMode.EdgeBindings
                    else SqlGraphDifferentMode.Edge
                else if consumeKeyword("EDGES") then
                    SqlGraphDifferentMode.Edges
                else
                    failExpected("EDGE, EDGE BINDINGS, or EDGES")
            Some(SqlGraphMatchMode.Different(mode))
        else
            None

    private def parseGraphPatterns(): NonEmptyList[SqlGraphPattern] =
        val head = parseGraphPattern()
        val tail = ListBuffer[SqlGraphPattern]()
        while consumeSymbol(",") do tail += parseGraphPattern()
        NonEmptyList(head, tail.toList)

    private def parseGraphPattern(): SqlGraphPattern =
        val name =
            if isGraphNameBinding() then
                val name = expectName()
                expectSymbol("=")
                Some(name)
            else
                None
        SqlGraphPattern(name, parseGraphPatternTerm())

    private def isGraphNameBinding(): Boolean =
        current match
            case Token.Identifier(_, _) => isSymbolAt(1, "=")
            case _ => false

    private def parseGraphPatternTerm(): SqlGraphPatternTerm =
        var left = parseGraphPatternAnd()
        var continue = true
        while continue do
            if isSymbol("|") && isSymbolAt(1, "+") && isSymbolAt(2, "|") then
                advance()
                advance()
                advance()
                left = SqlGraphPatternTerm.Alternation(left, parseGraphPatternAnd())
            else if isSymbol("|") then
                advance()
                left = SqlGraphPatternTerm.Or(left, parseGraphPatternAnd())
            else
                continue = false
        left

    private def parseGraphPatternAnd(): SqlGraphPatternTerm =
        var left = parseGraphPatternPrimary()
        while isGraphTermStart() do
            left = SqlGraphPatternTerm.And(left, parseGraphPatternPrimary())
        left

    private def isGraphTermStart(): Boolean =
        isSymbol("(") || isGraphEdgeStartAt(0)

    private def isGraphEdgeStartAt(n: Int): Boolean =
        if isSymbolAt(n, "-") || isSymbolAt(n, "~") then true
        else if isSymbolAt(n, "<") then
            isSymbolAt(n + 1, "-") || isSymbolAt(n + 1, "~")
        else
            false

    private def isGraphGroupedPatternStart(): Boolean =
        isSymbol("(") && (isSymbolAt(1, "(") || isGraphEdgeStartAt(1))

    private def parseGraphPatternPrimary(): SqlGraphPatternTerm =
        val base =
            if isGraphGroupedPatternStart() then
                expectSymbol("(")
                val term = parseGraphPatternTerm()
                expectSymbol(")")
                term
            else if isGraphEdgeStartAt(0) then
                parseGraphEdge()
            else
                parseGraphVertex()
        parseGraphQuantifier() match
            case Some(quantifier) => SqlGraphPatternTerm.Quantified(base, quantifier)
            case None => base

    private def parseGraphVertex(): SqlGraphPatternTerm =
        expectSymbol("(")
        val name =
            current match
                case Token.Identifier(n, quoted) if quoted || !isReservedKeyword(n) =>
                    Some(expectName())
                case _ =>
                    None
        val label = if consumeKeyword("IS") then Some(parseGraphLabel()) else None
        val where = if consumeKeyword("WHERE") then Some(parseOr()) else None
        expectSymbol(")")
        SqlGraphPatternTerm.Vertex(name, label, where)

    private def parseGraphEdge(): SqlGraphPatternTerm =
        val leftSymbol = parseGraphSymbol()
        expectSymbol("[")
        val name =
            current match
                case Token.Identifier(n, quoted) if quoted || !isReservedKeyword(n) =>
                    Some(expectName())
                case _ =>
                    None
        val label = if consumeKeyword("IS") then Some(parseGraphLabel()) else None
        val where = if consumeKeyword("WHERE") then Some(parseOr()) else None
        expectSymbol("]")
        val rightSymbol = parseGraphSymbol()
        SqlGraphPatternTerm.Edge(leftSymbol, name, label, where, rightSymbol)

    private def parseGraphSymbol(): SqlGraphSymbol =
        if consumeSymbol("<") then
            if consumeSymbol("-") then SqlGraphSymbol.LeftArrow
            else if consumeSymbol("~") then SqlGraphSymbol.LeftTildeArrow
            else failExpected("'-' or '~' after '<'")
        else if consumeSymbol("-") then
            if consumeSymbol(">") then SqlGraphSymbol.RightArrow else SqlGraphSymbol.Dash
        else if consumeSymbol("~") then
            if consumeSymbol(">") then SqlGraphSymbol.RightTildeArrow else SqlGraphSymbol.Tilde
        else
            failExpected("a graph edge symbol")

    private def parseGraphLabel(): SqlGraphLabel =
        var left = parseGraphLabelAnd()
        while consumeSymbol("|") do
            left = SqlGraphLabel.Or(left, parseGraphLabelAnd())
        left

    private def parseGraphLabelAnd(): SqlGraphLabel =
        var left = parseGraphLabelPrimary()
        while consumeSymbol("&") do
            left = SqlGraphLabel.And(left, parseGraphLabelPrimary())
        left

    private def parseGraphLabelPrimary(): SqlGraphLabel =
        if consumeSymbol("%") then
            SqlGraphLabel.Percent
        else if consumeSymbol("!") then
            expectSymbol("(")
            val label = parseGraphLabel()
            expectSymbol(")")
            SqlGraphLabel.Not(label)
        else if consumeSymbol("(") then
            val label = parseGraphLabel()
            expectSymbol(")")
            label
        else
            SqlGraphLabel.Label(expectName())

    private def parseGraphQuantifier(): Option[SqlGraphQuantifier] =
        if consumeSymbol("*") then
            Some(SqlGraphQuantifier.Asterisk)
        else if consumeSymbol("?") then
            Some(SqlGraphQuantifier.Question)
        else if consumeSymbol("+") then
            Some(SqlGraphQuantifier.Plus)
        else if isSymbol("{") then
            advance()
            val start = if isSymbol(",") || isSymbol("}") then None else Some(parseOr())
            if consumeSymbol(",") then
                val end = if isSymbol("}") then None else Some(parseOr())
                expectSymbol("}")
                Some(SqlGraphQuantifier.Between(start, end))
            else
                expectSymbol("}")
                start match
                    case Some(expr) => Some(SqlGraphQuantifier.Quantity(expr))
                    case None => failInvalid("graph pattern quantifier", "expected a quantity")
        else
            None

    private def parseGraphRowsMode(): Option[SqlGraphRowsMode] =
        if consumeKeyword("ONE") then
            expectKeyword("ROW")
            expectKeyword("PER")
            if consumeKeyword("MATCH") then
                Some(SqlGraphRowsMode.Match)
            else if consumeKeyword("VERTEX") then
                expectSymbol("(")
                val name = expectName()
                expectSymbol(")")
                Some(SqlGraphRowsMode.Vertex(name, parseGraphInPaths()))
            else if consumeKeyword("STEP") then
                expectSymbol("(")
                val vertex1 = expectName()
                expectSymbol(",")
                val edge = expectName()
                expectSymbol(",")
                val vertex2 = expectName()
                expectSymbol(")")
                Some(SqlGraphRowsMode.Step(vertex1, edge, vertex2, parseGraphInPaths()))
            else
                failExpected("MATCH, VERTEX, or STEP after ONE ROW PER")
        else
            None

    private def parseGraphInPaths(): List[String] =
        if consumeKeyword("IN") then
            expectSymbol("(")
            val paths = parseColumnNameList()
            expectSymbol(")")
            paths
        else
            Nil

    private def parseGraphExportMode(): Option[SqlGraphExportMode] =
        if consumeKeyword("EXPORT") then
            if consumeKeyword("ALL") then
                expectKeyword("SINGLETONS")
                expectKeyword("EXCEPT")
                expectSymbol("(")
                val names = parseNameList()
                expectSymbol(")")
                Some(SqlGraphExportMode.AllSingletons(names))
            else if consumeKeyword("SINGLETONS") then
                expectSymbol("(")
                val names = parseNameList()
                expectSymbol(")")
                Some(SqlGraphExportMode.Singletons(names))
            else if consumeKeyword("NO") then
                expectKeyword("SINGLETONS")
                Some(SqlGraphExportMode.NoSingletons)
            else
                failExpected("ALL, SINGLETONS, or NO after EXPORT")
        else
            None

    private def parseOptionalTableAlias(): Option[SqlTableAlias] =
        if consumeKeyword("AS") then
            Some(parseTableAliasName())
        else
            current match
                case Token.Identifier(name, quoted) if quoted || !isReservedKeyword(name) =>
                    Some(parseTableAliasName())
                case _ =>
                    None

    private def parseTableAliasName(): SqlTableAlias =
        val alias = expectName()
        val columnAliases =
            if consumeSymbol("(") then
                val names = parseColumnNameList()
                expectSymbol(")")
                names
            else
                Nil
        SqlTableAlias(alias, columnAliases)

    private def parseColumnNameList(): List[String] =
        val names = ListBuffer[String]()
        names += expectName()
        while consumeSymbol(",") do names += expectName()
        names.toList

    private def parseNameList(): NonEmptyList[String] =
        val names = ListBuffer[String]()
        names += expectName()
        while consumeSymbol(",") do names += expectName()
        NonEmptyList(names.head, names.tail.toList)

    // --------------------------------------------------------------- subquery

    private def isQueryStart(): Boolean =
        isKeyword("SELECT") || isKeyword("WITH") || isKeyword("VALUES")

    private def parseSubquery(): SqlQuery =
        parseQueryInternal()

object SqlParser:
    /**
     * Parses a single standard SQL expression.
     */
    def parseExpr(sql: String): SqlExpr =
        new Parser(sql, new SqlLexer(sql).tokenize()).parseExpr()

    /**
     * Parses a single standard SQL query.
     */
    def parseQuery(sql: String): SqlQuery =
        new Parser(sql, new SqlLexer(sql).tokenize()).parseQuery()