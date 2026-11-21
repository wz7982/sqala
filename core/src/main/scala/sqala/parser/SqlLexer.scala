package sqala.parser

/**
 * A hand-written lexer for standard SQL text.
 *
 * It skips whitespace and rejects SQL comments, reporting positions for
 * diagnostics. Statement terminators (`;`) are rejected because the parser
 * only accepts a single query or expression.
 */
final class SqlLexer(source: String):
    private var offset = 0

    private var line = 1

    private var column = 1

    private val tokens = List.newBuilder[TokenPosition]

    /**
     * Tokenizes the whole input and appends a Token.EndOfInput token.
     */
    def tokenize(): Vector[TokenPosition] =
        while offset < source.length do
            skipIgnored()
            if offset < source.length then
                tokens += readToken()
        tokens += TokenPosition(Token.EndOfInput, offset, line, column)
        tokens.result().toVector

    private def readToken(): TokenPosition =
        val startOffset = offset
        val startLine = line
        val startColumn = column
        val c = current
        val token =
            if isIdentifierStart(c) then
                readIdentifier()
            else if c == '"' then
                readQuotedIdentifier()
            else if c == '`' then
                readBacktickIdentifier()
            else if c == '\'' then
                readStringLiteral()
            else if c.isDigit || (c == '.' && peek(1).isDigit) then
                readNumber(startOffset, startLine, startColumn)
            else if c == ';' then
                fail("Unsupported statement terminator ';'", startOffset, startLine, startColumn)
            else
                readSymbol(startOffset, startLine, startColumn)
        TokenPosition(token, startOffset, startLine, startColumn)

    private def skipIgnored(): Unit =
        var continue = true
        while continue && offset < source.length do
            val c = current
            if c.isWhitespace then
                advance()
            else if c == '-' && peek(1) == '-' then
                fail("Line comments are not supported", offset, line, column)
            else if c == '/' && peek(1) == '*' then
                fail("Block comments are not supported", offset, line, column)
            else
                continue = false

    private def readIdentifier(): Token =
        val start = offset
        while offset < source.length && isIdentifierPart(current) do advance()
        Token.Identifier(source.substring(start, offset), false)

    private def readQuotedIdentifier(): Token =
        val startOffset = offset
        val startLine = line
        val startColumn = column
        advance()
        val builder = new StringBuilder
        var closed = false
        while !closed && offset < source.length do
            val c = current
            if c == '"' then
                if peek(1) == '"' then
                    builder.append('"')
                    advance()
                    advance()
                else
                    advance()
                    closed = true
            else
                builder.append(c)
                advance()
        if !closed then
            fail("Invalid quoted identifier: unterminated", startOffset, startLine, startColumn)
        Token.Identifier(builder.toString, true)

    private def readBacktickIdentifier(): Token =
        val startOffset = offset
        val startLine = line
        val startColumn = column
        advance()
        val builder = new StringBuilder
        var closed = false
        while !closed && offset < source.length do
            val c = current
            if c == '`' then
                if peek(1) == '`' then
                    builder.append('`')
                    advance()
                    advance()
                else
                    advance()
                    closed = true
            else
                builder.append(c)
                advance()
        if !closed then
            fail("Invalid backtick identifier: unterminated", startOffset, startLine, startColumn)
        Token.Identifier(builder.toString, true)

    private def readStringLiteral(): Token =
        val startOffset = offset
        val startLine = line
        val startColumn = column
        advance()
        val builder = new StringBuilder
        var closed = false
        while !closed && offset < source.length do
            val c = current
            if c == '\'' then
                if peek(1) == '\'' then
                    builder.append('\'')
                    advance()
                    advance()
                else
                    advance()
                    closed = true
            else
                builder.append(c)
                advance()
        if !closed then
            fail("Invalid string literal: unterminated", startOffset, startLine, startColumn)
        Token.StringLiteral(builder.toString)

    private def readNumber(startOffset: Int, startLine: Int, startColumn: Int): Token =
        val start = offset
        var hasDigits = false
        while offset < source.length && current.isDigit do
            advance()
            hasDigits = true
        if offset < source.length && current == '.' then
            advance()
            while offset < source.length && current.isDigit do
                advance()
                hasDigits = true
        if !hasDigits then
            fail("Invalid numeric literal", startOffset, startLine, startColumn)
        if offset < source.length && (current == 'e' || current == 'E') then
            advance()
            if offset < source.length && (current == '+' || current == '-') then advance()
            if offset >= source.length || !current.isDigit then
                fail("Invalid numeric literal", startOffset, startLine, startColumn)
            while offset < source.length && current.isDigit do advance()
        if offset < source.length && (current == '.' || isIdentifierPart(current)) then
            fail("Invalid numeric literal", startOffset, startLine, startColumn)
        val text = source.substring(start, offset)
        try
            Token.NumberLiteral(BigDecimal(text))
        catch
            case _: NumberFormatException =>
                fail("Invalid numeric literal", startOffset, startLine, startColumn)

    private def readSymbol(startOffset: Int, startLine: Int, startColumn: Int): Token =
        val two = peek(0).toString + peek(1).toString
        if two == "<>" || two == "!=" || two == ">=" || two == "<=" || two == "||" then
            advance()
            advance()
            Token.Symbol(two)
        else
            val c = current
            if "()[]{},.*+-/%=<>|^$?!&~".indexOf(c) >= 0 then
                advance()
                Token.Symbol(c.toString)
            else
                fail(s"Invalid character '$c'", startOffset, startLine, startColumn)

    private def current: Char =
        source.charAt(offset)

    private def peek(n: Int): Char =
        val index = offset + n
        if index < source.length then source.charAt(index) else '\u0000'

    private def advance(): Unit =
        val c = source.charAt(offset)
        offset += 1
        if c == '\n' then
            line += 1
            column = 1
        else
            column += 1

    private def isIdentifierStart(c: Char): Boolean =
        c.isLetter || c == '_'

    private def isIdentifierPart(c: Char): Boolean =
        c.isLetterOrDigit || c == '_'

    private def fail(message: String, atOffset: Int, atLine: Int, atColumn: Int): Nothing =
        throw ParseException.at(message, source, atOffset, atLine, atColumn)