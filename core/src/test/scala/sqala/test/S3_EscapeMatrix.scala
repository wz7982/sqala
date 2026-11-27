package sqala.test

import sqala.ast.expr.*
import sqala.parser.SqlParser
import sqala.printer.*

/**
 * String-literal escaping matrix: every dialect printer, both
 * standardEscapeStrings values, plain and inlined paths, over an extended
 * attack payload list. The expected output is the full algebraic form of the
 * escaping rule, so any drift in quote or backslash handling fails here.
 */
class S3_EscapeMatrix extends munit.FunSuite:
    private val payloads: List[String] = List(
        "plain", "x' OR '1'='1", "'; DROP TABLE \"t\"; --", "\\' OR 1=1 --",
        "a''b", "line1\nline2", "tab\there", "nul\u0000here", "back\\slash",
        "unicode\u2028\u2029", "';\u0000DROP TABLE t;--",
        "'", "''", "\\", "\\\\", "\\'", "'\\", "\\''", "''\\",
        "%' LIKE '%", "_", "[", "]", "--", "/*", "*/", ";",
        "a\"b", "a`b", "N'x'", "E'\\'", "$$", "chr(39)",
        "' UNION SELECT --", "' OR 1=1#", "' AND SLEEP(5)/*",
        "\r\n", "\u001a", "\\N", "0x", "char(0x27)"
    )

    private def show(s: String): String =
        s.flatMap {
            case '\n' => "\\n"
            case '\r' => "\\r"
            case '\t' => "\\t"
            case '\u0000' => "\\0"
            case '\u001a' => "\\x1a"
            case c => c.toString
        }

    private def printers: List[(String, Boolean => StandardSqlPrinter)] = List(
        ("standard", e => new StandardSqlPrinter:
            val standardEscapeStrings = e),
        ("postgresql", e => new PostgresqlPrinter(e)),
        ("sqlite", e => new SqlitePrinter(e)),
        ("oracle", e => new OraclePrinter(e)),
        ("h2", e => new H2Printer(e)),
        ("mysql", e => new MysqlPrinter(e)),
        ("sqlserver", e => new SqlserverPrinter(e))
    )

    /**
     * The escaping contract: single quotes are always doubled; a backslash is
     * verbatim when standardEscapeStrings is on and doubled when it is off.
     * Sqlserver prefixes every string literal with N on both paths.
     */
    private def expected(payload: String, esc: Boolean, tag: String): String =
        val sb = new StringBuilder
        if tag == "sqlserver" then sb.append("N")
        sb.append('\'')
        payload.foreach {
            case '\'' => sb.append("''")
            case '\\' => if esc then sb.append('\\') else sb.append("\\\\")
            case c => sb.append(c)
        }
        sb.append('\'')
        sb.toString

    private def printLiteral(mk: Boolean => StandardSqlPrinter, esc: Boolean, path: String, payload: String): String =
        val p = mk(esc)
        val lit: SqlExpr.StringLiteral = SqlExpr.StringLiteral(payload)
        if path == "inlined" then p.printExpr(SqlExpr.InlinedLiteral(lit))
        else p.printExpr(lit)
        p.sql

    test("literal output matches the escaping algebra for the whole matrix"):
        for (tag, mk) <- printers; esc <- List(true, false); path <- List("plain", "inlined"); payload <- payloads do
            val clue = s"$tag escape=$esc path=$path payload=[${show(payload)}]"
            assertEquals(printLiteral(mk, esc, path, payload), expected(payload, esc, tag), clue)

    /** True when every quote inside the literal body is doubled. */
    private def bodyQuotesDoubled(out: String): Boolean =
        val body =
            if out.startsWith("N'") then out.substring(2, out.length - 1)
            else out.substring(1, out.length - 1)
        var i = 0
        var ok = true
        while i < body.length && ok do
            if body.charAt(i) == '\'' then
                ok = i + 1 < body.length && body.charAt(i + 1) == '\''
                i += 1
            i += 1
        ok

    test("no payload terminates its literal early"):
        for (tag, mk) <- printers; esc <- List(true, false); path <- List("plain", "inlined"); payload <- payloads do
            val out = printLiteral(mk, esc, path, payload)
            assert(bodyQuotesDoubled(out), s"$tag escape=$esc path=$path payload=[${show(payload)}] leaked: $out")

    test("every printed literal is accepted by the standard parser and reprints identically"):
        for (tag, mk) <- printers; esc <- List(true, false); path <- List("plain", "inlined"); payload <- payloads do
            val clue = s"$tag escape=$esc path=$path payload=[${show(payload)}]"
            val printed = printLiteral(mk, esc, path, payload)
            val src = if printed.startsWith("N'") then printed.substring(1) else printed
            val ast =
                try SqlParser.parseExpr(src)
                catch case ex: Throwable => fail(s"literal failed to parse: $src", ex)
            val p2 = new PostgresqlPrinter(true)
            p2.printExpr(ast)
            assertEquals(p2.sql, src, clue)
