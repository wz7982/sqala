package sqala.test

import sqala.ast.expr.*
import sqala.ast.statement.*
import sqala.ast.table.{SqlTable, SqlTableAlias}
import sqala.ast.window.{SqlWindow, SqlWindowItem}
import sqala.parser.SqlParser
import sqala.printer.*
import sqala.test.QueryFixture.*

/**
 * Identifier injection surface: malicious table names, aliases, column
 * names, select aliases, CTE names, window names, and function names must
 * stay single quoted identifiers in every dialect and survive a
 * print-parse-print round-trip unchanged.
 */
class S2_IdentInjection extends munit.FunSuite:
    private val evil: List[String] = List(
        "a\"b", "a`b", "a]b", "a[b", "a'b", "\"", "`", "]",
        "; DROP TABLE \"t\"; --", "x' OR '1'='1", "line1\nline2",
        "nul\u0000x", "--", "/*", "abc def"
    )

    private def show(s: String): String =
        s.replace("\n", "\\n").replace("\u0000", "\\0")

    private def standardPrinter(): StandardSqlPrinter = new StandardSqlPrinter:
        val standardEscapeStrings = true

    private val dialects: List[(String, () => StandardSqlPrinter, Char, Char)] = List(
        ("standard", () => standardPrinter(), '"', '"'),
        ("postgresql", () => new PostgresqlPrinter(true), '"', '"'),
        ("sqlite", () => new SqlitePrinter(true), '"', '"'),
        ("oracle", () => new OraclePrinter(true), '"', '"'),
        ("h2", () => new H2Printer(true), '"', '"'),
        ("mysql", () => new MysqlPrinter(true), '`', '`'),
        ("sqlserver", () => new SqlserverPrinter(true), '[', ']')
    )

    test("printIdent wraps the payload with doubled closing quotes only"):
        for (tag, mk, lq, rq) <- dialects do
            for name <- evil do
                val p = mk()
                p.printIdent(name)
                val expected = s"$lq${name.replace(rq.toString, s"$rq$rq")}$rq"
                assertEquals(p.sql, expected, s"$tag payload [${show(name)}]")

    private def positions: List[(String, String => SqlQuery)] = List(
        "table" -> (e => select(nel(item(col("c"))), List(table(e)))),
        "table-alias" -> (e => select(nel(item(col("c"))), List(SqlTable.Ident("t", Some(SqlTableAlias(e, Nil)), None, None, None)))),
        "column-alias" -> (e => select(nel(item(col("c"))), List(SqlTable.Ident("t", Some(SqlTableAlias("al", List(e))), None, None, None)))),
        "column" -> (e => select(nel(item(col(e))), List(table("t")))),
        "column-qualifier" -> (e => select(nel(item(qcol(e, "c"))), List(table("t")))),
        "select-alias" -> (e => select(nel(itemAs(col("a"), e)), List(table("t")))),
        "cte-name" -> (e => SqlQuery.With(false, nel(SqlWithItem(e, Nil, simple)), select(nel(item(col("a"))), List(table(e))))),
        "cte-columns" -> (e => SqlQuery.With(false, nel(SqlWithItem("w", List(e), simple)), select(nel(item(col("a"))), List(table("w"))))),
        "window-name" -> (e => SqlQuery.Select(None, nel(item(col("a"))), Nil, None, None, None, List(SqlWindowItem(e, SqlWindow.Inlined(None, Nil, Nil, None))), None, Nil, None, None)),
        "window-ref" -> (e => SqlQuery.Select(None, nel(item(col("a"))), List(table("t")), None, None, None, List(SqlWindowItem("w", SqlWindow.Inlined(Some(e), Nil, Nil, None))), None, Nil, None, None)),
        "func-name" -> (e => select(nel(item(SqlExpr.GeneralFunc(None, e, Nil, Nil, Nil, None))))),
        "string-literal" -> (e => select(nel(item(SqlExpr.StringLiteral(e)))))
    )

    test("every identifier position round-trips through the parser unchanged"):
        for (pos, mk) <- positions; e <- evil do
            val p = new PostgresqlPrinter(true)
            p.printQuery(mk(e))
            val out = p.sql
            val ast =
                try SqlParser.parseQuery(out)
                catch case ex: Throwable => fail(s"$pos payload [${show(e)}] failed to re-parse:\n$out", ex)
            val p2 = new PostgresqlPrinter(true)
            p2.printQuery(ast)
            assertEquals(p2.sql, out, s"$pos payload [${show(e)}]")

    /**
     * Walks printed SQL with the dialect identifier-quote state machine and
     * fails if a closing identifier quote appears outside a well-formed
     * quoted region; string literals are skipped with the doubling rule.
     */
    private def assertNoIdentLeak(sql: String, lq: Char, rq: Char, clue: => String): Unit =
        var i = 0
        var inString = false
        while i < sql.length do
            val c = sql.charAt(i)
            if inString then
                if c == '\'' then
                    if i + 1 < sql.length && sql.charAt(i + 1) == '\'' then i += 1
                    else inString = false
            else if c == '\'' then inString = true
            else if c == lq then
                var j = i + 1
                var closed = false
                while j < sql.length && !closed do
                    if sql.charAt(j) == rq then
                        if j + 1 < sql.length && sql.charAt(j + 1) == rq then j += 1
                        else closed = true
                    j += 1
                assert(closed, s"unterminated identifier quote in $clue:\n$sql")
                i = j - 1
            i += 1
        assert(!inString, s"unterminated string literal in $clue:\n$sql")

    test("no identifier payload leaks an unquoted delimiter in any dialect"):
        val checked = dialects.filter(d => Set("postgresql", "mysql", "sqlserver").contains(d._1))
        for (tag, mk, lq, rq) <- checked; (pos, mkq) <- positions; e <- evil do
            val p = mk()
            val out =
                try { p.printQuery(mkq(e)); p.sql }
                catch case _: PrintException => ""
            if out.nonEmpty then
                assertNoIdentLeak(out, lq, rq, s"$tag $pos payload [${show(e)}]")
