package sqala.test

import sqala.ast.expr.*
import sqala.ast.statement.*
import sqala.ast.token.SqlUnsafeCustomToken
import sqala.printer.*
import sqala.util.NonEmptyList

object SecurityFixture:
    def std(escape: Boolean = true)(f: StandardSqlPrinter => Unit): String =
        val printer = new StandardSqlPrinter:
            val standardEscapeStrings = escape
        f(printer)
        printer.sql

    def render(p: StandardSqlPrinter)(f: StandardSqlPrinter => Unit): String =
        f(p)
        p.sql

    def nel[A](h: A, t: A*): NonEmptyList[A] = NonEmptyList(h, t.toList)

    /** Payloads that must never escape their surrounding quotes. */
    val payloads: List[String] = List(
        "x' OR '1'='1",
        "'; DROP TABLE \"t\"; --",
        "\\' OR 1=1 --",
        "a''b",
        "line1\nline2",
        "tab\there",
        "nul\u0000here",
        "back\\slash",
        "unicode\u2028\u2029",
        "';\u0000DROP TABLE t;--"
    )

class S1_Security extends munit.FunSuite:
    import SecurityFixture.*

    test("single quotes in literals are always doubled"):
        for payload <- payloads do
            val printed = std()(_.printExpr(SqlExpr.StringLiteral(payload)))
            assert(printed.startsWith("'") && printed.endsWith("'"), printed)
            val body = printed.substring(1, printed.length - 1)
            val quotes = body.count(_ == '\'')
            assertEquals(quotes, payload.count(_ == '\'') * 2, s"quote count for [$payload]: $printed")

    test("a quote payload stays a single literal token"):
        val printed = std()(_.printExpr(SqlExpr.StringLiteral("x' OR '1'='1")))
        assertEquals(printed, "'x'' OR ''1''=''1'")

    test("backslash doubling follows standardEscapeStrings"):
        assertEquals(std(true)(_.printExpr(SqlExpr.StringLiteral("a\\b"))), "'a\\b'")
        assertEquals(std(false)(_.printExpr(SqlExpr.StringLiteral("a\\b"))), "'a\\\\b'")

    test("backslash before a quote cannot break out in escape mode"):
        assertEquals(std(false)(_.printExpr(SqlExpr.StringLiteral("\\'"))), "'\\\\'''")
        assertEquals(std(true)(_.printExpr(SqlExpr.StringLiteral("\\'"))), "'\\'''")

    test("control characters are passed through verbatim"):
        assertEquals(std()(_.printExpr(SqlExpr.StringLiteral("a\nb"))), "'a\nb'")
        assertEquals(std()(_.printExpr(SqlExpr.StringLiteral("a\u0000b"))), "'a\u0000b'")

    test("identifier quotes are doubled per dialect"):
        assertEquals(std()(_.printIdent("a\"b")), """"a""b"""")
        assertEquals(render(new MysqlPrinter(true))(_.printIdent("a`b")), "`a``b`")
        assertEquals(render(new SqlserverPrinter(true))(_.printIdent("a]b")), "[a]]b]")

    test("an identifier payload stays a single quoted identifier"):
        val printed = std()(_.printIdent("""x"; DROP TABLE "t"; --"""))
        assertEquals(printed, """"x""; DROP TABLE ""t""; --"""")

    test("identifier escaping is applied everywhere an identifier is printed"):
        val evil = """a" = "b"""
        val q = SqlQuery.Select(
            None,
            nel(SqlSelectItem.Expr(SqlExpr.Column(Some(evil), evil), Some(evil))),
            List(QueryFixture.table(evil)),
            Some(SqlExpr.Binary(SqlExpr.Column(None, evil), SqlBinaryOperator.Equal, SqlExpr.NumberLiteral(1))),
            None, None, Nil, None, Nil, None, None
        )
        val printed = std()(_.printQuery(q))
        val bare = printed.replace("\"\"", "")
        assertEquals(bare.count(_ == '"'), 10, s"unescaped identifier quote leaked:\n$printed")

    test("a plain function name stays unquoted so the server folds its case"):
        assertEquals(
            std()(_.printExpr(SqlExpr.GeneralFunc(None, "ST_GeomFromText", Nil, Nil, Nil, None))),
            "ST_GeomFromText()"
        )

    test("a function name with unsupported characters is quoted, not stripped"):
        assertEquals(
            std()(_.printExpr(SqlExpr.GeneralFunc(None, "my-func", List(SqlExpr.NumberLiteral(1)), Nil, Nil, None))),
            """"my-func"(1)"""
        )

    test("a non-ASCII function name is quoted"):
        assertEquals(
            std()(_.printExpr(SqlExpr.GeneralFunc(None, "\u51fd\u6570", Nil, Nil, Nil, None))),
            "\"\u51fd\u6570\"()"
        )

    test("a quote inside a function name is doubled"):
        assertEquals(
            std()(_.printExpr(SqlExpr.GeneralFunc(None, "a\"b", Nil, Nil, Nil, None))),
            """"a""b"()"""
        )

    test("an empty identifier or function name is rejected"):
        intercept[PrintException](std()(_.printIdent("")))
        intercept[PrintException](std()(_.printExpr(SqlExpr.GeneralFunc(None, "", Nil, Nil, Nil, None))))

    test("non-finite numbers are rejected instead of printed"):
        for n <- List(Double.NaN, Double.PositiveInfinity, Double.NegativeInfinity) do
            intercept[PrintException](std()(_.printExpr(SqlExpr.NumberLiteral(n))))
        for n <- List(Float.NaN, Float.PositiveInfinity, Float.NegativeInfinity) do
            intercept[PrintException](std()(_.printExpr(SqlExpr.NumberLiteral(n))))

    test("unsafe custom tokens are emitted verbatim by design"):
        // SqlUnsafeCustomToken is the documented escape hatch; callers own its safety.
        val printed = std()(_.printExpr(SqlExpr.UnsafeCustom(SqlUnsafeCustomToken.Keyword("RAW SQL") :: Nil)))
        assertEquals(printed, "(RAW SQL)")

    test("inlined literals use the same escaping as bound-position literals"):
        for payload <- payloads do
            val direct = std()(_.printExpr(SqlExpr.StringLiteral(payload)))
            val inlined = std()(_.printExpr(SqlExpr.InlinedLiteral(SqlExpr.StringLiteral(payload))))
            assertEquals(inlined, direct, s"escaping diverged for [$payload]")

    test("every dialect doubles quotes in literals"):
        val printers: List[StandardSqlPrinter] = List(
            new MysqlPrinter(true), new PostgresqlPrinter(true), new SqlitePrinter(true),
            new OraclePrinter(true), new H2Printer(true)
        )
        for p <- printers do
            p.printExpr(SqlExpr.StringLiteral("a'b"))
            assertEquals(p.sql, "'a''b'", p.getClass.getSimpleName)

extension (p: StandardSqlPrinter)
    private def sqlOf(f: StandardSqlPrinter => Unit): String =
        f(p)
        p.sql