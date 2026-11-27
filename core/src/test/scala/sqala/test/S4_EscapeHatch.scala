package sqala.test

import sqala.ast.expr.*
import sqala.ast.quantifier.SqlQuantifier
import sqala.ast.statement.{SqlQuery, SqlSelectItem}
import sqala.ast.table.{SqlJoinType, SqlTable}
import sqala.ast.token.SqlUnsafeCustomToken
import sqala.printer.*
import sqala.util.NonEmptyList

import java.nio.file.Files
import scala.jdk.CollectionConverters.*

/**
 * Escape-hatch contract. Unsafe custom tokens are documented verbatim
 * passthroughs: keyword text is emitted as-is and caller owns its safety,
 * while sibling sub-expression tokens stay on the normal escaping path. The
 * suite also pins the token-gluing rules and guards that no new raw string
 * sink appears in the core printers outside the known allowlist.
 */
class S4_EscapeHatch extends munit.FunSuite:
    private def pg(f: StandardSqlPrinter => Unit): String =
        val p = new PostgresqlPrinter(true)
        f(p)
        p.sql

    private def kw(s: String): SqlUnsafeCustomToken = SqlUnsafeCustomToken.Keyword(s)
    private def tok(e: SqlExpr): SqlUnsafeCustomToken = SqlUnsafeCustomToken.Expr(e)
    private def evil: SqlExpr = SqlExpr.StringLiteral("a'b")

    /**
     * Every `sqlBuilder.append(variable)` in the core printers must use one of
     * the known allowlisted variables: escaped paths (printIdent/printChars),
     * numeric literals, the unsafe-token sink, or internal layout strings.
     * A new raw append of an arbitrary string variable fails this guard.
     */
    private val allowedSinks: Set[String] = Set(
        "n", "l", "p", "s", "c", "ident", "leftQuote", "rightQuote", "separator", "spaceString"
    )

    private def rawAppends(text: String): List[(Int, String)] =
        text.linesIterator.zipWithIndex
            .map((line, i) => (i + 1, line.trim))
            .collect {
                case (line, trimmed) if trimmed.startsWith("sqlBuilder.append(") =>
                    val arg = trimmed.stripPrefix("sqlBuilder.append(").takeWhile(_ != ')')
                    if arg.matches("[A-Za-z][A-Za-z0-9_]*") && !Set("None", "true", "false").contains(arg) then Some(line -> arg) else None
            }
            .flatten
            .toList

    private def violations(found: List[(Int, String)]): List[String] =
        found.filter((_, arg) => !allowedSinks.contains(arg)).map((line, arg) => s"line $line: append($arg)")

    test("keyword tokens are emitted verbatim in every slot"):
        assertEquals(pg(_.printExpr(SqlExpr.UnsafeCustom(List(kw("RAW SQL"))))), "(RAW SQL)")
        assertEquals(pg(_.printExpr(SqlExpr.UnsafeCustom(List(kw("' OR 1=1 --"))))), "(' OR 1=1 --)")
        assertEquals(
            pg(_.printExpr(SqlExpr.Binary(SqlExpr.Column(None, "a"), SqlBinaryOperator.UnsafeCustom(List(kw("<=>"))), evil))),
            "\"a\" <=> 'a''b'"
        )
        assertEquals(pg(_.printExpr(SqlExpr.Unary(SqlUnaryOperator.UnsafeCustom(List(kw("~"))), evil))), "~('a''b')")
        assertEquals(pg(_.printTable(SqlTable.Join(SqlTable.Ident("t1", None, None, None, None), SqlJoinType.UnsafeCustom(List(kw("STRAIGHT_JOIN"))), SqlTable.Ident("t2", None, None, None, None), None))), "\"t1\"\nSTRAIGHT_JOIN \"t2\"")
        assertEquals(
            pg(_.printQuery(SqlQuery.Select(Some(SqlQuantifier.UnsafeCustom(List(kw("UNIQUE")))), NonEmptyList(SqlSelectItem.Expr(evil, None), Nil), Nil, None, None, None, Nil, None, Nil, None, None))),
            "SELECT UNIQUE\n    'a''b'"
        )
        assertEquals(pg(_.printExpr(SqlExpr.IntervalLiteral("1'2", SqlIntervalField.Single(SqlTimeUnit.UnsafeCustom(List(kw("DAY"))))))), "INTERVAL '1''2' DAY")
        assertEquals(pg(_.printEncoding(SqlEncoding.UnsafeCustom(List(kw("FOR"), kw("SBCS"))))), "FOR SBCS")
        assertEquals(
            pg(_.printExpr(SqlExpr.QuantifiedComparisonPredicate(SqlExpr.NumberLiteral(1), SqlQuantifiedComparisonOperator.UnsafeCustom(List(kw("=>"))), SqlSubqueryQuantifier.All, SqlQuery.Select(None, NonEmptyList(SqlSelectItem.Expr(SqlExpr.NumberLiteral(1), None), Nil), Nil, None, None, None, Nil, None, Nil, None, None)))),
            "1 => ALL(\n    SELECT\n        1\n)"
        )

    test("a cast type escape hatch only emits its own tokens"):
        // Caller-supplied tokens must close the CAST parenthesis themselves.
        assertEquals(pg(_.printExpr(SqlExpr.Cast(evil, SqlType.UnsafeCustom(List(kw("MYTYPE("), kw("2")))))), "CAST('a''b' AS MYTYPE(2)")
        assertEquals(pg(_.printExpr(SqlExpr.Cast(evil, SqlType.UnsafeCustom(List(kw("MYTYPE("), kw("2"), kw(")")))))), "CAST('a''b' AS MYTYPE(2))")

    test("sub-expression tokens keep the normal escaping path"):
        assertEquals(pg(_.printExpr(SqlExpr.UnsafeCustom(List(kw("MATCH"), kw("("), tok(evil), kw(")"))))), "(MATCH ('a''b'))")
        assertEquals(pg(_.printExpr(SqlExpr.UnsafeCustom(List(kw("+"), tok(evil))))), "(+ 'a''b')")

    test("token gluing follows the word-boundary rule"):
        assertEquals(pg(_.printExpr(SqlExpr.UnsafeCustom(List(kw("A("), kw("B"), kw(")"), kw(",x"), kw("C"))))), "(A(B),x C)")
        assertEquals(pg(_.printExpr(SqlExpr.UnsafeCustom(List(kw(""), kw("A"))))), "( A)")
        assertEquals(pg(_.printExpr(SqlExpr.UnsafeCustom(List(kw("A  B"), kw("C"))))), "(A  B C)")

    test("the sink scan finds the known raw appends and flags unknown variables"):
        val synthetic =
            """
            |    sqlBuilder.append(evilInput)
            |    sqlBuilder.append("LITERAL")
            |    sqlBuilder.append(c)
            """.stripMargin
        assertEquals(violations(rawAppends(synthetic)), List("line 2: append(evilInput)"))

    test("no core printer appends an allowlist-excluded variable"):
        val printerDir =
            val cwd = java.nio.file.Paths.get(System.getProperty("user.dir")).toAbsolutePath
            var root = cwd
            while !Files.exists(root.resolve("build.sbt")) && root.getParent != null do root = root.getParent
            root.resolve("core/src/main/scala/sqala/printer")

        assert(Files.isDirectory(printerDir), s"printer sources not found at $printerDir")

        val files = Files.list(printerDir).iterator().asScala.toList
        val found = files.flatMap(f => rawAppends(new String(Files.readAllBytes(f), java.nio.charset.StandardCharsets.UTF_8)))
        assert(found.nonEmpty, "the scan found no raw appends at all; the scanner is broken")
        assertEquals(violations(found), Nil, s"new raw sqlBuilder.append sink in core printers")
