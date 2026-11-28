package sqala.test

import sqala.ast.expr.{SqlExpr, SqlSubqueryQuantifier}
import sqala.ast.quantifier.SqlQuantifier
import sqala.ast.statement.{SqlSelectItem, SqlSetOperator}
import sqala.ast.token.SqlUnsafeCustomToken
import sqala.test.Coverage.assertCoverage

object QuantifierFixture:
    def col(name: String): SqlExpr = SqlExpr.Column(None, name)

    val subqueryQuantifiers: Map[String, (SqlSubqueryQuantifier, String)] = Map(
        "Any" -> (SqlSubqueryQuantifier.Any, "ANY"),
        "All" -> (SqlSubqueryQuantifier.All, "ALL")
    )

    val quantifiers: Map[String, (SqlQuantifier, String)] = Map(
        "All" -> (SqlQuantifier.All, "ALL"),
        "Distinct" -> (SqlQuantifier.Distinct, "DISTINCT")
    )

    val setOperators: Map[String, (SqlSetOperator, String)] = Map(
        "Intersect" -> (SqlSetOperator.Intersect(None), "INTERSECT"),
        "Union" -> (SqlSetOperator.Union(None), "UNION"),
        "Except" -> (SqlSetOperator.Except(None), "EXCEPT")
    )

class L1_Printer_Quantifier extends munit.FunSuite:
    import QuantifierFixture.*

    test("subquery quantifier tokens"):
        assertCoverage[SqlSubqueryQuantifier]("Any", "All")
        assertEquals(subqueryQuantifiers.keySet, Coverage.casesOf[SqlSubqueryQuantifier])

        for (name, (q, expected)) <- subqueryQuantifiers.toList.sortBy(_._1) do
            assertEquals(createSql(_.printSubqueryQuantifier(q)), expected, name)

    test("quantifier tokens"):
        assertCoverage[SqlQuantifier]("All", "Distinct", "UnsafeCustom")
        assertEquals(quantifiers.keySet + "UnsafeCustom", Coverage.casesOf[SqlQuantifier])

        for (name, (q, expected)) <- quantifiers.toList.sortBy(_._1) do
            assertEquals(createSql(_.printQuantifier(q)), expected, name)

    test("quantifier UnsafeCustom is space-joined"):
        assertEquals(
            createSql(_.printQuantifier(SqlQuantifier.UnsafeCustom(List(SqlUnsafeCustomToken.Keyword("CUSTOM"))))),
            "CUSTOM"
        )
        assertEquals(
            createSql(_.printQuantifier(SqlQuantifier.UnsafeCustom(
                List(SqlUnsafeCustomToken.Keyword("NOT"), SqlUnsafeCustomToken.Keyword("DISTINCT"))
            ))),
            "NOT DISTINCT"
        )

    test("set operator tokens"):
        assertCoverage[SqlSetOperator]("Intersect", "Union", "Except")
        assertEquals(setOperators.keySet, Coverage.casesOf[SqlSetOperator])

        for (name, (op, expected)) <- setOperators.toList.sortBy(_._1) do
            assertEquals(createSql(_.printSetOperator(op)), expected, name)

    test("set operator quantifier suffix"):
        val cases: List[(String, SqlSetOperator, String)] = List(
            ("UNION DISTINCT", SqlSetOperator.Union(Some(SqlQuantifier.Distinct)), "UNION DISTINCT"),
            ("UNION ALL", SqlSetOperator.Union(Some(SqlQuantifier.All)), "UNION ALL"),
            ("INTERSECT DISTINCT", SqlSetOperator.Intersect(Some(SqlQuantifier.Distinct)), "INTERSECT DISTINCT"),
            ("INTERSECT ALL", SqlSetOperator.Intersect(Some(SqlQuantifier.All)), "INTERSECT ALL"),
            ("EXCEPT DISTINCT", SqlSetOperator.Except(Some(SqlQuantifier.Distinct)), "EXCEPT DISTINCT"),
            ("EXCEPT ALL", SqlSetOperator.Except(Some(SqlQuantifier.All)), "EXCEPT ALL")
        )

        for (name, op, expected) <- cases do
            assertEquals(createSql(_.printSetOperator(op)), expected, name)

    test("set operator precedence"):
        assertEquals(SqlSetOperator.Intersect(None).precedence, 20)
        assertEquals(SqlSetOperator.Union(None).precedence, 10)
        assertEquals(SqlSetOperator.Except(None).precedence, 10)

    test("select item asterisk"):
        assertEquals(createSql(_.printSelectItem(SqlSelectItem.Asterisk(None))), "*")
        assertEquals(createSql(_.printSelectItem(SqlSelectItem.Asterisk(Some("t")))), """"t".*""")

    test("select item expression with alias"):
        assertCoverage[SqlSelectItem]("Asterisk", "Expr")
        assertEquals(createSql(_.printSelectItem(SqlSelectItem.Expr(col("a"), None))), """"a"""")
        assertEquals(createSql(_.printSelectItem(SqlSelectItem.Expr(col("a"), Some("x")))), """"a" AS "x"""")

    test("unsafe custom token keyword is emitted verbatim"):
        assertCoverage[SqlUnsafeCustomToken]("Keyword", "Expr")
        assertEquals(createSql(_.printUnsafeCustomTokens(List(SqlUnsafeCustomToken.Keyword("FOO")))), "FOO")
        assertEquals(createSql(_.printUnsafeCustomTokens(List(SqlUnsafeCustomToken.Keyword("a'b")))), "a'b")

    test("unsafe custom token expression delegates to printExpr"):
        assertEquals(createSql(_.printUnsafeCustomTokens(List(SqlUnsafeCustomToken.Expr(col("a"))))), """"a"""")

    test("unsafe custom tokens keep word boundaries apart"):
        assertEquals(
            createSql(_.printUnsafeCustomTokens(List(SqlUnsafeCustomToken.Keyword("NOT"), SqlUnsafeCustomToken.Keyword("NULL")))),
            "NOT NULL"
        )
        assertEquals(
            createSql(_.printUnsafeCustomTokens(List(SqlUnsafeCustomToken.Keyword("DECIMAL(10,"), SqlUnsafeCustomToken.Keyword("2)")))),
            "DECIMAL(10, 2)"
        )

    test("unsafe custom tokens glue punctuation to its operand"):
        assertEquals(
            createSql(_.printUnsafeCustomTokens(List(
                SqlUnsafeCustomToken.Keyword("MATCH("),
                SqlUnsafeCustomToken.Expr(col("a")),
                SqlUnsafeCustomToken.Keyword(")"),
                SqlUnsafeCustomToken.Keyword("AGAINST("),
                SqlUnsafeCustomToken.Expr(SqlExpr.StringLiteral("x")),
                SqlUnsafeCustomToken.Keyword(")")
            ))),
            """MATCH("a") AGAINST('x')"""
        )
        assertEquals(
            createSql(_.printUnsafeCustomTokens(List(
                SqlUnsafeCustomToken.Keyword("f("),
                SqlUnsafeCustomToken.Expr(col("a")),
                SqlUnsafeCustomToken.Keyword(","),
                SqlUnsafeCustomToken.Expr(col("b")),
                SqlUnsafeCustomToken.Keyword(")")
            ))),
            """f("a", "b")"""
        )
        assertEquals(
            createSql(_.printUnsafeCustomTokens(List(SqlUnsafeCustomToken.Keyword("f("), SqlUnsafeCustomToken.Keyword(")")))),
            "f()"
        )