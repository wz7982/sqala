package sqala.test

import sqala.ast.expr.{SqlBinaryOperator, SqlExpr, SqlQuantifiedComparisonOperator, SqlUnaryOperator}
import sqala.ast.token.SqlUnsafeCustomToken
import sqala.test.Coverage.assertCoverage

object OperatorFixture:
    def col(name: String): SqlExpr = SqlExpr.Column(None, name)

    def bin(left: SqlExpr, op: SqlBinaryOperator, right: SqlExpr): SqlExpr =
        SqlExpr.Binary(left, op, right)

    val binaryTokens: Map[String, (SqlBinaryOperator, String)] = Map(
        "Times" -> (SqlBinaryOperator.Times, "*"),
        "Div" -> (SqlBinaryOperator.Div, "/"),
        "Plus" -> (SqlBinaryOperator.Plus, "+"),
        "Minus" -> (SqlBinaryOperator.Minus, "-"),
        "Concat" -> (SqlBinaryOperator.Concat, "||"),
        "Equal" -> (SqlBinaryOperator.Equal, "="),
        "NotEqual" -> (SqlBinaryOperator.NotEqual, "<>"),
        "IsDistinctFrom" -> (SqlBinaryOperator.IsDistinctFrom(false), "IS DISTINCT FROM"),
        "Is" -> (SqlBinaryOperator.Is(false), "IS"),
        "GreaterThan" -> (SqlBinaryOperator.GreaterThan, ">"),
        "GreaterThanEqual" -> (SqlBinaryOperator.GreaterThanEqual, ">="),
        "LessThan" -> (SqlBinaryOperator.LessThan, "<"),
        "LessThanEqual" -> (SqlBinaryOperator.LessThanEqual, "<="),
        "Overlaps" -> (SqlBinaryOperator.Overlaps, "OVERLAPS"),
        "And" -> (SqlBinaryOperator.And, "AND"),
        "Or" -> (SqlBinaryOperator.Or, "OR")
    )

    val unaryTokens: Map[String, (SqlUnaryOperator, String)] = Map(
        "Positive" -> (SqlUnaryOperator.Positive, "+"),
        "Negative" -> (SqlUnaryOperator.Negative, "-"),
        "Not" -> (SqlUnaryOperator.Not, "NOT")
    )

    val quantifiedTokens: Map[String, (SqlQuantifiedComparisonOperator, String)] = Map(
        "Equal" -> (SqlQuantifiedComparisonOperator.Equal, "="),
        "NotEqual" -> (SqlQuantifiedComparisonOperator.NotEqual, "<>"),
        "GreaterThan" -> (SqlQuantifiedComparisonOperator.GreaterThan, ">"),
        "GreaterThanEqual" -> (SqlQuantifiedComparisonOperator.GreaterThanEqual, ">="),
        "LessThan" -> (SqlQuantifiedComparisonOperator.LessThan, "<"),
        "LessThanEqual" -> (SqlQuantifiedComparisonOperator.LessThanEqual, "<=")
    )

class L1_Printer_Operator extends munit.FunSuite:
    import OperatorFixture.*

    test("binary operator tokens"):
        assertCoverage[SqlBinaryOperator](
            "Times", "Div", "Plus", "Minus", "Concat", "Equal", "NotEqual",
            "IsDistinctFrom", "Is", "GreaterThan", "GreaterThanEqual", "LessThan",
            "LessThanEqual", "Overlaps", "And", "Or", "UnsafeCustom"
        )
        assertEquals(binaryTokens.keySet + "UnsafeCustom", Coverage.casesOf[SqlBinaryOperator])

        for (name, (op, expected)) <- binaryTokens.toList.sortBy(_._1) do
            assertEquals(createSql(_.printBinaryOperator(op)), expected, name)

    test("binary operator parameterized variants"):
        assertEquals(createSql(_.printBinaryOperator(SqlBinaryOperator.IsDistinctFrom(true))), "IS NOT DISTINCT FROM")
        assertEquals(createSql(_.printBinaryOperator(SqlBinaryOperator.Is(true))), "IS NOT")

    test("unary operator tokens"):
        assertCoverage[SqlUnaryOperator]("Positive", "Negative", "Not", "UnsafeCustom")
        assertEquals(unaryTokens.keySet + "UnsafeCustom", Coverage.casesOf[SqlUnaryOperator])

        for (name, (op, expected)) <- unaryTokens.toList.sortBy(_._1) do
            assertEquals(createSql(_.printUnaryOperator(op)), expected, name)

    test("precedence: parentheses required by SQL semantics"):
        val a = col("a")
        val b = col("b")
        val c = col("c")

        val cases: List[(String, SqlExpr, String)] = List(
            ("(a + b) * c", bin(bin(a, SqlBinaryOperator.Plus, b), SqlBinaryOperator.Times, c), """("a" + "b") * "c""""),
            ("a + b * c", bin(a, SqlBinaryOperator.Plus, bin(b, SqlBinaryOperator.Times, c)), """"a" + "b" * "c""""),
            ("a * b + c", bin(bin(a, SqlBinaryOperator.Times, b), SqlBinaryOperator.Plus, c), """"a" * "b" + "c""""),
            ("a - (b - c)", bin(a, SqlBinaryOperator.Minus, bin(b, SqlBinaryOperator.Minus, c)), """"a" - ("b" - "c")"""),
            ("(a - b) - c", bin(bin(a, SqlBinaryOperator.Minus, b), SqlBinaryOperator.Minus, c), """"a" - "b" - "c""""),
            ("(a OR b) AND c", bin(bin(a, SqlBinaryOperator.Or, b), SqlBinaryOperator.And, c), """("a" OR "b") AND "c""""),
            ("a AND (b OR c)", bin(a, SqlBinaryOperator.And, bin(b, SqlBinaryOperator.Or, c)), """"a" AND ("b" OR "c")"""),
            ("a OR b AND c", bin(a, SqlBinaryOperator.Or, bin(b, SqlBinaryOperator.And, c)), """"a" OR "b" AND "c""""),
            ("a = b AND c", bin(bin(a, SqlBinaryOperator.Equal, b), SqlBinaryOperator.And, c), """"a" = "b" AND "c""""),
            ("a = (b = c)", bin(a, SqlBinaryOperator.Equal, bin(b, SqlBinaryOperator.Equal, c)), """"a" = ("b" = "c")"""),
            ("(a = b) = c", bin(bin(a, SqlBinaryOperator.Equal, b), SqlBinaryOperator.Equal, c), """("a" = "b") = "c"""")
        )

        for (name, expr, expected) <- cases do
            assertEquals(createSql(_.printExpr(expr)), expected, name)

    test("UnsafeCustom operator emits bare tokens"):
        val tokens = List(SqlUnsafeCustomToken.Keyword("CUSTOM"))
        assertEquals(createSql(_.printBinaryOperator(SqlBinaryOperator.UnsafeCustom(tokens))), "CUSTOM")
        assertEquals(createSql(_.printUnaryOperator(SqlUnaryOperator.UnsafeCustom(tokens))), "CUSTOM")

    // The right operand keeps parentheses at equal precedence. This is a deliberate
    // defence: `-` and `/` are not associative, so `a - b - c` and `a - (b - c)` can
    // differ. Keeping the parentheses also preserves the AST across a reparse, since
    // the parser is left-associative.
    test("equal-precedence right operand keeps parentheses"):
        val a = col("a")
        val b = col("b")
        val c = col("c")

        assertEquals(
            createSql(_.printExpr(bin(a, SqlBinaryOperator.Plus, bin(b, SqlBinaryOperator.Plus, c)))),
            """"a" + ("b" + "c")"""
        )
        assertEquals(
            createSql(_.printExpr(bin(a, SqlBinaryOperator.Times, bin(b, SqlBinaryOperator.Times, c)))),
            """"a" * ("b" * "c")"""
        )
        assertEquals(
            createSql(_.printExpr(bin(bin(a, SqlBinaryOperator.Plus, b), SqlBinaryOperator.Plus, c))),
            """"a" + "b" + "c""""
        )

    test("quantified comparison operator tokens"):
        assertCoverage[SqlQuantifiedComparisonOperator](
            "Equal", "NotEqual", "GreaterThan", "GreaterThanEqual",
            "LessThan", "LessThanEqual", "UnsafeCustom"
        )
        assertEquals(quantifiedTokens.keySet + "UnsafeCustom", Coverage.casesOf[SqlQuantifiedComparisonOperator])

        for (name, (op, expected)) <- quantifiedTokens.toList.sortBy(_._1) do
            assertEquals(createSql(_.printQuantifiedCompareOperator(op)), expected, name)

        assertEquals(
            createSql(_.printQuantifiedCompareOperator(
                SqlQuantifiedComparisonOperator.UnsafeCustom(List(SqlUnsafeCustomToken.Keyword("CUSTOM")))
            )),
            "CUSTOM"
        )

    // Regression: printQuantifiedCompareOperator used printList(tokens) and
    // inherited the default ", " separator, while every other UnsafeCustom site
    // passes " ". Multi-token custom operators came out comma-joined.
    test("regression: multi-token custom quantified operator is space-joined"):
        val tokens = List(SqlUnsafeCustomToken.Keyword("NOT"), SqlUnsafeCustomToken.Keyword("LIKE"))
        assertEquals(
            createSql(_.printQuantifiedCompareOperator(SqlQuantifiedComparisonOperator.UnsafeCustom(tokens))),
            "NOT LIKE"
        )