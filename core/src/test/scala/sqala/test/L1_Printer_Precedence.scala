package sqala.test

import sqala.ast.expr.*
import sqala.ast.order.{SqlOrdering, SqlOrderingItem}
import sqala.ast.statement.{SqlQuery, SqlSelectItem}
import sqala.ast.table.SqlTable
import sqala.ast.token.SqlUnsafeCustomToken
import sqala.ast.window.SqlWindow
import sqala.parser.SqlParser
import sqala.util.NonEmptyList

object PrecedenceFixture:
    def col(n: String): SqlExpr = SqlExpr.Column(None, n)
    def num(n: Int): SqlExpr = SqlExpr.NumberLiteral(n)
    def str(s: String): SqlExpr = SqlExpr.StringLiteral(s)
    def nel[A](h: A, t: A*): NonEmptyList[A] = NonEmptyList(h, t.toList)

    val tokens: List[SqlUnsafeCustomToken] = SqlUnsafeCustomToken.Keyword("OP") :: Nil
    val path: SqlExpr = SqlExpr.StringLiteral("$.b")

    /** The parser wraps literals in InlinedLiteral; unwrap so ASTs compare equal. */
    def norm(e: SqlExpr): SqlExpr = e match
        case SqlExpr.InlinedLiteral(l) => norm(l)
        case SqlExpr.Binary(l, op, r) => SqlExpr.Binary(norm(l), op, norm(r))
        case SqlExpr.Unary(op, x) => SqlExpr.Unary(op, norm(x))
        case SqlExpr.Tuple(items) => SqlExpr.Tuple(items.map(norm))
        case SqlExpr.In(x, in, n) =>
            val rhs = in match
                case SqlInRightOperand.Values(items) => SqlInRightOperand.Values(items.map(norm))
                case other => other
            SqlExpr.In(norm(x), rhs, n)
        case SqlExpr.Between(x, s, e2, n) => SqlExpr.Between(norm(x), norm(s), norm(e2), n)
        case SqlExpr.Like(x, p, esc, n) => SqlExpr.Like(norm(x), norm(p), esc.map(norm), n)
        case SqlExpr.SimilarTo(x, p, esc, n) => SqlExpr.SimilarTo(norm(x), norm(p), esc.map(norm), n)
        case SqlExpr.JsonTest(x, t, u, n) => SqlExpr.JsonTest(norm(x), t, u, n)
        case SqlExpr.QuantifiedComparisonPredicate(x, op, q, query) =>
            SqlExpr.QuantifiedComparisonPredicate(norm(x), op, q, query)
        case SqlExpr.Cast(x, t) => SqlExpr.Cast(norm(x), t)
        case SqlExpr.GeneralFunc(q, n, args, w, o, f) =>
            SqlExpr.GeneralFunc(q, n, args.map(norm), w, o, f)
        case SqlExpr.Window(x, w) => SqlExpr.Window(norm(x), w)
        case other => other

    /**
     * `Is` is excluded: SQL only allows NULL/TRUE/FALSE/DISTINCT FROM/JSON after IS,
     * so an arbitrary operand is not valid SQL and the parser rightly rejects it.
     */
    val binaryOps: List[(String, SqlBinaryOperator)] = List(
        "Times" -> SqlBinaryOperator.Times,
        "Div" -> SqlBinaryOperator.Div,
        "Plus" -> SqlBinaryOperator.Plus,
        "Minus" -> SqlBinaryOperator.Minus,
        "Concat" -> SqlBinaryOperator.Concat,
        "Equal" -> SqlBinaryOperator.Equal,
        "NotEqual" -> SqlBinaryOperator.NotEqual,
        "IsDistinctFrom" -> SqlBinaryOperator.IsDistinctFrom(false),
        "GreaterThan" -> SqlBinaryOperator.GreaterThan,
        "GreaterThanEqual" -> SqlBinaryOperator.GreaterThanEqual,
        "LessThan" -> SqlBinaryOperator.LessThan,
        "LessThanEqual" -> SqlBinaryOperator.LessThanEqual,
        "Overlaps" -> SqlBinaryOperator.Overlaps,
        "And" -> SqlBinaryOperator.And,
        "Or" -> SqlBinaryOperator.Or
    )

    /** `SELECT "b" FROM "u"`, literal-free so reparsing cannot introduce wrappers. */
    val sub: SqlQuery.Select = SqlQuery.Select(
        None,
        nel(SqlSelectItem.Expr(col("b"), None)),
        List(SqlTable.Ident("u", None, None, None, None)),
        None, None, None, Nil, None, Nil, None, None
    )

    /** Operands that are valid standalone SQL, so a parse failure means a real defect. */
    val allChildren: List[(String, SqlExpr)] =
        val a = col("a")
        val b = col("b")
        List(
            "Binary(Plus)" -> SqlExpr.Binary(a, SqlBinaryOperator.Plus, b),
            "Binary(Times)" -> SqlExpr.Binary(a, SqlBinaryOperator.Times, b),
            "Binary(Equal)" -> SqlExpr.Binary(a, SqlBinaryOperator.Equal, b),
            "Binary(And)" -> SqlExpr.Binary(a, SqlBinaryOperator.And, b),
            "Binary(Or)" -> SqlExpr.Binary(a, SqlBinaryOperator.Or, b),
            "Binary(Concat)" -> SqlExpr.Binary(a, SqlBinaryOperator.Concat, b),
            "Unary(Not)" -> SqlExpr.Unary(SqlUnaryOperator.Not, a),
            "Unary(Negative)" -> SqlExpr.Unary(SqlUnaryOperator.Negative, a),
            "In" -> SqlExpr.In(a, SqlInRightOperand.Values(nel(num(1), num(2))), false),
            "Between" -> SqlExpr.Between(a, num(1), num(2), false),
            "Like" -> SqlExpr.Like(a, str("s"), None, false),
            "SimilarTo" -> SqlExpr.SimilarTo(a, str("s"), None, false),
            "JsonTest" -> SqlExpr.JsonTest(a, None, None, false),
            "Tuple" -> SqlExpr.Tuple(nel(a, b)),
            "QuantifiedComparison" -> SqlExpr.QuantifiedComparisonPredicate(
                a, SqlQuantifiedComparisonOperator.Equal, SqlSubqueryQuantifier.Any, sub
            ),
            "Exists" -> SqlExpr.ExistsPredicate(sub),
            "Subquery" -> SqlExpr.Subquery(sub),
            "Cast" -> SqlExpr.Cast(a, SqlType.Int),
            "Func" -> SqlExpr.GeneralFunc(None, "ABS", List(a), Nil, Nil, None),
            "Window" -> SqlExpr.Window(
                SqlExpr.GeneralFunc(None, "SUM", List(a), Nil, Nil, None),
                SqlWindow.Inlined(None, Nil, List(SqlOrderingItem(b, Some(SqlOrdering.Asc), None)), None)
            )
        )

    def childrenNamed(names: String*): List[(String, SqlExpr)] =
        allChildren.filter((n, _) => names.contains(n))

    /**
     * Each entry builds a predicate around a subject expression. `Between` is
     * handled separately because its three operands use two different boundaries.
     */
    val predicates: List[(String, SqlExpr => SqlExpr)] = List(
        "In" -> (s => SqlExpr.In(s, SqlInRightOperand.Values(nel(num(1), num(2))), false)),
        "NotIn" -> (s => SqlExpr.In(s, SqlInRightOperand.Values(nel(num(1), num(2))), true)),
        "Like" -> (s => SqlExpr.Like(s, str("s"), None, false)),
        "NotLike" -> (s => SqlExpr.Like(s, str("s"), None, true)),
        "SimilarTo" -> (s => SqlExpr.SimilarTo(s, str("s"), None, false)),
        "NotSimilarTo" -> (s => SqlExpr.SimilarTo(s, str("s"), None, true)),
        "JsonTest" -> (s => SqlExpr.JsonTest(s, None, None, false)),
        "NotJsonTest" -> (s => SqlExpr.JsonTest(s, None, None, true)),
        "JsonTestObject" -> (s => SqlExpr.JsonTest(s, Some(SqlJsonNodeType.Object), Some(SqlJsonUniquenessMode.With), false)),
        "Quantified" -> (s => SqlExpr.QuantifiedComparisonPredicate(
            s, SqlQuantifiedComparisonOperator.Equal, SqlSubqueryQuantifier.Any, sub
        )),
        "Between" -> (s => SqlExpr.Between(s, num(1), num(2), false)),
        "NotBetween" -> (s => SqlExpr.Between(s, num(1), num(2), true))
    )

    /** Print each combination, reparse it, and compare against the original AST. */
    def check(children: List[(String, SqlExpr)]): (Int, List[String]) =
        val failures = List.newBuilder[String]
        var checked = 0

        for (childName, child) <- children; (opName, op) <- binaryOps do
            for (side, expr) <- List(
                "left" -> SqlExpr.Binary(child, op, col("z")),
                "right" -> SqlExpr.Binary(col("z"), op, child)
            ) do
                checked += 1
                checkPair(s"$childName as $side of $opName", expr, failures)

        (checked, failures.result())

    def checkPair(label: String, expr: SqlExpr, failures: scala.collection.mutable.Builder[String, List[String]]): Unit =
        val printed = createSql(_.printExpr(expr))
        try
            val back = norm(SqlParser.parseExpr(printed))
            if back != norm(expr) then
                failures += s"$label\n    printed:  $printed\n    expected: ${norm(expr)}\n    reparsed: $back"
        catch
            case ex: Exception =>
                failures += s"$label\n    printed: $printed\n    parse failed: ${ex.getMessage}"

    def report(checked: Int, failures: List[String]): String =
        s"$checked combinations checked, ${failures.size} failed:\n\n${failures.mkString("\n\n")}"

    /** Fixture labels carry a variant suffix; strip it to recover the enum case name. */
    def caseName(label: String): String =
        label.takeWhile(_ != '(') match
            case "QuantifiedComparison" => "QuantifiedComparisonPredicate"
            case "Exists" => "ExistsPredicate"
            case "Func" => "GeneralFunc"
            case other => other

    /** Every SqlExpr case, built with the smallest valid arguments. */
    val all: List[(String, SqlExpr)] = allChildren ++ List(
        "Column" -> col("a"),
        "NullLiteral" -> SqlExpr.NullLiteral,
        "StringLiteral" -> SqlExpr.StringLiteral("s"),
        "NumberLiteral" -> SqlExpr.NumberLiteral(1),
        "BooleanLiteral" -> SqlExpr.BooleanLiteral(true),
        "TimeLiteral" -> SqlExpr.TimeLiteral(SqlTimeType.Date, "2020-01-01"),
        "IntervalLiteral" -> SqlExpr.IntervalLiteral("1", SqlIntervalField.Single(SqlTimeUnit.Day)),
        "InlinedLiteral" -> SqlExpr.InlinedLiteral(SqlExpr.StringLiteral("s")),
        "Array" -> SqlExpr.Array(List(col("a"))),
        "Case" -> SqlExpr.Case(nel(SqlCaseBranch(col("a"), col("a"))), None),
        "SimpleCase" -> SqlExpr.SimpleCase(col("a"), nel(SqlCaseBranch(col("a"), col("a"))), None),
        "Coalesce" -> SqlExpr.Coalesce(nel(col("a"))),
        "NullIf" -> SqlExpr.NullIf(col("a"), col("a")),
        "Grouping" -> SqlExpr.Grouping(nel(col("a"))),
        "IdentFunc" -> SqlExpr.IdentFunc("CURRENT_DATE"),
        "SubstringFunc" -> SqlExpr.SubstringFunc(col("a"), SqlExpr.NumberLiteral(1), None),
        "TrimFunc" -> SqlExpr.TrimFunc(Some(SqlTrim(Some(SqlTrimMode.Both), None)), col("a")),
        "OverlayFunc" -> SqlExpr.OverlayFunc(col("a"), SqlExpr.StringLiteral("s"), SqlExpr.NumberLiteral(1), None),
        "PositionFunc" -> SqlExpr.PositionFunc(SqlExpr.StringLiteral("s"), col("a")),
        "ExtractFunc" -> SqlExpr.ExtractFunc(SqlTimeUnit.Year, col("a")),
        "CountAsteriskFunc" -> SqlExpr.CountAsteriskFunc(None, None),
        "ListAggFunc" -> SqlExpr.ListAggFunc(None, col("a"), SqlExpr.StringLiteral(","), None, Nil, None),
        "NullsTreatmentFunc" -> SqlExpr.NullsTreatmentFunc("LAG", List(col("a")), None),
        "NthValueFunc" -> SqlExpr.NthValueFunc(col("a"), SqlExpr.NumberLiteral(2), None, None),
        "GeneralFunc" -> SqlExpr.GeneralFunc(None, "ABS", List(col("a")), Nil, Nil, None),
        "MatchPhase" -> SqlExpr.MatchPhase(SqlMatchPhase.Final, col("a")),
        "JsonSerializeFunc" -> SqlExpr.JsonSerializeFunc(col("a"), None),
        "JsonParseFunc" -> SqlExpr.JsonParseFunc(col("a"), None, None),
        "JsonQueryFunc" -> SqlExpr.JsonQueryFunc(col("a"), path, Nil, None, None, None, None, None),
        "JsonValueFunc" -> SqlExpr.JsonValueFunc(col("a"), path, Nil, None, None, None),
        "JsonObjectFunc" -> SqlExpr.JsonObjectFunc(List(SqlJsonObjectItem(col("a"), col("a"))), None, None, None),
        "JsonArrayFunc" -> SqlExpr.JsonArrayFunc(List(SqlJsonArrayItem(col("a"), None)), None, None),
        "JsonExistsFunc" -> SqlExpr.JsonExistsFunc(col("a"), path, Nil, None),
        "JsonObjectAggFunc" -> SqlExpr.JsonObjectAggFunc(SqlJsonObjectItem(col("a"), col("a")), None, None, None, None),
        "JsonArrayAggFunc" -> SqlExpr.JsonArrayAggFunc(SqlJsonArrayItem(col("a"), None), Nil, None, None, None),
        "UnsafeCustom" -> SqlExpr.UnsafeCustom(tokens)
    )

class L1_Printer_Precedence extends munit.FunSuite:
    import PrecedenceFixture.*

    test("unary operator precedences"):
        assertEquals(SqlUnaryOperator.Positive.precedence, 70)
        assertEquals(SqlUnaryOperator.Negative.precedence, 70)
        assertEquals(SqlUnaryOperator.Not.precedence, 25)
        assertEquals(SqlUnaryOperator.UnsafeCustom(tokens).precedence, 0)

    test("NOT sits between AND and the comparison tier"):
        assert(SqlBinaryOperator.And.precedence < SqlUnaryOperator.Not.precedence)
        assert(SqlUnaryOperator.Not.precedence < SqlBinaryOperator.Equal.precedence)

    test("unary sign binds tighter than multiplication"):
        assert(SqlUnaryOperator.Negative.precedence > SqlBinaryOperator.Times.precedence)

    test("quantified comparison operator precedences"):
        assertEquals(SqlQuantifiedComparisonOperator.Equal.precedence, SqlBinaryOperator.Equal.precedence)
        assertEquals(SqlQuantifiedComparisonOperator.NotEqual.precedence, SqlBinaryOperator.Equal.precedence)
        assertEquals(SqlQuantifiedComparisonOperator.GreaterThan.precedence, SqlBinaryOperator.Equal.precedence)
        assertEquals(SqlQuantifiedComparisonOperator.GreaterThanEqual.precedence, SqlBinaryOperator.Equal.precedence)
        assertEquals(SqlQuantifiedComparisonOperator.LessThan.precedence, SqlBinaryOperator.Equal.precedence)
        assertEquals(SqlQuantifiedComparisonOperator.LessThanEqual.precedence, SqlBinaryOperator.Equal.precedence)
        assertEquals(SqlQuantifiedComparisonOperator.UnsafeCustom(tokens).precedence, 0)

    test("binary and unary expressions inherit their operator precedence"):
        for op <- List(
            SqlBinaryOperator.Times, SqlBinaryOperator.Div, SqlBinaryOperator.Plus,
            SqlBinaryOperator.Minus, SqlBinaryOperator.Concat, SqlBinaryOperator.Equal,
            SqlBinaryOperator.NotEqual, SqlBinaryOperator.GreaterThan, SqlBinaryOperator.And,
            SqlBinaryOperator.Or, SqlBinaryOperator.Is(true), SqlBinaryOperator.IsDistinctFrom(false),
            SqlBinaryOperator.UnsafeCustom(tokens)
        ) do
            assertEquals(SqlExpr.Binary(col("a"), op, col("a")).precedence, op.precedence, op.toString)

        for op <- List(
            SqlUnaryOperator.Positive, SqlUnaryOperator.Negative,
            SqlUnaryOperator.Not, SqlUnaryOperator.UnsafeCustom(tokens)
        ) do
            assertEquals(SqlExpr.Unary(op, col("a")).precedence, op.precedence, op.toString)

    test("quantified predicates inherit their operator precedence"):
        for op <- List(
            SqlQuantifiedComparisonOperator.Equal,
            SqlQuantifiedComparisonOperator.LessThan,
            SqlQuantifiedComparisonOperator.UnsafeCustom(tokens)
        ) do
            val e = SqlExpr.QuantifiedComparisonPredicate(col("a"), op, SqlSubqueryQuantifier.Any, QueryFixture.simple)
            assertEquals(e.precedence, op.precedence, op.toString)

    test("the predicate tier is exactly these seven cases"):
        val predicates = all
            .filter((n, e) => e.precedence == SqlBinaryOperator.Equal.precedence &&
                caseName(n) != "Binary" && caseName(n) != "Unary")
            .map((n, _) => caseName(n))
        assertEquals(
            predicates.sorted,
            List("Between", "ExistsPredicate", "In", "JsonTest", "Like",
                "QuantifiedComparisonPredicate", "SimilarTo").sorted
        )

    test("every other expression kind is atomic"):
        val nonAtomic = Set(
            "Binary(Plus)", "Binary(Times)", "Binary(Equal)", "Binary(And)", "Binary(Or)", "Binary(Concat)",
            "Unary(Not)", "Unary(Negative)", "In", "Between", "Like", "SimilarTo", "JsonTest",
            "QuantifiedComparison", "Exists"
        )
        val wrong = all.filterNot((n, _) => nonAtomic.contains(n)).filterNot((_, e) => e.precedence == 100)
        assertEquals(wrong.map((n, _) => n), Nil)

    test("a self-parenthesizing custom expression is atomic, its operator is not"):
        assertEquals(SqlExpr.UnsafeCustom(tokens).precedence, 100)
        assertEquals(SqlBinaryOperator.UnsafeCustom(tokens).precedence, 0)
        assertEquals(SqlUnaryOperator.UnsafeCustom(tokens).precedence, 0)

    test("every SqlExpr case has a precedence assignment"):
        Coverage.assertCoverage[SqlExpr](all.map((n, _) => caseName(n))*)

    test("every unary and quantified operator case has a precedence"):
        Coverage.assertCoverage[SqlUnaryOperator]("Positive", "Negative", "Not", "UnsafeCustom")
        Coverage.assertCoverage[SqlQuantifiedComparisonOperator](
            "Equal", "NotEqual", "GreaterThan", "GreaterThanEqual", "LessThan", "LessThanEqual", "UnsafeCustom"
        )

    test("baseline: nested binary, unary minus and tuple operands round-trip"):
        val (checked, failures) = check(childrenNamed(
            "Binary(Plus)", "Binary(Times)", "Binary(Equal)", "Binary(And)", "Binary(Or)",
            "Binary(Concat)", "Unary(Negative)", "Tuple"
        ))
        assert(failures.isEmpty, report(checked, failures))

    test("atomic operands round-trip without extra parentheses"):
        val (checked, failures) = check(childrenNamed("Cast", "Func", "Window", "Subquery"))
        assert(failures.isEmpty, report(checked, failures))

    // Regression: Between and JsonTest used to fall through to
    // `case _ => false`, so `Between(a,1,2) + 1` printed as `a BETWEEN 1 AND 2 + 1`
    // and reparsed as `a BETWEEN 1 AND (2 + 1)` - a silent change of meaning.
    test("regression: Between and JsonTest operands are parenthesized"):
        val (checked, failures) = check(childrenNamed("Between", "JsonTest"))
        assert(failures.isEmpty, report(checked, failures))

    // Regression: the left-hand rule used `<` instead of `<=`, so an
    // In/Like/SimilarTo operand of a comparison at the same precedence got no
    // parentheses: `a IN (1,2) = z` instead of `(a IN (1,2)) = z`.
    test("regression: In/Like/SimilarTo as left operand of a comparison"):
        val (checked, failures) = check(childrenNamed("In", "Like", "SimilarTo"))
        assert(failures.isEmpty, report(checked, failures))

    // Regression: printUnaryExpr parenthesizes only the operand, so
    // `NOT(a) * z` printed as `NOT("a") * "z"` and reparsed as NOT(a * z).
    // The outer parentheses must come from the parent: `(NOT("a")) * "z"`.
    test("regression: Unary(Not) operand gets outer parentheses"):
        val (checked, failures) = check(childrenNamed("Unary(Not)"))
        assert(failures.isEmpty, report(checked, failures))

    test("subquery predicates as operands round-trip"):
        val (checked, failures) = check(childrenNamed("QuantifiedComparison", "Exists"))
        assert(failures.isEmpty, report(checked, failures))

    test("every parseable operand kind survives the full operator matrix"):
        val (checked, failures) = check(allChildren)
        assert(failures.isEmpty, report(checked, failures))

    test("every subject kind round-trips through every predicate"):
        val failures = List.newBuilder[String]
        var checked = 0
        for (subjectName, subject) <- allChildren; (predName, build) <- predicates do
            checked += 1
            checkPair(s"$subjectName as subject of $predName", build(subject), failures)
        val fs = failures.result()
        assert(fs.isEmpty, s"$checked combinations checked, ${fs.size} failed:\n\n${fs.mkString("\n\n")}")

    // Regression: the bounds were printed with the AND boundary, but the parser
    // reads them with parseConcat, so anything at the NOT or comparison tier was
    // left bare and then rejected: `"a" BETWEEN "a" = "b" AND 9`.
    test("Between start and end operands round-trip"):
        val failures = List.newBuilder[String]
        var checked = 0
        for (name, child) <- allChildren do
            for (position, expr) <- List(
                "start" -> SqlExpr.Between(col("a"), child, num(9), false),
                "end" -> SqlExpr.Between(col("a"), num(0), child, false)
            ) do
                checked += 1
                checkPair(s"$name as $position of Between", expr, failures)
        val fs = failures.result()
        assert(fs.isEmpty, s"$checked combinations checked, ${fs.size} failed:\n\n${fs.mkString("\n\n")}")

    test("Like and SimilarTo pattern operands round-trip"):
        val failures = List.newBuilder[String]
        var checked = 0
        for (name, child) <- allChildren do
            for (position, expr) <- List(
                "Like" -> SqlExpr.Like(col("a"), child, None, false),
                "SimilarTo" -> SqlExpr.SimilarTo(col("a"), child, None, false)
            ) do
                checked += 1
                checkPair(s"$name as pattern of $position", expr, failures)
        val fs = failures.result()
        assert(fs.isEmpty, s"$checked combinations checked, ${fs.size} failed:\n\n${fs.mkString("\n\n")}")

    test("a comparison subject is parenthesized, not swallowed"):
        val subject = SqlExpr.Binary(col("a"), SqlBinaryOperator.Equal, col("b"))
        assertEquals(
            createSql(_.printExpr(SqlExpr.In(subject, SqlInRightOperand.Values(nel(num(1))), false))),
            """("a" = "b") IN (1)"""
        )
        assertEquals(
            createSql(_.printExpr(SqlExpr.JsonTest(subject, None, None, false))),
            """("a" = "b") IS JSON"""
        )
        assertEquals(
            createSql(_.printExpr(SqlExpr.Between(subject, num(1), num(2), false))),
            """("a" = "b") BETWEEN 1 AND 2"""
        )

    test("a NOT subject is parenthesized, not extended to the right"):
        val subject = SqlExpr.Unary(SqlUnaryOperator.Not, col("a"))
        assertEquals(
            createSql(_.printExpr(SqlExpr.In(subject, SqlInRightOperand.Values(nel(num(1))), false))),
            """(NOT("a")) IN (1)"""
        )
        assertEquals(
            createSql(_.printExpr(SqlExpr.JsonTest(subject, None, None, false))),
            """(NOT("a")) IS JSON"""
        )

    test("an arithmetic subject needs no parentheses"):
        val subject = SqlExpr.Binary(col("a"), SqlBinaryOperator.Plus, col("b"))
        assertEquals(
            createSql(_.printExpr(SqlExpr.In(subject, SqlInRightOperand.Values(nel(num(1))), false))),
            """"a" + "b" IN (1)"""
        )
        assertEquals(
            createSql(_.printExpr(SqlExpr.Between(subject, num(1), num(2), false))),
            """"a" + "b" BETWEEN 1 AND 2"""
        )

    test("Between bounds use the comparison boundary, not the AND boundary"):
        val comparison = SqlExpr.Binary(col("a"), SqlBinaryOperator.Equal, col("b"))
        val conjunction = SqlExpr.Binary(col("a"), SqlBinaryOperator.And, col("b"))
        assertEquals(
            createSql(_.printExpr(SqlExpr.Between(col("z"), comparison, num(2), false))),
            """"z" BETWEEN ("a" = "b") AND 2"""
        )
        assertEquals(
            createSql(_.printExpr(SqlExpr.Between(col("z"), conjunction, num(2), false))),
            """"z" BETWEEN ("a" AND "b") AND 2"""
        )

    test("an unknown-precedence operand is always parenthesized"):
        val custom = SqlExpr.Binary(col("a"), SqlBinaryOperator.UnsafeCustom(tokens), col("b"))
        assertEquals(custom.precedence, 0)
        assertEquals(
            createSql(_.printExpr(SqlExpr.Binary(custom, SqlBinaryOperator.Or, col("z")))),
            """("a" OP "b") OR "z""""
        )
        assertEquals(
            createSql(_.printExpr(SqlExpr.Between(col("z"), custom, num(2), false))),
            """"z" BETWEEN ("a" OP "b") AND 2"""
        )
        assertEquals(
            createSql(_.printExpr(SqlExpr.In(custom, SqlInRightOperand.Values(nel(num(1))), false))),
            """("a" OP "b") IN (1)"""
        )