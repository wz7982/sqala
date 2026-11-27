package sqala.test

import sqala.ast.expr.*
import sqala.ast.order.{SqlNullsOrdering, SqlOrdering}
import sqala.ast.quantifier.SqlQuantifier
import sqala.ast.window.{SqlWindowFrameExcludeMode, SqlWindowFrameUnit}
import sqala.ir.*
import sqala.test.IrFixture.*
import sqala.util.NonEmptyList

/**
 * Expression lowering: column resolution, NOT simplification, division
 * rewriting, IN folding, predicates, CASE, functions, windows, JSON,
 * aggregates, match phases, and unsafe custom tokens.
 */
class I1_Expr extends munit.FunSuite:
    test("native expressions and column references"):
        assertEquals(pgExpr(n(1)), lines("SELECT", """    1 AS "x""""))
        assertEquals(pgExpr(col("id")), lines("SELECT", """    "id" AS "x""""))
        assertEquals(
            pgExpr(col(IrTable.Ident("user", Nil), "id")),
            lines("SELECT", """    "t1"."id" AS "x"""")
        )
        assertEquals(
            pgExpr(IrExpr.Column(Some(IrTable.PatternVariable("M")), "x")),
            lines("SELECT", """    "M"."x" AS "x"""")
        )

    test("tuple and array lower to SQL row and array constructors"):
        assertEquals(
            pgExpr(IrExpr.Tuple(NonEmptyList(n(1), List(n(2), n(3))))),
            lines("SELECT", """    (1, 2, 3) AS "x"""")
        )
        assertEquals(
            pgExpr(IrExpr.Array(List(n(1), n(2)))),
            lines("SELECT", """    ARRAY[1, 2] AS "x"""")
        )

    test("not simplifies boolean literals"):
        assertEquals(
            pgExpr(IrExpr.Unary(SqlUnaryOperator.Not, b(true))),
            lines("SELECT", """    FALSE AS "x"""")
        )

    test("not flips comparison predicates"):
        assertEquals(
            pgExpr(IrExpr.Unary(SqlUnaryOperator.Not, IrExpr.Binary(col("a"), SqlBinaryOperator.Equal, col("b")))),
            lines("SELECT", """    "a" <> "b" AS "x"""")
        )
        assertEquals(
            pgExpr(IrExpr.Unary(SqlUnaryOperator.Not, IrExpr.Binary(col("a"), SqlBinaryOperator.NotEqual, col("b")))),
            lines("SELECT", """    "a" = "b" AS "x"""")
        )
        assertEquals(
            pgExpr(IrExpr.Unary(SqlUnaryOperator.Not, IrExpr.Binary(col("a"), SqlBinaryOperator.Is(false), IrExpr.Native(SqlExpr.NullLiteral)))),
            lines("SELECT", """    "a" IS NOT NULL AS "x"""")
        )

    test("not flips like in and between"):
        assertEquals(
            pgExpr(IrExpr.Unary(SqlUnaryOperator.Not, IrExpr.Like(col("a"), s("x%"), false))),
            lines("SELECT", """    "a" NOT LIKE 'x%' AS "x"""")
        )
        assertEquals(
            pgExpr(IrExpr.Unary(SqlUnaryOperator.Not, IrExpr.In(col("a"), IrInRightOperand.Values(List(n(1))), false))),
            lines("SELECT", """    "a" NOT IN (1) AS "x"""")
        )
        assertEquals(
            pgExpr(IrExpr.Unary(SqlUnaryOperator.Not, IrExpr.Between(col("a"), n(1), n(9), false))),
            lines("SELECT", """    "a" NOT BETWEEN 1 AND 9 AS "x"""")
        )

    test("double not is eliminated and other expressions are wrapped"):
        assertEquals(
            pgExpr(IrExpr.Unary(SqlUnaryOperator.Not, IrExpr.Unary(SqlUnaryOperator.Not, col("a")))),
            lines("SELECT", """    "a" AS "x"""")
        )
        assertEquals(
            pgExpr(IrExpr.Unary(SqlUnaryOperator.Not, IrExpr.Binary(col("a"), SqlBinaryOperator.Plus, col("b")))),
            lines("SELECT", """    NOT("a" + "b") AS "x"""")
        )
        assertEquals(
            pgExpr(IrExpr.Unary(SqlUnaryOperator.Not, IrExpr.Native(SqlExpr.StringLiteral("z")))),
            lines("SELECT", """    NOT('z') AS "x"""")
        )

    test("not flips json test"):
        assertEquals(
            pgExpr(IrExpr.Unary(SqlUnaryOperator.Not, IrExpr.JsonTest(col("j"), Some(SqlJsonNodeType.Value), false))),
            lines("SELECT", """    "j" IS NOT JSON VALUE AS "x"""")
        )
        assertEquals(
            pgExpr(IrExpr.JsonTest(col("j"), None, false)),
            lines("SELECT", """    "j" IS JSON AS "x"""")
        )

    test("division lowers both operands to decimal casts"):
        assertEquals(
            pgExpr(IrExpr.Binary(n(4), SqlBinaryOperator.Div, n(2))),
            lines("SELECT", """    CAST(4 AS DECIMAL(38, 10)) / CAST(2 AS DECIMAL(38, 10)) AS "x"""")
        )

    test("empty in lists fold to constant comparisons"):
        assertEquals(
            pgExpr(IrExpr.In(col("a"), IrInRightOperand.Values(Nil), false)),
            lines("SELECT", """    1 = 0 AS "x"""")
        )
        assertEquals(
            pgExpr(IrExpr.In(col("a"), IrInRightOperand.Values(Nil), true)),
            lines("SELECT", """    1 <> 0 AS "x"""")
        )

    test("in accepts values and subqueries and not is carried"):
        assertEquals(
            pgExpr(IrExpr.Like(col("a"), s("x%"), true)),
            lines("SELECT", """    "a" NOT LIKE 'x%' AS "x"""")
        )
        assertEquals(
            pgExpr(IrExpr.Between(col("a"), n(1), n(9), true)),
            lines("SELECT", """    "a" NOT BETWEEN 1 AND 9 AS "x"""")
        )
        assertEquals(
            pgExpr(IrExpr.In(col("a"), IrInRightOperand.Subquery(sel(List(IrSelectItem(n(1), "v")))), false)),
            lines(
                "SELECT",
                """    "a" IN (""",
                "        SELECT",
                """            1 AS "v"""",
                """    ) AS "x""""
            )
        )

    test("case coalesce nullif and cast lower recursively"):
        assertEquals(
            pgExpr(IrExpr.Case(NonEmptyList(IrCaseBranch(col("a"), b(true)), List(IrCaseBranch(col("b"), b(false)))), Some(s("d")))),
            lines("SELECT", """    CASE WHEN "a" THEN TRUE WHEN "b" THEN FALSE ELSE 'd' END AS "x"""")
        )
        assertEquals(
            pgExpr(IrExpr.Case(NonEmptyList(IrCaseBranch(col("a"), b(true)), Nil), None)),
            lines("SELECT", """    CASE WHEN "a" THEN TRUE END AS "x"""")
        )
        assertEquals(
            pgExpr(IrExpr.Coalesce(NonEmptyList(col("a"), List(col("b"))))),
            lines("SELECT", """    COALESCE("a", "b") AS "x"""")
        )
        assertEquals(
            pgExpr(IrExpr.NullIf(col("a"), s(""))),
            lines("SELECT", """    NULLIF("a", '') AS "x"""")
        )
        assertEquals(
            pgExpr(IrExpr.Cast(col("a"), SqlType.Int)),
            lines("SELECT", """    CAST("a" AS INTEGER) AS "x"""")
        )

    test("window lowers partition order and frame"):
        assertEquals(
            pgExpr(IrExpr.Window(
                IrExpr.GeneralFunc(None, "SUM", List(col("n")), Nil, Nil, None),
                List(col("a")),
                List(IrOrderingItem(col("id"), SqlOrdering.Asc, Some(SqlNullsOrdering.First))),
                Some(IrWindowFrame.Start(SqlWindowFrameUnit.Rows, IrWindowFrameBound.UnboundedPreceding, None))
            )),
            lines("SELECT", """    SUM("n") OVER (PARTITION BY "a" ORDER BY "id" ASC NULLS FIRST ROWS UNBOUNDED PRECEDING) AS "x"""")
        )
        assertEquals(
            pgExpr(IrExpr.Window(
                col("n"),
                Nil,
                List(IrOrderingItem(col("id"), SqlOrdering.Desc, None)),
                Some(IrWindowFrame.Between(
                    SqlWindowFrameUnit.Groups,
                    IrWindowFrameBound.Preceding(n(2)),
                    IrWindowFrameBound.Following(n(1)),
                    Some(SqlWindowFrameExcludeMode.Group)
                ))
            )),
            lines("SELECT", """    "n" OVER (ORDER BY "id" DESC GROUPS BETWEEN 2 PRECEDING AND 1 FOLLOWING EXCLUDE GROUP) AS "x"""")
        )

    test("subquery exists and quantified comparison lower their queries"):
        assertEquals(
            pgExpr(IrExpr.Subquery(sel(List(IrSelectItem(n(1), "one"))))),
            lines(
                "SELECT",
                "    (",
                "        SELECT",
                """            1 AS "one"""",
                """    ) AS "x""""
            )
        )
        assertEquals(
            pgExpr(IrExpr.ExistsPredicate(sel(List(IrSelectItem(n(1), "one"))))),
            lines(
                "SELECT",
                "    EXISTS(",
                "        SELECT",
                """            1 AS "one"""",
                """    ) AS "x""""
            )
        )
        assertEquals(
            pgExpr(IrExpr.QuantifiedComparisonPredicate(col("a"), SqlQuantifiedComparisonOperator.Equal, SqlSubqueryQuantifier.Any, sel(List(IrSelectItem(col("b"), "b"))))),
            lines(
                "SELECT",
                """    "a" = ANY(""",
                "        SELECT",
                """            "b" AS "b"""",
                """    ) AS "x""""
            )
        )

    test("grouping and ident functions lower directly"):
        assertEquals(
            pgExpr(IrExpr.Grouping(NonEmptyList(col("a"), Nil))),
            lines("SELECT", """    GROUPING("a") AS "x"""")
        )
        assertEquals(
            pgExpr(IrExpr.IdentFunc("CURRENT_TIMESTAMP")),
            lines("SELECT", """    CURRENT_TIMESTAMP AS "x"""")
        )

    test("special string and datetime functions"):
        assertEquals(
            pgExpr(IrExpr.SubstringFunc(col("a"), n(2), Some(n(3)))),
            lines("SELECT", """    SUBSTRING("a" FROM 2 FOR 3) AS "x"""")
        )
        assertEquals(
            pgExpr(IrExpr.SubstringFunc(col("a"), n(2), None)),
            lines("SELECT", """    SUBSTRING("a" FROM 2) AS "x"""")
        )
        assertEquals(
            pgExpr(IrExpr.OverlayFunc(col("a"), s("X"), n(2), None)),
            lines("SELECT", """    OVERLAY("a" PLACING 'X' FROM 2) AS "x"""")
        )
        assertEquals(
            pgExpr(IrExpr.PositionFunc(s("x"), col("a"))),
            lines("SELECT", """    POSITION('x' IN "a") AS "x"""")
        )
        assertEquals(
            pgExpr(IrExpr.ExtractFunc(SqlTimeUnit.Year, col("dt"))),
            lines("SELECT", """    EXTRACT(YEAR FROM "dt") AS "x"""")
        )

    test("json scalar functions"):
        assertEquals(
            pgExpr(IrExpr.JsonSerializeFunc(col("j"))),
            lines("SELECT", """    JSON_SERIALIZE("j") AS "x"""")
        )
        assertEquals(
            pgExpr(IrExpr.JsonParseFunc(col("t"))),
            lines("SELECT", """    JSON("t") AS "x"""")
        )
        assertEquals(
            pgExpr(IrExpr.JsonQueryFunc(col("j"), s("$.a"))),
            lines("SELECT", """    JSON_QUERY("j", '$.a') AS "x"""")
        )
        assertEquals(
            pgExpr(IrExpr.JsonValueFunc(col("j"), s("$.a"))),
            lines("SELECT", """    JSON_VALUE("j", '$.a') AS "x"""")
        )
        assertEquals(
            pgExpr(IrExpr.JsonExistsFunc(col("j"), s("$.a"))),
            lines("SELECT", """    JSON_EXISTS("j", '$.a') AS "x"""")
        )
        assertEquals(
            pgExpr(IrExpr.JsonObjectFunc(List(IrJsonObjectItem(s("k"), col("v"))))),
            lines("SELECT", """    JSON_OBJECT('k' VALUE "v") AS "x"""")
        )
        assertEquals(
            pgExpr(IrExpr.JsonArrayFunc(List(col("a"), col("b")))),
            lines("SELECT", """    JSON_ARRAY("a", "b") AS "x"""")
        )

    test("json aggregates carry order by and filter"):
        assertEquals(
            pgExpr(IrExpr.JsonObjectAggFunc(IrJsonObjectItem(s("k"), col("v")), Some(col("f")))),
            lines("SELECT", """    JSON_OBJECTAGG('k' VALUE "v") FILTER (WHERE "f") AS "x"""")
        )
        assertEquals(
            pgExpr(IrExpr.JsonArrayAggFunc(col("v"), List(IrOrderingItem(col("v"), SqlOrdering.Asc, None)), None)),
            lines("SELECT", """    JSON_ARRAYAGG("v" ORDER BY "v" ASC) AS "x"""")
        )

    test("aggregate functions lower filter quantifier and ordering"):
        assertEquals(
            pgExpr(IrExpr.CountAsteriskFunc(Some(col("f")))),
            lines("SELECT", """    COUNT(*) FILTER (WHERE "f") AS "x"""")
        )
        assertEquals(
            pgExpr(IrExpr.ListAggFunc(Some(SqlQuantifier.Distinct), col("a"), s(","), List(IrOrderingItem(col("a"), SqlOrdering.Asc, None)), Some(col("f")))),
            lines("SELECT", """    STRING_AGG(DISTINCT "a", ',' ORDER BY "a" ASC) FILTER (WHERE "f") AS "x"""")
        )
        assertEquals(
            pgExpr(IrExpr.NullsTreatmentFunc("FIRST_VALUE", List(col("a")), Some(SqlWindowNullsMode.Ignore))),
            lines("SELECT", """    FIRST_VALUE("a") IGNORE NULLS AS "x"""")
        )
        assertEquals(
            pgExpr(IrExpr.NthValueFunc(col("a"), n(2), Some(SqlNthValueFromMode.Last), Some(SqlWindowNullsMode.Ignore))),
            lines("SELECT", """    NTH_VALUE("a", 2) FROM LAST IGNORE NULLS AS "x"""")
        )
        assertEquals(
            pgExpr(IrExpr.GeneralFunc(Some(SqlQuantifier.Distinct), "FOO", List(col("a")), List(IrOrderingItem(col("a"), SqlOrdering.Asc, None)), Nil, None)),
            lines("SELECT", """    FOO(DISTINCT "a" ORDER BY "a" ASC) AS "x"""")
        )
        assertEquals(
            pgExpr(IrExpr.GeneralFunc(None, "BAR", Nil, Nil, List(IrOrderingItem(col("a"), SqlOrdering.Asc, None)), None)),
            lines("SELECT", """    BAR() WITHIN GROUP (ORDER BY "a" ASC) AS "x"""")
        )

    test("match phase wrappers and unary operators"):
        assertEquals(
            pgExpr(IrExpr.MatchPhase(SqlMatchPhase.Final, col("a"))),
            lines("SELECT", """    FINAL "a" AS "x"""")
        )
        assertEquals(
            pgExpr(IrExpr.MatchPhase(SqlMatchPhase.Running, col("a"))),
            lines("SELECT", """    RUNNING "a" AS "x"""")
        )
        assertEquals(
            pgExpr(IrExpr.Unary(SqlUnaryOperator.Negative, n(1))),
            lines("SELECT", """    -(1) AS "x"""")
        )

    test("unsafe custom tokens lower keywords and expressions"):
        assertEquals(
            pgExpr(IrExpr.UnsafeCustom(List(IrUnsafeToken.Keyword("SOME("), IrUnsafeToken.Expr(col("a")), IrUnsafeToken.Keyword(")")))),
            lines("SELECT", """    (SOME("a")) AS "x"""")
        )
