package sqala.test

import sqala.dynamic.dsl.*
import sqala.test.DynamicFixture.*

/**
 * Expression operators: comparisons, null predicates, membership,
 * ranges, arithmetic, boolean logic, the `NOT` simplification rules,
 * conditionals, intervals, and date part extraction.
 */
class D2_Expr extends munit.FunSuite:
    private def x(e: Expr): String =
        pg(select(e.as("x")))

    test("binary comparisons render their operator"):
        assertEquals(x(column("a") === column("b")), lines("SELECT", """    "a" = "b" AS "x""""))
        assertEquals(x(column("a") <> column("b")), lines("SELECT", """    "a" <> "b" AS "x""""))
        assertEquals(x(column("a") > column("b")), lines("SELECT", """    "a" > "b" AS "x""""))
        assertEquals(x(column("a") >= column("b")), lines("SELECT", """    "a" >= "b" AS "x""""))
        assertEquals(x(column("a") < column("b")), lines("SELECT", """    "a" < "b" AS "x""""))
        assertEquals(x(column("a") <= column("b")), lines("SELECT", """    "a" <= "b" AS "x""""))

    test("the null-safe operators render IS [NOT] DISTINCT FROM"):
        assertEquals(x(column("a") <=> value(1)), lines("SELECT", """    "a" IS NOT DISTINCT FROM 1 AS "x""""))
        assertEquals(x(!(column("a") <=> value(1))), lines("SELECT", """    "a" IS DISTINCT FROM 1 AS "x""""))

    test("isNull and its negation render IS [NOT] NULL"):
        assertEquals(x(column("a").isNull), lines("SELECT", """    "a" IS NULL AS "x""""))
        assertEquals(x(!column("a").isNull), lines("SELECT", """    "a" IS NOT NULL AS "x""""))

    test("in renders value lists, subqueries, and a false constant when empty"):
        assertEquals(x(column("s").in(value("a"), value("b"))), lines("SELECT", """    "s" IN ('a', 'b') AS "x""""))
        assertEquals(
            x(column("id").in(select(column("user_id").as("user_id")).from(table("post")))),
            lines(
                "SELECT",
                """    "id" IN (""",
                "        SELECT",
                """            "user_id" AS "user_id"""",
                "        FROM",
                """            "post"""",
                """    ) AS "x""""
            )
        )
        assertEquals(x(column("s").in(List[Expr]())), lines("SELECT", """    1 = 0 AS "x""""))

    test("between renders a closed range"):
        assertEquals(x(column("n").between(value(1), value(9))), lines("SELECT", """    "n" BETWEEN 1 AND 9 AS "x""""))

    test("quantified comparisons render ANY and ALL subqueries"):
        assertEquals(
            x(column("a") > any(select(column("b").as("b")).from(table("s")))),
            lines(
                "SELECT",
                """    "a" > ANY(""",
                "        SELECT",
                """            "b" AS "b"""",
                "        FROM",
                """            "s"""",
                """    ) AS "x""""
            )
        )
        assertEquals(
            x(column("a") <= all(select(column("b").as("b")).from(table("s")))),
            lines(
                "SELECT",
                """    "a" <= ALL(""",
                "        SELECT",
                """            "b" AS "b"""",
                "        FROM",
                """            "s"""",
                """    ) AS "x""""
            )
        )

    test("arithmetic renders the operator chain left to right"):
        assertEquals(x(column("a") + column("b") * column("c") / column("d") - column("e")), lines("SELECT", """    "a" + "b" * "c" / "d" - "e" AS "x""""))

    test("unary sign renders as a parenthesized prefix"):
        assertEquals(x(-(column("a"))), lines("SELECT", """    -("a") AS "x""""))
        assertEquals(x(+(column("a"))), lines("SELECT", """    +("a") AS "x""""))

    test("boolean logic renders AND and OR"):
        assertEquals(x((column("a") > value(1)) && (column("b") < value(2))), lines("SELECT", """    "a" > 1 AND "b" < 2 AS "x""""))
        assertEquals(x((column("a") > value(1)) || (column("b") < value(2))), lines("SELECT", """    "a" > 1 OR "b" < 2 AS "x""""))

    test("not simplifies boolean literals"):
        assertEquals(x(!(value(true))), lines("SELECT", """    FALSE AS "x""""))
        assertEquals(x(!(value(false))), lines("SELECT", """    TRUE AS "x""""))

    test("not flips IN instead of nesting"):
        assertEquals(x(!column("s").in(value("a"))), lines("SELECT", """    "s" NOT IN ('a') AS "x""""))
        assertEquals(x(!(column("s").in(List(value("a"))))), lines("SELECT", """    "s" NOT IN ('a') AS "x""""))

    test("not flips BETWEEN instead of nesting"):
        assertEquals(x(!column("n").between(value(1), value(9))), lines("SELECT", """    "n" NOT BETWEEN 1 AND 9 AS "x""""))

    test("not keeps a plain negation for non-flippable expressions"):
        assertEquals(x(!(column("a") === column("b"))), lines("SELECT", """    NOT("a" = "b") AS "x""""))

    test("caseWhen builds a searched case with an else branch"):
        assertEquals(
            x(caseWhen((column("a") > value(1)) -> value("hot"), (column("a") > value(0)) -> value("warm")).otherwise(value("cold"))),
            lines("SELECT", """    CASE WHEN "a" > 1 THEN 'hot' WHEN "a" > 0 THEN 'warm' ELSE 'cold' END AS "x"""")
        )

    test("caseWhen accepts the list overload"):
        assertEquals(
            x(caseWhen(List((column("a") > value(1)) -> value("hot"))).otherwise(value("cold"))),
            lines("SELECT", """    CASE WHEN "a" > 1 THEN 'hot' ELSE 'cold' END AS "x"""")
        )

    test("coalesce ifNull and nullIf render their functions"):
        assertEquals(
            pg(select(coalesce(column("a"), value(0)).as("x"), ifNull(column("a"), value(0)).as("y"), nullIf(column("a"), value(0)).as("z"))),
            lines(
                "SELECT",
                """    COALESCE("a", 0) AS "x",""",
                """    COALESCE("a", 0) AS "y",""",
                """    NULLIF("a", 0) AS "z""""
            )
        )

    test("grouping renders over a grouped column"):
        assertEquals(
            pg(select(grouping(column("a")).as("g")).from(table("t")).groupBy(column("a"))),
            lines(
                "SELECT",
                """    GROUPING("a") AS "g"""",
                "FROM",
                """    "t"""",
                "GROUP BY",
                """    "a""""
            )
        )

    test("subquery and exists embed queries as expressions"):
        assertEquals(
            x(subquery(select(value(1).as("one")))),
            lines(
                "SELECT",
                """    (""",
                "        SELECT",
                """            1 AS "one"""",
                """    ) AS "x""""
            )
        )
        assertEquals(
            pg(select(exists(select(column("id").as("id")).from(table("post")).where(column("post", "user_id") === column("user", "id"))).as("x")).from(table("user"))),
            lines(
                "SELECT",
                """    EXISTS(""",
                "        SELECT",
                """            "id" AS "id"""",
                "        FROM",
                """            "post"""",
                "        WHERE",
                """            "post"."user_id" = "user"."id"""",
                """    ) AS "x"""",
                "FROM",
                """    "user""""
            )
        )

    test("interval arithmetic renders single-unit interval literals"):
        assertEquals(
            pg(select((column("d") + inlinedValue(3.day)).as("x")).from(table("t"))),
            lines(
                "SELECT",
                """    "d" + INTERVAL '3' DAY AS "x"""",
                "FROM",
                """    "t""""
            )
        )
        assertEquals(
            pg(select((column("d") + value(1.hour)).as("x")).from(table("t"))),
            lines(
                "SELECT",
                """    "d" + INTERVAL '1' HOUR AS "x"""",
                "FROM",
                """    "t""""
            )
        )

    test("date part accessors render EXTRACT"):
        assertEquals(
            pg(select(column("t").year.as("y"), column("t").minute.as("m")).from(table("t"))),
            lines(
                "SELECT",
                """    EXTRACT(YEAR FROM "t") AS "y",""",
                """    EXTRACT(MINUTE FROM "t") AS "m"""",
                "FROM",
                """    "t""""
            )
        )
