package sqala.test

import sqala.static.dsl.*
import sqala.test.StaticFixture.*

/**
 * Ranking window functions: `RANK`, `DENSE_RANK`, `PERCENT_RANK`,
 * `CUME_DIST`, `ROW_NUMBER` and `NTILE`. Covers the generated sql,
 * the inferred result type, and the argument rules of `NTILE`.
 */
class W1_WindowRank extends munit.FunSuite:
    test("ranking functions print the expected sql"):
        assertEquals(
            sql(from(Wide).map(w => (
                a = rank().over(partitionBy(w.id).sortBy(w.score.desc)),
                b = denseRank().over(partitionBy(w.id).sortBy(w.score.desc)),
                c = percentRank().over(partitionBy(w.id).sortBy(w.score.desc)),
                d = cumeDist().over(partitionBy(w.id).sortBy(w.score.desc)),
                e = rowNumber().over(partitionBy(w.id).sortBy(w.score.desc)),
                f = ntile(4).over(partitionBy(w.id).sortBy(w.score.desc))
            ))),
            lines(
                "SELECT",
                """    RANK() OVER (PARTITION BY "t1"."id" ORDER BY "t1"."score" DESC) AS "c1",""",
                """    DENSE_RANK() OVER (PARTITION BY "t1"."id" ORDER BY "t1"."score" DESC) AS "c2",""",
                """    PERCENT_RANK() OVER (PARTITION BY "t1"."id" ORDER BY "t1"."score" DESC) AS "c3",""",
                """    CUME_DIST() OVER (PARTITION BY "t1"."id" ORDER BY "t1"."score" DESC) AS "c4",""",
                """    ROW_NUMBER() OVER (PARTITION BY "t1"."id" ORDER BY "t1"."score" DESC) AS "c5",""",
                """    NTILE(4) OVER (PARTITION BY "t1"."id" ORDER BY "t1"."score" DESC) AS "c6"""",
                "FROM",
                """    "wide" AS "t1""""
            )
        )

    test("ranking functions work without a partition clause"):
        assertEquals(
            sql(from(Wide).map(w => rank().over(sortBy(w.id)))),
            lines(
                "SELECT",
                """    RANK() OVER (ORDER BY "t1"."id" ASC) AS "c1"""",
                "FROM",
                """    "wide" AS "t1""""
            )
        )

    test("integer ranking functions yield a long"):
        assertEquals(compileErrors("from(Wide).map(w => expect[Long](rank().over(sortBy(w.id))))"), "")
        assertEquals(compileErrors("from(Wide).map(w => expect[Long](denseRank().over(sortBy(w.id))))"), "")
        assertEquals(compileErrors("from(Wide).map(w => expect[Long](rowNumber().over(sortBy(w.id))))"), "")
        assertEquals(compileErrors("from(Wide).map(w => expect[Long](ntile(4).over(sortBy(w.id))))"), "")

    test("fractional ranking functions yield a decimal"):
        assertEquals(compileErrors("from(Wide).map(w => expect[BigDecimal](percentRank().over(sortBy(w.id))))"), "")
        assertEquals(compileErrors("from(Wide).map(w => expect[BigDecimal](cumeDist().over(sortBy(w.id))))"), "")

    test("ntile accepts numeric operand forms"):
        assertEquals(compileErrors("from(Wide).map(w => ntile(4).over(sortBy(w.id)))"), "")
        assertEquals(compileErrors("from(Wide).map(w => ntile(w.id).over(sortBy(w.id)))"), "")
        assertEquals(compileErrors("from(Wide).map(w => ntile(w.score).over(sortBy(w.id)))"), "")
        assertEquals(compileErrors("from(Wide).map(w => ntile(w.id + 1).over(sortBy(w.id)))"), "")
        assertEquals(compileErrors("from(Wide).map(w => ntile(from(Wide).map(x => max(x.id))).over(sortBy(w.id)))"), "")

    test("ntile rejects a non-numeric bucket count"):
        assert(compileErrors("""from(Wide).map(w => ntile("x").over(sortBy(w.id)))""").startsWith("error: The arguments type should be numeric, but got java.lang.String."))
        assertEquals(
            messages(compileErrors("from(Wide).map(w => ntile(w.name).over(sortBy(w.id)))")),
            List("The arguments type should be numeric, but got java.lang.String.")
        )

    test("ranking functions require an over clause"):
        assertEquals(messages(compileErrors("from(Wide).map(w => rank())")), List("Window function requires an over clause."))
        assertEquals(messages(compileErrors("from(Wide).map(w => denseRank())")), List("Window function requires an over clause."))
        assertEquals(messages(compileErrors("from(Wide).map(w => percentRank())")), List("Window function requires an over clause."))
        assertEquals(messages(compileErrors("from(Wide).map(w => cumeDist())")), List("Window function requires an over clause."))
        assertEquals(messages(compileErrors("from(Wide).map(w => rowNumber())")), List("Window function requires an over clause."))
        assertEquals(messages(compileErrors("from(Wide).map(w => ntile(4))")), List("Window function requires an over clause."))

    test("ranking functions cannot be nested inside a window function"):
        assertEquals(
            messages(compileErrors("from(Wide).map(w => lag(rank().over(sortBy(w.id))).over(sortBy(w.id)))")),
            List("Window function calls cannot be nested.")
        )

    test("ranking functions cannot be used as a filter condition"):
        assert(compileErrors("from(Wide).filter(w => rank().over(sortBy(w.id)) > 1)").nonEmpty)