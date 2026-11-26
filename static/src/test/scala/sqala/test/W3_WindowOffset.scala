package sqala.test

import sqala.static.dsl.*
import sqala.test.StaticFixture.*

/**
 * Offset window functions `LAG` and `LEAD` in all three arities
 * (value, value plus offset, value plus offset and default), each
 * with its `IGNORE NULLS` variant.
 */
class W3_WindowOffset extends munit.FunSuite:
    test("lag prints the expected sql for every arity"):
        assertEquals(
            sql(from(Wide).map(w => (
                a = lag(w.name).over(sortBy(w.id)),
                b = lag(w.name, 2).over(sortBy(w.id)),
                c = lag(w.name, 2, "x").over(sortBy(w.id))
            ))),
            lines(
                "SELECT",
                """    LAG("t1"."name") OVER (ORDER BY "t1"."id" ASC) AS "c1",""",
                """    LAG("t1"."name", 2) OVER (ORDER BY "t1"."id" ASC) AS "c2",""",
                """    LAG("t1"."name", 2, 'x') OVER (ORDER BY "t1"."id" ASC) AS "c3"""",
                "FROM",
                """    "wide" AS "t1""""
            )
        )

    test("lead prints the expected sql for every arity"):
        assertEquals(
            sql(from(Wide).map(w => (
                a = lead(w.name).over(sortBy(w.id)),
                b = lead(w.name, 2).over(sortBy(w.id)),
                c = lead(w.name, 2, "x").over(sortBy(w.id))
            ))),
            lines(
                "SELECT",
                """    LEAD("t1"."name") OVER (ORDER BY "t1"."id" ASC) AS "c1",""",
                """    LEAD("t1"."name", 2) OVER (ORDER BY "t1"."id" ASC) AS "c2",""",
                """    LEAD("t1"."name", 2, 'x') OVER (ORDER BY "t1"."id" ASC) AS "c3"""",
                "FROM",
                """    "wide" AS "t1""""
            )
        )

    test("ignore nulls variants print the expected sql"):
        assertEquals(
            sql(from(Wide).map(w => (
                a = lagIgnoreNulls(w.name).over(sortBy(w.id)),
                b = lagIgnoreNulls(w.name, 2).over(sortBy(w.id)),
                c = lagIgnoreNulls(w.name, 2, "x").over(sortBy(w.id)),
                d = leadIgnoreNulls(w.name).over(sortBy(w.id)),
                e = leadIgnoreNulls(w.name, 2).over(sortBy(w.id)),
                f = leadIgnoreNulls(w.name, 2, "x").over(sortBy(w.id))
            ))),
            lines(
                "SELECT",
                """    LAG("t1"."name") IGNORE NULLS OVER (ORDER BY "t1"."id" ASC) AS "c1",""",
                """    LAG("t1"."name", 2) IGNORE NULLS OVER (ORDER BY "t1"."id" ASC) AS "c2",""",
                """    LAG("t1"."name", 2, 'x') IGNORE NULLS OVER (ORDER BY "t1"."id" ASC) AS "c3",""",
                """    LEAD("t1"."name") IGNORE NULLS OVER (ORDER BY "t1"."id" ASC) AS "c4",""",
                """    LEAD("t1"."name", 2) IGNORE NULLS OVER (ORDER BY "t1"."id" ASC) AS "c5",""",
                """    LEAD("t1"."name", 2, 'x') IGNORE NULLS OVER (ORDER BY "t1"."id" ASC) AS "c6"""",
                "FROM",
                """    "wide" AS "t1""""
            )
        )

    test("offset functions accept a partition and a frame"):
        assertEquals(
            sql(from(Wide).map(w => lag(w.name, 2).over(partitionBy(w.id).sortBy(w.id).rowsBetween(1.preceding, currentRow)))),
            lines(
                "SELECT",
                """    LAG("t1"."name", 2) OVER (PARTITION BY "t1"."id" ORDER BY "t1"."id" ASC ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS "c1"""",
                "FROM",
                """    "wide" AS "t1""""
            )
        )

    test("offset functions always yield an optional value"):
        assertEquals(compileErrors("from(Wide).map(w => expect[Option[String]](lag(w.name).over(sortBy(w.id))))"), "")
        assertEquals(compileErrors("from(Wide).map(w => expect[Option[String]](lag(w.name, 2).over(sortBy(w.id))))"), "")
        assertEquals(compileErrors("from(Wide).map(w => expect[Option[String]](lag(w.name, 2, \"x\").over(sortBy(w.id))))"), "")
        assertEquals(compileErrors("from(Wide).map(w => expect[Option[String]](leadIgnoreNulls(w.name, 2, \"x\").over(sortBy(w.id))))"), "")
        assertEquals(compileErrors("from(Wide).map(w => expect[Option[Long]](lag(w.score).over(sortBy(w.id))))"), "")

    test("the default value widens the result type"):
        assertEquals(compileErrors("from(Wide).map(w => expect[Option[Long]](lag(w.id, 2, 1L).over(sortBy(w.id))))"), "")
        assertEquals(compileErrors("from(Nums).map(n => expect[Option[Double]](lead(n.i, 2, 1.0).over(sortBy(n.i))))"), "")
        assertEquals(compileErrors("from(Nums).map(n => expect[Option[BigDecimal]](lead(n.i, 2, BigDecimal(1)).over(sortBy(n.i))))"), "")

    test("offset functions accept numeric offset forms"):
        assertEquals(compileErrors("from(Wide).map(w => lag(w.name, 2).over(sortBy(w.id)))"), "")
        assertEquals(compileErrors("from(Wide).map(w => lag(w.name, w.id).over(sortBy(w.id)))"), "")
        assertEquals(compileErrors("from(Wide).map(w => lag(w.name, w.score).over(sortBy(w.id)))"), "")
        assertEquals(compileErrors("from(Wide).map(w => lag(w.name, w.id + 1).over(sortBy(w.id)))"), "")
        assertEquals(compileErrors("from(Wide).map(w => lag(w.name, from(Wide).map(x => max(x.id))).over(sortBy(w.id)))"), "")

    test("offset functions reject a non-numeric offset"):
        assertEquals(
            messages(compileErrors("""from(Wide).map(w => lag(w.name, "x").over(sortBy(w.id)))""")),
            List("The arguments type should be numeric, but got java.lang.String.")
        )
        assertEquals(
            messages(compileErrors("from(Wide).map(w => lead(w.name, w.name).over(sortBy(w.id)))")),
            List("The arguments type should be numeric, but got java.lang.String.")
        )

    test("offset functions reject an incompatible default"):
        assertEquals(
            messages(compileErrors("from(Wide).map(w => lag(w.name, 2, 1).over(sortBy(w.id)))")),
            List("Types java.lang.String and scala.Int are incompatible.")
        )
        assertEquals(
            messages(compileErrors("from(Wide).map(w => leadIgnoreNulls(w.id, 2, \"x\").over(sortBy(w.id)))")),
            List("Types scala.Int and java.lang.String are incompatible.")
        )

    test("offset functions require an over clause"):
        assertEquals(messages(compileErrors("from(Wide).map(w => lag(w.name))")), List("Window function requires an over clause."))
        assertEquals(messages(compileErrors("from(Wide).map(w => lead(w.name, 2))")), List("Window function requires an over clause."))
        assertEquals(messages(compileErrors("""from(Wide).map(w => lagIgnoreNulls(w.name, 2, "x"))""")), List("Window function requires an over clause."))

    test("offset functions reject aggregate and window operands"):
        assert(compileErrors("from(Wide).map(w => lag(sum(w.id)).over(sortBy(w.id)))").nonEmpty)
        assertEquals(
            messages(compileErrors("from(Wide).map(w => lead(rowNumber().over(sortBy(w.id))).over(sortBy(w.id)))")),
            List("Window function calls cannot be nested.")
        )