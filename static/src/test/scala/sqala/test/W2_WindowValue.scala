package sqala.test

import sqala.static.dsl.*
import sqala.test.StaticFixture.*

/**
 * Value-taking window functions: `FIRST_VALUE`, `LAST_VALUE`,
 * `NTH_VALUE` and their `IGNORE NULLS` / `FROM LAST` variants.
 */
class W2_WindowValue extends munit.FunSuite:
    test("value window functions print the expected sql"):
        assertEquals(
            sql(from(Wide).map(w => (
                a = firstValue(w.name).over(sortBy(w.id)),
                b = firstValueIgnoreNulls(w.name).over(sortBy(w.id)),
                c = lastValue(w.name).over(sortBy(w.id)),
                d = lastValueIgnoreNulls(w.name).over(sortBy(w.id))
            ))),
            lines(
                "SELECT",
                """    FIRST_VALUE("t1"."name") OVER (ORDER BY "t1"."id" ASC) AS "c1",""",
                """    FIRST_VALUE("t1"."name") IGNORE NULLS OVER (ORDER BY "t1"."id" ASC) AS "c2",""",
                """    LAST_VALUE("t1"."name") OVER (ORDER BY "t1"."id" ASC) AS "c3",""",
                """    LAST_VALUE("t1"."name") IGNORE NULLS OVER (ORDER BY "t1"."id" ASC) AS "c4"""",
                "FROM",
                """    "wide" AS "t1""""
            )
        )

    test("nth value variants print the expected sql"):
        assertEquals(
            sql(from(Wide).map(w => (
                a = nthValue(w.name, 2).over(sortBy(w.id)),
                b = nthValueIgnoreNulls(w.name, 2).over(sortBy(w.id)),
                c = nthValueFromLast(w.name, 2).over(sortBy(w.id)),
                d = nthValueFromLastIgnoreNulls(w.name, 2).over(sortBy(w.id))
            ))),
            lines(
                "SELECT",
                """    NTH_VALUE("t1"."name", 2) OVER (ORDER BY "t1"."id" ASC) AS "c1",""",
                """    NTH_VALUE("t1"."name", 2) IGNORE NULLS OVER (ORDER BY "t1"."id" ASC) AS "c2",""",
                """    NTH_VALUE("t1"."name", 2) FROM LAST OVER (ORDER BY "t1"."id" ASC) AS "c3",""",
                """    NTH_VALUE("t1"."name", 2) FROM LAST IGNORE NULLS OVER (ORDER BY "t1"."id" ASC) AS "c4"""",
                "FROM",
                """    "wide" AS "t1""""
            )
        )

    test("value window functions accept a frame and a partition"):
        assertEquals(
            sql(from(Wide).map(w => firstValue(w.name).over(partitionBy(w.id).sortBy(w.id).rowsBetween(unboundedPreceding, currentRow)))),
            lines(
                "SELECT",
                """    FIRST_VALUE("t1"."name") OVER (PARTITION BY "t1"."id" ORDER BY "t1"."id" ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "c1"""",
                "FROM",
                """    "wide" AS "t1""""
            )
        )

    test("value window functions always yield an optional value"):
        assertEquals(compileErrors("from(Wide).map(w => expect[Option[String]](firstValue(w.name).over(sortBy(w.id))))"), "")
        assertEquals(compileErrors("from(Wide).map(w => expect[Option[String]](firstValueIgnoreNulls(w.name).over(sortBy(w.id))))"), "")
        assertEquals(compileErrors("from(Wide).map(w => expect[Option[String]](lastValue(w.name).over(sortBy(w.id))))"), "")
        assertEquals(compileErrors("from(Wide).map(w => expect[Option[String]](lastValueIgnoreNulls(w.name).over(sortBy(w.id))))"), "")
        assertEquals(compileErrors("from(Wide).map(w => expect[Option[String]](nthValue(w.name, 2).over(sortBy(w.id))))"), "")
        assertEquals(compileErrors("from(Wide).map(w => expect[Option[String]](nthValueFromLastIgnoreNulls(w.name, 2).over(sortBy(w.id))))"), "")

    test("an optional operand stays optional"):
        assertEquals(compileErrors("from(Wide).map(w => expect[Option[Long]](firstValue(w.score).over(sortBy(w.id))))"), "")
        assertEquals(compileErrors("from(Wide).map(w => expect[Option[Long]](nthValue(w.score, 2).over(sortBy(w.id))))"), "")

    test("value window functions accept scalar operand forms"):
        assertEquals(compileErrors("""from(Wide).map(w => firstValue("x").over(sortBy(w.id)))"""), "")
        assertEquals(compileErrors("from(Wide).map(w => firstValue(w.name).over(sortBy(w.id)))"), "")
        assertEquals(compileErrors("from(Wide).map(w => firstValue(w.score).over(sortBy(w.id)))"), "")
        assertEquals(compileErrors("""from(Wide).map(w => firstValue(w.name ++ "x").over(sortBy(w.id)))"""), "")
        assertEquals(compileErrors("from(Wide).map(w => firstValue(from(Wide).map(x => max(x.name))).over(sortBy(w.id)))"), "")

    test("nth value rejects a non-numeric position"):
        assertEquals(
            messages(compileErrors("""from(Wide).map(w => nthValue(w.name, "x").over(sortBy(w.id)))""")),
            List("The arguments type should be numeric, but got java.lang.String.")
        )
        assertEquals(
            messages(compileErrors("from(Wide).map(w => nthValueFromLast(w.name, w.name).over(sortBy(w.id)))")),
            List("The arguments type should be numeric, but got java.lang.String.")
        )

    test("value window functions require an over clause"):
        assertEquals(messages(compileErrors("from(Wide).map(w => firstValue(w.name))")), List("Window function requires an over clause."))
        assertEquals(messages(compileErrors("from(Wide).map(w => lastValueIgnoreNulls(w.name))")), List("Window function requires an over clause."))
        assertEquals(messages(compileErrors("from(Wide).map(w => nthValue(w.name, 2))")), List("Window function requires an over clause."))
        assertEquals(messages(compileErrors("from(Wide).map(w => nthValueFromLast(w.name, 2))")), List("Window function requires an over clause."))

    test("an aggregate operand is allowed but an ungrouped sort key is not"):
        assertEquals(compileErrors("from(Wide).map(w => firstValue(sum(w.id)).over())"), "")
        assertEquals(compileErrors("from(Wide).map(w => nthValue(count(), 2).over())"), "")
        assertEquals(
            messages(compileErrors("from(Wide).map(w => firstValue(sum(w.id)).over(sortBy(w.id)))")),
            List("Ungrouped column references must appear in the groupBy clause or be used in an aggregate function.")
        )
        assert(compileErrors("from(Wide).map(w => nthValue(count(), 2).over(sortBy(w.id)))").nonEmpty)

    test("value window functions reject a window operand"):
        assertEquals(
            messages(compileErrors("from(Wide).map(w => firstValue(rowNumber().over(sortBy(w.id))).over(sortBy(w.id)))")),
            List("Window function calls cannot be nested.")
        )