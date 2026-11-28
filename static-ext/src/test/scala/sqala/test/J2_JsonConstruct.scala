package sqala.test

import sqala.static.dsl.*
import sqala.static.ext.dsl.*
import sqala.test.ExtFixture.*

/**
 * JSON constructors and aggregates: `JSON_OBJECT`, `JSON_ARRAY`,
 * `JSON_OBJECTAGG` and `JSON_ARRAYAGG`. Covers the generated sql, the
 * inferred result type, the pair list shapes, and the aggregate rules.
 */
class J2_JsonConstruct extends munit.FunSuite:
    test("jsonObject prints one pair per key and value"):
        assertEquals(
            sql(from(Doc).map(d => jsonObject((d.id, d.body)))),
            lines(
                "SELECT",
                """    JSON_OBJECT("t1"."id" VALUE "t1"."body") AS "c1"""",
                "FROM",
                """    "doc" AS "t1""""
            )
        )
        assertEquals(
            sql(from(Doc).map(d => jsonObject(((d.id, d.body), (d.body, d.id))))),
            lines(
                "SELECT",
                """    JSON_OBJECT("t1"."id" VALUE "t1"."body", "t1"."body" VALUE "t1"."id") AS "c1"""",
                "FROM",
                """    "doc" AS "t1""""
            )
        )
        assertEquals(
            sql(from(Doc).map(d => jsonObject(((d.id, d.body), (d.body, d.id), (d.id, d.id))))),
            lines(
                "SELECT",
                """    JSON_OBJECT("t1"."id" VALUE "t1"."body", "t1"."body" VALUE "t1"."id", "t1"."id" VALUE "t1"."id") AS "c1"""",
                "FROM",
                """    "doc" AS "t1""""
            )
        )

    test("jsonObject accepts literal keys and subquery values"):
        assertEquals(
            sql(from(Doc).map(d => jsonObject(("k", d.id)))),
            lines(
                "SELECT",
                """    JSON_OBJECT('k' VALUE "t1"."id") AS "c1"""",
                "FROM",
                """    "doc" AS "t1""""
            )
        )
        assertEquals(
            sql(from(Doc).map(d => jsonObject((d.body, from(Doc).map(x => max(x.id)))))),
            lines(
                "SELECT",
                """    JSON_OBJECT("t1"."body" VALUE (""",
                "        SELECT",
                """            MAX("t2"."id") AS "c1"""",
                "        FROM",
                """            "doc" AS "t2"""",
                "    )) AS \"c1\"",
                "FROM",
                """    "doc" AS "t1""""
            )
        )

    test("jsonArray prints its operand"):
        assertEquals(
            sql(from(Doc).map(d => jsonArray(d.id))),
            lines(
                "SELECT",
                """    JSON_ARRAY("t1"."id") AS "c1"""",
                "FROM",
                """    "doc" AS "t1""""
            )
        )
        assertEquals(
            sql(from(Doc).map(d => jsonArray(d.id + 1))),
            lines(
                "SELECT",
                """    JSON_ARRAY("t1"."id" + 1) AS "c1"""",
                "FROM",
                """    "doc" AS "t1""""
            )
        )
        assertEquals(
            sql(from(Doc).map(_ => jsonArray(Array(1, 2)))),
            lines(
                "SELECT",
                """    JSON_ARRAY(ARRAY[1, 2]) AS "c1"""",
                "FROM",
                """    "doc" AS "t1""""
            )
        )
        assertEquals(
            sql(from(Arr).map(a => jsonArray(a.nums))),
            lines(
                "SELECT",
                """    JSON_ARRAY("t1"."nums") AS "c1"""",
                "FROM",
                """    "arr" AS "t1""""
            )
        )

    test("jsonArray expands a tuple into separate elements"):
        assertEquals(
            sql(from(Doc).map(d => jsonArray((d.id, d.body)))),
            lines(
                "SELECT",
                """    JSON_ARRAY("t1"."id", "t1"."body") AS "c1"""",
                "FROM",
                """    "doc" AS "t1""""
            )
        )
        assertEquals(
            sql(from(Doc).map(d => jsonArray((d.id, d.body, d.id)))),
            lines(
                "SELECT",
                """    JSON_ARRAY("t1"."id", "t1"."body", "t1"."id") AS "c1"""",
                "FROM",
                """    "doc" AS "t1""""
            )
        )
        assertEquals(
            sql(from(Doc).map(_ => jsonArray((1, "x", true)))),
            lines(
                "SELECT",
                """    JSON_ARRAY(1, 'x', TRUE) AS "c1"""",
                "FROM",
                """    "doc" AS "t1""""
            )
        )

    test("jsonArray tuple elements keep their own shape"):
        assertEquals(
            sql(from(Doc).map(d => jsonArray((d.id, (d.body, d.id))))),
            lines(
                "SELECT",
                """    JSON_ARRAY("t1"."id", ("t1"."body", "t1"."id")) AS "c1"""",
                "FROM",
                """    "doc" AS "t1""""
            )
        )
        assertEquals(
            sql(from(Doc).map(d => jsonArray((d.id, d.obody)))),
            lines(
                "SELECT",
                """    JSON_ARRAY("t1"."id", "t1"."obody") AS "c1"""",
                "FROM",
                """    "doc" AS "t1""""
            )
        )
        assertEquals(
            sql(from(Doc).map(d => jsonArray((d.id, rank().over())))),
            lines(
                "SELECT",
                """    JSON_ARRAY("t1"."id", RANK() OVER ()) AS "c1"""",
                "FROM",
                """    "doc" AS "t1""""
            )
        )
        assertEquals(
            sql(from(Doc).map(d => jsonArray((d.id, from(Doc).map(x => max(x.id)))))),
            lines(
                "SELECT",
                """    JSON_ARRAY("t1"."id", (""",
                "        SELECT",
                """            MAX("t2"."id") AS "c1"""",
                "        FROM",
                """            "doc" AS "t2"""",
                "    )) AS \"c1\"",
                "FROM",
                """    "doc" AS "t1""""
            )
        )

    test("jsonArray accepts a subquery operand"):
        assertEquals(
            sql(from(Doc).map(_ => jsonArray(from(Doc).map(x => max(x.id))))),
            lines(
                "SELECT",
                "    JSON_ARRAY((",
                "        SELECT",
                """            MAX("t2"."id") AS "c1"""",
                "        FROM",
                """            "doc" AS "t2"""",
                "    )) AS \"c1\"",
                "FROM",
                """    "doc" AS "t1""""
            )
        )

    test("json aggregates print the expected sql"):
        assertEquals(
            sql(from(Doc).groupBy(d => d.id).map((_, d) => (a = jsonArrayAgg(d.body), b = jsonObjectAgg((d.body, d.id))))),
            lines(
                "SELECT",
                """    JSON_ARRAYAGG("t1"."body") AS "c1",""",
                """    JSON_OBJECTAGG("t1"."body" VALUE "t1"."id") AS "c2"""",
                "FROM",
                """    "doc" AS "t1"""",
                "GROUP BY",
                """    "t1"."id""""
            )
        )

    test("json constructors and aggregates yield an optional json"):
        assertEquals(compileErrors("from(Doc).map(d => expect[Option[sqala.metadata.Json]](jsonObject((d.id, d.body))))"), "")
        assertEquals(compileErrors("from(Doc).map(d => expect[Option[sqala.metadata.Json]](jsonArray(d.id)))"), "")
        assertEquals(compileErrors("from(Doc).map(d => expect[Option[sqala.metadata.Json]](jsonArray((d.id, d.body))))"), "")
        assertEquals(compileErrors("from(Doc).groupBy(d => d.id).map((g, d) => expect[Option[sqala.metadata.Json]](jsonArrayAgg(d.body)))"), "")
        assertEquals(compileErrors("from(Doc).groupBy(d => d.id).map((g, d) => expect[Option[sqala.metadata.Json]](jsonObjectAgg((d.body, d.id))))"), "")

    test("json aggregates accept ungrouped columns and a window clause"):
        assertEquals(compileErrors("from(Doc).groupBy(d => d.id).map((g, d) => jsonArrayAgg(d.body))"), "")
        assertEquals(compileErrors("from(Doc).groupBy(d => d.id).map((g, d) => jsonObjectAgg((d.body, d.id)))"), "")
        assertEquals(compileErrors("from(Doc).map(d => jsonArrayAgg(d.body).over())"), "")
        assertEquals(compileErrors("from(Doc).map(d => jsonObjectAgg((d.body, d.id)).over())"), "")
        assertEquals(compileErrors("from(Doc).groupBy(d => d.id).having((g, d) => jsonArrayAgg(d.body) === jsonArrayAgg(d.body)).map((g, d) => count())"), "")

    test("json aggregates reject nested aggregates and window operands"):
        assertEquals(
            messages(compileErrors("from(Doc).map(d => jsonArrayAgg(count()))")),
            List("Aggregate function calls cannot be nested.")
        )
        assertEquals(
            messages(compileErrors("from(Doc).map(d => jsonArrayAgg(rank().over()))")),
            List("Aggregate function calls cannot contain window function calls.")
        )

    test("jsonObject requires a list of pairs"):
        assert(compileErrors("from(Doc).map(d => jsonObject(((d.id, d.body), d.id)))").contains("AsJsonObject"))

    test("jsonArray accepts expressions and tuples, not sequences"):
        assert(compileErrors("from(Doc).map(d => jsonArray(Seq(1, 2)))").contains("This value cannot be converted to a query expression."))
        assert(compileErrors("from(Doc).map(d => jsonArray(EmptyTuple))").contains("This value cannot be converted to a query expression."))

    test("jsonArray rejects ungrouped columns after groupBy"):
        assertEquals(
            messages(compileErrors("from(Doc).groupBy(d => d.id).map((g, d) => jsonArray((g, d.body)))")),
            List("Ungrouped column references must appear in the groupBy clause or be used in an aggregate function.")
        )