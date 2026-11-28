package sqala.test

import sqala.static.dsl.*

/**
 * Hierarchical queries. `connectBy` builds a recursive CTE whose seed is
 * the `startWith` predicate and whose step joins the prior level, carrying
 * a depth counter that the projection receives as its second argument.
 */
class Q5_Recursive extends munit.FunSuite:
    import StaticFixture.*

    test("connectBy builds a seed and a recursive step"):
        assertEquals(
            sql(from(Node).connectBy((n, prior) => n.parentId === prior.id).startWith(n => n.parentId.isEmpty).map((n, l) => (n.name, l))),
            lines(
                "WITH RECURSIVE",
                """"cte1"("c1", "c2", "c3", "c4") AS (""",
                "    (",
                "        SELECT",
                """            "t1"."id" AS "c1",""",
                """            "t1"."parent_id" AS "c2",""",
                """            "t1"."name" AS "c3",""",
                """            1 AS "c4"""",
                "        FROM",
                """            "node" AS "t1"""",
                "        WHERE",
                """            "t1"."parent_id" IS NULL""",
                "    )",
                "    UNION ALL",
                "    (",
                "        SELECT",
                """            "t1"."id" AS "c1",""",
                """            "t1"."parent_id" AS "c2",""",
                """            "t1"."name" AS "c3",""",
                """            "t2"."c4" + 1 AS "c4"""",
                "        FROM",
                """            "node" AS "t1"""",
                """            INNER JOIN "cte1" AS "t2" ON "t1"."parent_id" = "t2"."c1"""",
                "    )",
                ")",
                "SELECT",
                """    "t3"."c3" AS "c1",""",
                """    "t3"."c4" AS "c2"""",
                "FROM",
                """    "cte1" AS "t3""""
            )
        )

    test("a filter before connectBy applies to both the seed and the step"):
        assertEquals(
            sql(from(Node).filter(n => n.id > 0).connectBy((n, prior) => n.parentId === prior.id).startWith(n => n.parentId.isEmpty).map((n, l) => (n.name, l))),
            lines(
                "WITH RECURSIVE",
                """"cte1"("c1", "c2", "c3", "c4") AS (""",
                "    (",
                "        SELECT",
                """            "t1"."id" AS "c1",""",
                """            "t1"."parent_id" AS "c2",""",
                """            "t1"."name" AS "c3",""",
                """            1 AS "c4"""",
                "        FROM",
                """            "node" AS "t1"""",
                "        WHERE",
                """            "t1"."id" > 0 AND "t1"."parent_id" IS NULL""",
                "    )",
                "    UNION ALL",
                "    (",
                "        SELECT",
                """            "t1"."id" AS "c1",""",
                """            "t1"."parent_id" AS "c2",""",
                """            "t1"."name" AS "c3",""",
                """            "t2"."c4" + 1 AS "c4"""",
                "        FROM",
                """            "node" AS "t1"""",
                """            INNER JOIN "cte1" AS "t2" ON "t1"."parent_id" = "t2"."c1"""",
                "        WHERE",
                """            "t1"."id" > 0""",
                "    )",
                ")",
                "SELECT",
                """    "t3"."c3" AS "c1",""",
                """    "t3"."c4" AS "c2"""",
                "FROM",
                """    "cte1" AS "t3""""
            )
        )