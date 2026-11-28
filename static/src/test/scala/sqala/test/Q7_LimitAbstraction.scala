package sqala.test

import sqala.static.dsl.*
import sqala.static.dsl.statement.query.Query
import sqala.test.StaticFixture.{lines, sql}

/**
 * `take` and `drop` as part of the `Query` abstraction: set-operation and
 * recursive queries limit through the same interface, a limited set query
 * qualifies as a scalar subquery, and a base-typed query exposes both
 * operations.
 */
class Q7_LimitAbstraction extends munit.FunSuite:
    test("set-operation queries take and drop"):
        assertEquals(
            sql(from(User).union(from(User)).drop(1).take(2)),
            lines(
                "(",
                "    SELECT",
                """        "t1"."id" AS "c1",""",
                """        "t1"."name" AS "c2",""",
                """        "t1"."age" AS "c3"""",
                "    FROM",
                """        "user" AS "t1"""",
                ")",
                "UNION",
                "(",
                "    SELECT",
                """        "t2"."id" AS "c1",""",
                """        "t2"."name" AS "c2",""",
                """        "t2"."age" AS "c3"""",
                "    FROM",
                """        "user" AS "t2"""",
                ")",
                "LIMIT 2 OFFSET 1"
            )
        )

    test("recursive queries take and drop on the final select"):
        assertEquals(
            sql(from(Node).connectBy((n, prior) => n.parentId === prior.id).startWith(n => n.parentId.isEmpty).map((n, l) => (n.name, l)).drop(1).take(2)),
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
                """    "cte1" AS "t3"""",
                "LIMIT 2 OFFSET 1"
            )
        )

    test("a set query becomes a scalar subquery after take(1)"):
        assertEquals(compileErrors("from(User).filter(u => u.id == from(User).map(u => u.id).union(from(User).map(u => u.id)).take(1))"), "")
        assert(compileErrors("from(User).filter(u => u.id == from(User).map(u => u.id).union(from(User).map(u => u.id)))")
            .startsWith("error: More than one row returned by a subquery used as an expression."))

    test("take(0) yields an empty set query"):
        assertEquals(compileErrors("from(User).union(from(User)).take(0)"), "")
        assert(sql(from(User).union(from(User)).take(0)).endsWith("LIMIT 0"))

    test("the Query abstraction exposes take and drop"):
        val q: Query[?, ?, ?, ?] = from(User).union(from(User))
        assertEquals(sql(q.drop(1).take(2)), sql(from(User).union(from(User)).drop(1).take(2)))
        assertEquals(compileErrors("from(User).forUpdate.take(1)"), "")