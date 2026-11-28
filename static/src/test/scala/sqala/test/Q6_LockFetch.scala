package sqala.test

import sqala.static.dsl.*
import sqala.test.StaticFixture.{lines, sql}

/**
 * Row locking clauses, `FETCH ... WITH TIES` on every query shape,
 * conditional filtering, and the recursion depth limit of `connectBy`.
 */
class Q6_LockFetch extends munit.FunSuite:
    test("update locks print the expected sql"):
        assertEquals(
            sql(from(User).forUpdate),
            lines(
                "SELECT",
                """    "t1"."id" AS "c1",""",
                """    "t1"."name" AS "c2",""",
                """    "t1"."age" AS "c3"""",
                "FROM",
                """    "user" AS "t1"""",
                "FOR UPDATE"
            )
        )
        assertEquals(
            sql(from(User).forUpdateNoWait),
            lines(
                "SELECT",
                """    "t1"."id" AS "c1",""",
                """    "t1"."name" AS "c2",""",
                """    "t1"."age" AS "c3"""",
                "FROM",
                """    "user" AS "t1"""",
                "FOR UPDATE NOWAIT"
            )
        )
        assertEquals(
            sql(from(User).forUpdateSkipLocked),
            lines(
                "SELECT",
                """    "t1"."id" AS "c1",""",
                """    "t1"."name" AS "c2",""",
                """    "t1"."age" AS "c3"""",
                "FROM",
                """    "user" AS "t1"""",
                "FOR UPDATE SKIP LOCKED"
            )
        )

    test("share locks print the expected sql"):
        assertEquals(
            sql(from(User).forShare),
            lines(
                "SELECT",
                """    "t1"."id" AS "c1",""",
                """    "t1"."name" AS "c2",""",
                """    "t1"."age" AS "c3"""",
                "FROM",
                """    "user" AS "t1"""",
                "FOR SHARE"
            )
        )
        assertEquals(
            sql(from(User).forShareNoWait),
            lines(
                "SELECT",
                """    "t1"."id" AS "c1",""",
                """    "t1"."name" AS "c2",""",
                """    "t1"."age" AS "c3"""",
                "FROM",
                """    "user" AS "t1"""",
                "FOR SHARE NOWAIT"
            )
        )
        assertEquals(
            sql(from(User).forShareSkipLocked),
            lines(
                "SELECT",
                """    "t1"."id" AS "c1",""",
                """    "t1"."name" AS "c2",""",
                """    "t1"."age" AS "c3"""",
                "FROM",
                """    "user" AS "t1"""",
                "FOR SHARE SKIP LOCKED"
            )
        )

    test("a lock clause combines with filtering and paging"):
        assertEquals(
            sql(from(User).filter(u => u.id > 1).sortBy(u => u.id).take(2).forUpdateSkipLocked),
            lines(
                "SELECT",
                """    "t1"."id" AS "c1",""",
                """    "t1"."name" AS "c2",""",
                """    "t1"."age" AS "c3"""",
                "FROM",
                """    "user" AS "t1"""",
                "WHERE",
                """    "t1"."id" > 1""",
                "ORDER BY",
                """    "t1"."id" ASC""",
                "LIMIT 2",
                "FOR UPDATE SKIP LOCKED"
            )
        )

    test("a lock clause is terminal"):
        assert(compileErrors("from(User).forUpdate.forShare").nonEmpty)
        assert(compileErrors("from(User).forUpdate.filter(u => u.id > 1)").nonEmpty)
        assert(compileErrors("from(User).forShare.take(1)").nonEmpty)

    test("takeWithTies prints fetch with ties on a sorted select"):
        assertEquals(
            sql(from(User).sortBy(u => u.name).takeWithTies(5)),
            lines(
                "SELECT",
                """    "t1"."id" AS "c1",""",
                """    "t1"."name" AS "c2",""",
                """    "t1"."age" AS "c3"""",
                "FROM",
                """    "user" AS "t1"""",
                "ORDER BY",
                """    "t1"."name" ASC""",
                "FETCH NEXT 5 ROWS WITH TIES"
            )
        )

    test("takeWithTies keeps an existing offset"):
        assertEquals(
            sql(from(User).sortBy(u => u.name).drop(2).takeWithTies(5)),
            lines(
                "SELECT",
                """    "t1"."id" AS "c1",""",
                """    "t1"."name" AS "c2",""",
                """    "t1"."age" AS "c3"""",
                "FROM",
                """    "user" AS "t1"""",
                "ORDER BY",
                """    "t1"."name" ASC""",
                "OFFSET 2 ROWS FETCH NEXT 5 ROWS WITH TIES"
            )
        )

    test("takeWithTies works on a set query"):
        assertEquals(
            sql(from(User).sortBy(u => u.name).union(from(User).sortBy(u => u.name)).sortBy(u => u.name).takeWithTies(5)),
            lines(
                "(",
                "    SELECT",
                """        "t1"."id" AS "c1",""",
                """        "t1"."name" AS "c2",""",
                """        "t1"."age" AS "c3"""",
                "    FROM",
                """        "user" AS "t1"""",
                "    ORDER BY",
                """        "t1"."name" ASC""",
                ")",
                "UNION",
                "(",
                "    SELECT",
                """        "t2"."id" AS "c1",""",
                """        "t2"."name" AS "c2",""",
                """        "t2"."age" AS "c3"""",
                "    FROM",
                """        "user" AS "t2"""",
                "    ORDER BY",
                """        "t2"."name" ASC""",
                ")",
                "ORDER BY",
                """    "c2" ASC""",
                "FETCH NEXT 5 ROWS WITH TIES"
            )
        )

    test("takeWithTies works on a recursive query"):
        assertEquals(
            sql(from(Node).connectBy((n, prior) => n.parentId === prior.id).startWith(n => n.parentId.isEmpty).map((n, l) => (n.name, l)).sortBy((n, _) => n.name).takeWithTies(5)),
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
                "ORDER BY",
                """    "t3"."c3" ASC""",
                "FETCH NEXT 5 ROWS WITH TIES"
            )
        )

    test("takeWithTies requires a sorted query"):
        assert(compileErrors("from(User).takeWithTies(5)").nonEmpty)
        assert(compileErrors("from(User).union(from(User)).takeWithTies(5)").nonEmpty)

    test("takeWithTies participates in row count inference"):
        assertEquals(compileErrors("from(User).map(u => from(User).map(x => x.id).sortBy(x => x.id).takeWithTies(1).asExpr)"), "")
        assert(compileErrors("from(User).map(u => from(User).map(x => x.id).sortBy(x => x.id).takeWithTies(2).asExpr)").nonEmpty)

    test("filterIf applies the condition only when the test holds"):
        assertEquals(
            sql(from(User).filterIf(true)(u => u.id > 1)),
            lines(
                "SELECT",
                """    "t1"."id" AS "c1",""",
                """    "t1"."name" AS "c2",""",
                """    "t1"."age" AS "c3"""",
                "FROM",
                """    "user" AS "t1"""",
                "WHERE",
                """    "t1"."id" > 1"""
            )
        )
        assertEquals(
            sql(from(User).filterIf(false)(u => u.id > 1)),
            lines(
                "SELECT",
                """    "t1"."id" AS "c1",""",
                """    "t1"."name" AS "c2",""",
                """    "t1"."age" AS "c3"""",
                "FROM",
                """    "user" AS "t1""""
            )
        )

    test("filterIf accumulates with filter and validates its condition"):
        assertEquals(
            sql(from(User).filter(u => u.age > 0).filterIf(true)(u => u.id > 1)),
            lines(
                "SELECT",
                """    "t1"."id" AS "c1",""",
                """    "t1"."name" AS "c2",""",
                """    "t1"."age" AS "c3"""",
                "FROM",
                """    "user" AS "t1"""",
                "WHERE",
                """    "t1"."age" > 0 AND "t1"."id" > 1"""
            )
        )
        assert(compileErrors("from(User).filterIf(true)(u => u.id)").nonEmpty)
        assert(compileErrors("from(User).filterIf(true)(u => count() > 1)").nonEmpty)

    test("withFilter drives the for comprehension guard"):
        assertEquals(
            sql(for u <- from(User) if u.id > 1 yield u.name),
            lines(
                "SELECT",
                """    "t1"."name" AS "c1"""",
                "FROM",
                """    "user" AS "t1"""",
                "WHERE",
                """    "t1"."id" > 1"""
            )
        )
        assertEquals(
            sql(for u <- from(User) if u.id > 1 if u.age > 0 yield u.name),
            sql(from(User).filter(u => u.id > 1).filter(u => u.age > 0).map(u => u.name))
        )

    test("preMap projects the columns carried through the recursion"):
        assertEquals(
            sql(from(Node).connectBy((n, prior) => n.parentId === prior.id).startWith(n => n.parentId.isEmpty).preMap(n => (id = n.id, name = n.name))((n, _, _) => (id = n.id, name = n.name)).map((n, l) => (n.id, n.name, l))),
            lines(
                "WITH RECURSIVE",
                """"cte1"("c1", "c2", "c3") AS (""",
                "    (",
                "        SELECT",
                """            "t1"."id" AS "c1",""",
                """            "t1"."name" AS "c2",""",
                """            1 AS "c3"""",
                "        FROM",
                """            "node" AS "t1"""",
                "        WHERE",
                """            "t1"."parent_id" IS NULL""",
                "    )",
                "    UNION ALL",
                "    (",
                "        SELECT",
                """            "t1"."id" AS "c1",""",
                """            "t1"."name" AS "c2",""",
                """            "t2"."c3" + 1 AS "c3"""",
                "        FROM",
                """            "node" AS "t1"""",
                """            INNER JOIN "cte1" AS "t2" ON "t1"."parent_id" = "t2"."c1"""",
                "    )",
                ")",
                "SELECT",
                """    "t3"."c1" AS "c1",""",
                """    "t3"."c2" AS "c2",""",
                """    "t3"."c3" AS "c3"""",
                "FROM",
                """    "cte1" AS "t3""""
            )
        )

    test("maxDepth keeps the seed branch and limits the step branch"):
        assertEquals(
            sql(from(Node).connectBy((n, prior) => n.parentId === prior.id).startWith(n => n.parentId.isEmpty).maxDepth(5).map((n, l) => (n.name, l))),
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
                "        WHERE",
                """            "t2"."c4" <= 5""",
                "    )",
                ")",
                "SELECT",
                """    "t3"."c3" AS "c1",""",
                """    "t3"."c4" AS "c2"""",
                "FROM",
                """    "cte1" AS "t3""""
            )
        )