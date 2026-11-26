package sqala.test

import sqala.static.dsl.*

/**
 * Set operations. Each operand is parenthesised, and a chained operation
 * nests the previous result as its left operand.
 */
class Q3_SetOperation extends munit.FunSuite:
    import StaticFixture.*

    test("union and union all parenthesise both operands"):
        assertEquals(
            sql(from(User).map(u => u.id).union(from(Post).map(p => p.userId))),
            lines(
                "(",
                "    SELECT",
                """        "t1"."id" AS "c1"""",
                "    FROM",
                """        "user" AS "t1"""",
                ")",
                "UNION",
                "(",
                "    SELECT",
                """        "t2"."user_id" AS "c1"""",
                "    FROM",
                """        "post" AS "t2"""",
                ")"
            )
        )
        assertEquals(
            sql(from(User).map(u => u.id).unionAll(from(Post).map(p => p.userId))),
            lines(
                "(",
                "    SELECT",
                """        "t1"."id" AS "c1"""",
                "    FROM",
                """        "user" AS "t1"""",
                ")",
                "UNION ALL",
                "(",
                "    SELECT",
                """        "t2"."user_id" AS "c1"""",
                "    FROM",
                """        "post" AS "t2"""",
                ")"
            )
        )

    test("except and except all print their own keyword"):
        assertEquals(
            sql(from(User).map(u => u.id).except(from(Post).map(p => p.userId))),
            lines(
                "(",
                "    SELECT",
                """        "t1"."id" AS "c1"""",
                "    FROM",
                """        "user" AS "t1"""",
                ")",
                "EXCEPT",
                "(",
                "    SELECT",
                """        "t2"."user_id" AS "c1"""",
                "    FROM",
                """        "post" AS "t2"""",
                ")"
            )
        )
        assertEquals(
            sql(from(User).map(u => u.id).exceptAll(from(Post).map(p => p.userId))),
            lines(
                "(",
                "    SELECT",
                """        "t1"."id" AS "c1"""",
                "    FROM",
                """        "user" AS "t1"""",
                ")",
                "EXCEPT ALL",
                "(",
                "    SELECT",
                """        "t2"."user_id" AS "c1"""",
                "    FROM",
                """        "post" AS "t2"""",
                ")"
            )
        )

    test("intersect and intersect all print their own keyword"):
        assertEquals(
            sql(from(User).map(u => u.id).intersect(from(Post).map(p => p.userId))),
            lines(
                "(",
                "    SELECT",
                """        "t1"."id" AS "c1"""",
                "    FROM",
                """        "user" AS "t1"""",
                ")",
                "INTERSECT",
                "(",
                "    SELECT",
                """        "t2"."user_id" AS "c1"""",
                "    FROM",
                """        "post" AS "t2"""",
                ")"
            )
        )
        assertEquals(
            sql(from(User).map(u => u.id).intersectAll(from(Post).map(p => p.userId))),
            lines(
                "(",
                "    SELECT",
                """        "t1"."id" AS "c1"""",
                "    FROM",
                """        "user" AS "t1"""",
                ")",
                "INTERSECT ALL",
                "(",
                "    SELECT",
                """        "t2"."user_id" AS "c1"""",
                "    FROM",
                """        "post" AS "t2"""",
                ")"
            )
        )

    test("a chained operation nests the previous result"):
        assertEquals(
            sql(from(User).map(u => u.id).union(from(Post).map(p => p.userId)).union(from(Wide).map(w => w.id))),
            lines(
                "(",
                "    (",
                "        SELECT",
                """            "t1"."id" AS "c1"""",
                "        FROM",
                """            "user" AS "t1"""",
                "    )",
                "    UNION",
                "    (",
                "        SELECT",
                """            "t2"."user_id" AS "c1"""",
                "        FROM",
                """            "post" AS "t2"""",
                "    )",
                ")",
                "UNION",
                "(",
                "    SELECT",
                """        "t3"."id" AS "c1"""",
                "    FROM",
                """        "wide" AS "t3"""",
                ")"
            )
        )

    test("ordering and paging apply to the whole set result"):
        assertEquals(
            sql(from(User).map(u => u.id).union(from(Post).map(p => p.userId)).sortBy(x => x.desc).take(3).drop(1)),
            lines(
                "(",
                "    SELECT",
                """        "t1"."id" AS "c1"""",
                "    FROM",
                """        "user" AS "t1"""",
                ")",
                "UNION",
                "(",
                "    SELECT",
                """        "t2"."user_id" AS "c1"""",
                "    FROM",
                """        "post" AS "t2"""",
                ")",
                "ORDER BY",
                """    "c1" DESC""",
                "LIMIT 3 OFFSET 1"
            )
        )

    test("each operand keeps its own filter"):
        assertEquals(
            sql(from(User).filter(u => u.age > 18).map(u => u.id).union(from(Post).filter(p => p.viewCount > 0).map(p => p.userId))),
            lines(
                "(",
                "    SELECT",
                """        "t1"."id" AS "c1"""",
                "    FROM",
                """        "user" AS "t1"""",
                "    WHERE",
                """        "t1"."age" > 18""",
                ")",
                "UNION",
                "(",
                "    SELECT",
                """        "t2"."user_id" AS "c1"""",
                "    FROM",
                """        "post" AS "t2"""",
                "    WHERE",
                """        "t2"."view_count" > 0""",
                ")"
            )
        )

    test("operands must agree on column count and type"):
        assert(compileErrors("from(User).map(u => u.id).union(from(Post).map(p => p.title))").nonEmpty)
        assert(compileErrors("from(User).map(u => (u.id, u.name)).union(from(Post).map(p => p.userId))").nonEmpty)

    test("a tuple projection unions column by column"):
        assertEquals(
            sql(from(User).map(u => (u.id, u.name)).union(from(Post).map(p => (p.userId, p.title)))),
            lines(
                "(",
                "    SELECT",
                """        "t1"."id" AS "c1",""",
                """        "t1"."name" AS "c2"""",
                "    FROM",
                """        "user" AS "t1"""",
                ")",
                "UNION",
                "(",
                "    SELECT",
                """        "t2"."user_id" AS "c1",""",
                """        "t2"."title" AS "c2"""",
                "    FROM",
                """        "post" AS "t2"""",
                ")"
            )
        )

    test("a named projection unions field by field"):
        assertEquals(
            sql(from(User).map(u => (i = u.id, n = u.name)).union(from(Post).map(p => (i = p.userId, n = p.title)))),
            lines(
                "(",
                "    SELECT",
                """        "t1"."id" AS "c1",""",
                """        "t1"."name" AS "c2"""",
                "    FROM",
                """        "user" AS "t1"""",
                ")",
                "UNION",
                "(",
                "    SELECT",
                """        "t2"."user_id" AS "c1",""",
                """        "t2"."title" AS "c2"""",
                "    FROM",
                """        "post" AS "t2"""",
                ")"
            )
        )
        assertEquals(
            sql(from(User).map(u => (i = u.id)).union(from(Post).map(p => (i = p.userId)))),
            lines(
                "(",
                "    SELECT",
                """        "t1"."id" AS "c1"""",
                "    FROM",
                """        "user" AS "t1"""",
                ")",
                "UNION",
                "(",
                "    SELECT",
                """        "t2"."user_id" AS "c1"""",
                "    FROM",
                """        "post" AS "t2"""",
                ")"
            )
        )

    test("a named set result can be used as a table source"):
        assertEquals(
            sql(from(from(User).map(u => (i = u.id, n = u.name)).union(from(Post).map(p => (i = p.userId, n = p.title)))).map(s => (s.i, s.n))),
            lines(
                "SELECT",
                """    "t3"."c1" AS "c1",""",
                """    "t3"."c2" AS "c2"""",
                "FROM",
                "    (",
                "        (",
                "            SELECT",
                """                "t1"."id" AS "c1",""",
                """                "t1"."name" AS "c2"""",
                "            FROM",
                """                "user" AS "t1"""",
                "        )",
                "        UNION",
                "        (",
                "            SELECT",
                """                "t2"."user_id" AS "c1",""",
                """                "t2"."title" AS "c2"""",
                "            FROM",
                """                "post" AS "t2"""",
                "        )",
                """    ) AS "t3""""
            )
        )

    test("a single field set result keeps its field name as a table source"):
        assertEquals(
            sql(from(from(User).map(u => (i = u.id)).union(from(Post).map(p => (i = p.userId)))).map(s => s.i)),
            lines(
                "SELECT",
                """    "t3"."c1" AS "c1"""",
                "FROM",
                "    (",
                "        (",
                "            SELECT",
                """                "t1"."id" AS "c1"""",
                "            FROM",
                """                "user" AS "t1"""",
                "        )",
                "        UNION",
                "        (",
                "            SELECT",
                """                "t2"."user_id" AS "c1"""",
                "            FROM",
                """                "post" AS "t2"""",
                "        )",
                """    ) AS "t3""""
            )
        )