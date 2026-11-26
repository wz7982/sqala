package sqala.test

import sqala.static.dsl.*

/**
 * Join clauses: every join type, chained joins, lateral sources, and the
 * `Option` wrapping that outer joins apply to the nullable side.
 */
class Q2_Join extends munit.FunSuite:
    import StaticFixture.*

    private def optional[K <: ExprKind](e: Expr[Option[String], K]): Expr[Option[String], K] = e

    test("an inner join prints the join condition inline"):
        assertEquals(
            sql(from(User.join(Post).on((u, p) => u.id === p.userId)).map((u, p) => (u.name, p.title))),
            lines(
                "SELECT",
                """    "t1"."name" AS "c1",""",
                """    "t2"."title" AS "c2"""",
                "FROM",
                """    "user" AS "t1"""",
                """    INNER JOIN "post" AS "t2" ON "t1"."id" = "t2"."user_id""""
            )
        )

    test("outer joins print their own join keyword"):
        assertEquals(
            sql(from(User.leftJoin(Post).on((u, p) => u.id === p.userId)).map((u, p) => (u.name, p.title))),
            lines(
                "SELECT",
                """    "t1"."name" AS "c1",""",
                """    "t2"."title" AS "c2"""",
                "FROM",
                """    "user" AS "t1"""",
                """    LEFT OUTER JOIN "post" AS "t2" ON "t1"."id" = "t2"."user_id""""
            )
        )
        assertEquals(
            sql(from(User.rightJoin(Post).on((u, p) => u.id === p.userId)).map((u, p) => (u.name, p.title))),
            lines(
                "SELECT",
                """    "t1"."name" AS "c1",""",
                """    "t2"."title" AS "c2"""",
                "FROM",
                """    "user" AS "t1"""",
                """    RIGHT OUTER JOIN "post" AS "t2" ON "t1"."id" = "t2"."user_id""""
            )
        )
        assertEquals(
            sql(from(User.fullJoin(Post).on((u, p) => u.id === p.userId)).map((u, p) => (u.name, p.title))),
            lines(
                "SELECT",
                """    "t1"."name" AS "c1",""",
                """    "t2"."title" AS "c2"""",
                "FROM",
                """    "user" AS "t1"""",
                """    FULL OUTER JOIN "post" AS "t2" ON "t1"."id" = "t2"."user_id""""
            )
        )

    test("a cross join carries no condition"):
        assertEquals(
            sql(from(User.crossJoin(Post)).map((u, p) => (u.name, p.title))),
            lines(
                "SELECT",
                """    "t1"."name" AS "c1",""",
                """    "t2"."title" AS "c2"""",
                "FROM",
                """    "user" AS "t1"""",
                """    CROSS JOIN "post" AS "t2""""
            )
        )

    test("chained joins append one clause per table"):
        assertEquals(
            sql(from(User.join(Post).on((u, p) => u.id === p.userId).join(Wide).on((u, _, w) => u.id === w.id)).map((u, p, w) => (u.name, p.title, w.flag))),
            lines(
                "SELECT",
                """    "t1"."name" AS "c1",""",
                """    "t2"."title" AS "c2",""",
                """    "t3"."flag" AS "c3"""",
                "FROM",
                """    "user" AS "t1"""",
                """    INNER JOIN "post" AS "t2" ON "t1"."id" = "t2"."user_id"""",
                """    INNER JOIN "wide" AS "t3" ON "t1"."id" = "t3"."id""""
            )
        )

    test("a compound join condition stays in the on clause"):
        assertEquals(
            sql(from(User.join(Post).on((u, p) => u.id === p.userId && p.viewCount > 0)).map((u, _) => u.name)),
            lines(
                "SELECT",
                """    "t1"."name" AS "c1"""",
                "FROM",
                """    "user" AS "t1"""",
                """    INNER JOIN "post" AS "t2" ON "t1"."id" = "t2"."user_id" AND "t2"."view_count" > 0"""
            )
        )

    test("a filter after a join becomes a where clause"):
        assertEquals(
            sql(from(User.join(Post).on((u, p) => u.id === p.userId)).filter((u, _) => u.age > 18).map((u, _) => u.name)),
            lines(
                "SELECT",
                """    "t1"."name" AS "c1"""",
                "FROM",
                """    "user" AS "t1"""",
                """    INNER JOIN "post" AS "t2" ON "t1"."id" = "t2"."user_id"""",
                "WHERE",
                """    "t1"."age" > 18"""
            )
        )

    test("a self join aliases the same table twice"):
        assertEquals(
            sql(from(User.join(User).on((a, b) => a.id === b.id)).map((a, b) => (a.name, b.name))),
            lines(
                "SELECT",
                """    "t1"."name" AS "c1",""",
                """    "t2"."name" AS "c2"""",
                "FROM",
                """    "user" AS "t1"""",
                """    INNER JOIN "user" AS "t2" ON "t1"."id" = "t2"."id""""
            )
        )

    test("grouping after a join passes every table to the projection"):
        assertEquals(
            sql(from(User.join(Post).on((u, p) => u.id === p.userId)).groupBy((u, _) => u.name).map((g, _, _) => (g, count()))),
            lines(
                "SELECT",
                """    "t1"."name" AS "c1",""",
                """    COUNT(*) AS "c2"""",
                "FROM",
                """    "user" AS "t1"""",
                """    INNER JOIN "post" AS "t2" ON "t1"."id" = "t2"."user_id"""",
                "GROUP BY",
                """    "t1"."name""""
            )
        )

    test("sorting after a join references the joined table"):
        assertEquals(
            sql(from(User.join(Post).on((u, p) => u.id === p.userId)).map((u, p) => (u.name, p.viewCount)).sortBy((_, p) => p.viewCount.desc)),
            lines(
                "SELECT",
                """    "t1"."name" AS "c1",""",
                """    "t2"."view_count" AS "c2"""",
                "FROM",
                """    "user" AS "t1"""",
                """    INNER JOIN "post" AS "t2" ON "t1"."id" = "t2"."user_id"""",
                "ORDER BY",
                """    "t2"."view_count" DESC"""
            )
        )

    test("a lateral source may reference the left table"):
        assertEquals(
            sql(from(User.joinLateral(u => from(Post).filter(p => p.userId === u.id).map(p => (t = p.title))).on((_, _) => true)).map((u, t) => (u.name, t.t))),
            lines(
                "SELECT",
                """    "t1"."name" AS "c1",""",
                """    "t3"."c1" AS "c2"""",
                "FROM",
                """    "user" AS "t1"""",
                "    INNER JOIN LATERAL (",
                "        SELECT",
                """            "t2"."title" AS "c1"""",
                "        FROM",
                """            "post" AS "t2"""",
                "        WHERE",
                """            "t2"."user_id" = "t1"."id"""",
                """    ) AS "t3" ON TRUE"""
            )
        )

    test("a lateral source keeps its own ordering and limit"):
        assertEquals(
            sql(from(User.joinLateral(u => from(Post).filter(p => p.userId === u.id).map(p => (t = p.title)).sortBy(p => p.viewCount.desc).take(2)).on((_, _) => true)).map((u, t) => (u.name, t.t))),
            lines(
                "SELECT",
                """    "t1"."name" AS "c1",""",
                """    "t3"."c1" AS "c2"""",
                "FROM",
                """    "user" AS "t1"""",
                "    INNER JOIN LATERAL (",
                "        SELECT",
                """            "t2"."title" AS "c1"""",
                "        FROM",
                """            "post" AS "t2"""",
                "        WHERE",
                """            "t2"."user_id" = "t1"."id"""",
                "        ORDER BY",
                """            "t2"."view_count" DESC""",
                "        LIMIT 2",
                """    ) AS "t3" ON TRUE"""
            )
        )

    test("a lateral cross join needs no condition"):
        assertEquals(
            sql(from(User.crossJoinLateral(u => from(Post).filter(p => p.userId === u.id).map(p => (t = p.title)))).map((u, t) => (u.name, t.t))),
            lines(
                "SELECT",
                """    "t1"."name" AS "c1",""",
                """    "t3"."c1" AS "c2"""",
                "FROM",
                """    "user" AS "t1"""",
                "    CROSS JOIN LATERAL (",
                "        SELECT",
                """            "t2"."title" AS "c1"""",
                "        FROM",
                """            "post" AS "t2"""",
                "        WHERE",
                """            "t2"."user_id" = "t1"."id"""",
                """    ) AS "t3""""
            )
        )

    test("a lateral left join keeps the nullable side optional"):
        assertEquals(
            sql(from(User.leftJoinLateral(u => from(Post).filter(p => p.userId === u.id).map(p => (t = p.title))).on((_, _) => true)).map((u, t) => (u.name, t.t))),
            lines(
                "SELECT",
                """    "t1"."name" AS "c1",""",
                """    "t3"."c1" AS "c2"""",
                "FROM",
                """    "user" AS "t1"""",
                "    LEFT OUTER JOIN LATERAL (",
                "        SELECT",
                """            "t2"."title" AS "c1"""",
                "        FROM",
                """            "post" AS "t2"""",
                "        WHERE",
                """            "t2"."user_id" = "t1"."id"""",
                """    ) AS "t3" ON TRUE"""
            )
        )
        from(User.leftJoinLateral(u => from(Post).filter(p => p.userId === u.id).map(p => (t = p.title))).on((_, _) => true)).map((_, t) => optional(t.t))

    test("an outer join wraps the nullable side with Option"):
        from(User.leftJoin(Post).on((u, p) => u.id === p.userId)).map((_, p) => optional(p.title))
        from(User.rightJoin(Post).on((u, p) => u.id === p.userId)).map((u, _) => optional(u.name))
        from(User.fullJoin(Post).on((u, p) => u.id === p.userId)).map((u, p) => (optional(u.name), optional(p.title)))
        assert(compileErrors("from(User.join(Post).on((u, p) => u.id === p.userId)).map((u, p) => optional(p.title))").nonEmpty)

    test("a nullable join column can be defaulted"):
        assertEquals(
            sql(from(User.leftJoin(Post).on((u, p) => u.id === p.userId)).map((_, p) => p.title.getOrElse("x"))),
            lines(
                "SELECT",
                """    COALESCE("t2"."title", 'x') AS "c1"""",
                "FROM",
                """    "user" AS "t1"""",
                """    LEFT OUTER JOIN "post" AS "t2" ON "t1"."id" = "t2"."user_id""""
            )
        )