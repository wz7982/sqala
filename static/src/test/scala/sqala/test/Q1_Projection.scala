package sqala.test

import sqala.static.dsl.*

/**
 * A projected query used as a table source. The projection may mix scalar
 * fields with whole rows, so the outer query has to resolve a field through
 * two levels of aliasing: the subquery's column alias, then the field inside
 * the embedded row.
 */
class Q1_Projection extends munit.FunSuite:
    import StaticFixture.*

    test("a whole row in a projection expands to every column"):
        assertEquals(
            sql(from(User).map(u => (x = u.id, y = u.name, a = u))),
            lines(
                "SELECT",
                """    "t1"."id" AS "c1",""",
                """    "t1"."name" AS "c2",""",
                """    "t1"."id" AS "c3",""",
                """    "t1"."name" AS "c4",""",
                """    "t1"."age" AS "c5"""",
                "FROM",
                """    "user" AS "t1""""
            )
        )

    test("an outer field reference resolves through the embedded row"):
        assertEquals(
            sql(from(from(User).map(u => (x = u.id, y = u.name, a = u))).map(s => (s.a.id, s.a.name, s.x))),
            lines(
                "SELECT",
                """    "t2"."c3" AS "c1",""",
                """    "t2"."c4" AS "c2",""",
                """    "t2"."c1" AS "c3"""",
                "FROM",
                "    (",
                "        SELECT",
                """            "t1"."id" AS "c1",""",
                """            "t1"."name" AS "c2",""",
                """            "t1"."id" AS "c3",""",
                """            "t1"."name" AS "c4",""",
                """            "t1"."age" AS "c5"""",
                "        FROM",
                """            "user" AS "t1"""",
                """    ) AS "t2""""
            )
        )

    test("the embedded row can precede the scalar fields"):
        assertEquals(
            sql(from(from(User).map(u => (a = u, x = u.id))).map(s => (s.x, s.a.name))),
            lines(
                "SELECT",
                """    "t2"."c4" AS "c1",""",
                """    "t2"."c2" AS "c2"""",
                "FROM",
                "    (",
                "        SELECT",
                """            "t1"."id" AS "c1",""",
                """            "t1"."name" AS "c2",""",
                """            "t1"."age" AS "c3",""",
                """            "t1"."id" AS "c4"""",
                "        FROM",
                """            "user" AS "t1"""",
                """    ) AS "t2""""
            )
        )

    test("a computed field and an embedded row coexist in one projection"):
        assertEquals(
            sql(from(from(User).map(u => (n = u.name, d = u.age + 1, a = u))).map(s => (s.d, s.a.age))),
            lines(
                "SELECT",
                """    "t2"."c2" AS "c1",""",
                """    "t2"."c5" AS "c2"""",
                "FROM",
                "    (",
                "        SELECT",
                """            "t1"."name" AS "c1",""",
                """            "t1"."age" + 1 AS "c2",""",
                """            "t1"."id" AS "c3",""",
                """            "t1"."name" AS "c4",""",
                """            "t1"."age" AS "c5"""",
                "        FROM",
                """            "user" AS "t1"""",
                """    ) AS "t2""""
            )
        )

    test("a join condition can reference a field inside the embedded row"):
        assertEquals(
            sql(from(User.join(from(User).map(u => (x = u.id, a = u))).on((u, s) => u.id === s.a.id)).map((u, s) => (u.name, s.a.name))),
            lines(
                "SELECT",
                """    "t1"."name" AS "c1",""",
                """    "t3"."c3" AS "c2"""",
                "FROM",
                """    "user" AS "t1"""",
                "    INNER JOIN (",
                "        SELECT",
                """            "t2"."id" AS "c1",""",
                """            "t2"."id" AS "c2",""",
                """            "t2"."name" AS "c3",""",
                """            "t2"."age" AS "c4"""",
                "        FROM",
                """            "user" AS "t2"""",
                """    ) AS "t3" ON "t1"."id" = "t3"."c2""""
            )
        )

    test("an embedded row survives a second round of projection"):
        assertEquals(
            sql(from(from(from(User).map(u => (x = u.id, a = u))).map(s => (b = s.a, z = s.x))).map(t => (t.b.id, t.z))),
            lines(
                "SELECT",
                """    "t3"."c1" AS "c1",""",
                """    "t3"."c4" AS "c2"""",
                "FROM",
                "    (",
                "        SELECT",
                """            "t2"."c2" AS "c1",""",
                """            "t2"."c3" AS "c2",""",
                """            "t2"."c4" AS "c3",""",
                """            "t2"."c1" AS "c4"""",
                "        FROM",
                "            (",
                "                SELECT",
                """                    "t1"."id" AS "c1",""",
                """                    "t1"."id" AS "c2",""",
                """                    "t1"."name" AS "c3",""",
                """                    "t1"."age" AS "c4"""",
                "                FROM",
                """                    "user" AS "t1"""",
                """            ) AS "t2"""",
                """    ) AS "t3""""
            )
        )

    test("a filter on an embedded field uses the subquery column alias"):
        assertEquals(
            sql(from(from(User).map(u => (x = u.id, a = u))).filter(s => s.a.age > 18).map(s => s.a.name)),
            lines(
                "SELECT",
                """    "t2"."c3" AS "c1"""",
                "FROM",
                "    (",
                "        SELECT",
                """            "t1"."id" AS "c1",""",
                """            "t1"."id" AS "c2",""",
                """            "t1"."name" AS "c3",""",
                """            "t1"."age" AS "c4"""",
                "        FROM",
                """            "user" AS "t1"""",
                """    ) AS "t2"""",
                "WHERE",
                """    "t2"."c4" > 18"""
            )
        )

    test("projecting the embedded row alone re-expands its columns"):
        assertEquals(
            sql(from(from(User).map(u => (x = u.id, a = u))).map(s => s.a)),
            lines(
                "SELECT",
                """    "t2"."c2" AS "c1",""",
                """    "t2"."c3" AS "c2",""",
                """    "t2"."c4" AS "c3"""",
                "FROM",
                "    (",
                "        SELECT",
                """            "t1"."id" AS "c1",""",
                """            "t1"."id" AS "c2",""",
                """            "t1"."name" AS "c3",""",
                """            "t1"."age" AS "c4"""",
                "        FROM",
                """            "user" AS "t1"""",
                """    ) AS "t2""""
            )
        )

    test("an aggregate over an embedded field"):
        assertEquals(
            sql(from(from(User).map(u => (x = u.id, a = u))).map(s => count(s.a.id))),
            lines(
                "SELECT",
                """    COUNT("t2"."c2") AS "c1"""",
                "FROM",
                "    (",
                "        SELECT",
                """            "t1"."id" AS "c1",""",
                """            "t1"."id" AS "c2",""",
                """            "t1"."name" AS "c3",""",
                """            "t1"."age" AS "c4"""",
                "        FROM",
                """            "user" AS "t1"""",
                """    ) AS "t2""""
            )
        )

    test("grouping by an embedded field"):
        assertEquals(
            sql(from(from(User).map(u => (x = u.id, a = u))).groupBy(s => s.a.age).map((g, _) => (g, count()))),
            lines(
                "SELECT",
                """    "t2"."c4" AS "c1",""",
                """    COUNT(*) AS "c2"""",
                "FROM",
                "    (",
                "        SELECT",
                """            "t1"."id" AS "c1",""",
                """            "t1"."id" AS "c2",""",
                """            "t1"."name" AS "c3",""",
                """            "t1"."age" AS "c4"""",
                "        FROM",
                """            "user" AS "t1"""",
                """    ) AS "t2"""",
                "GROUP BY",
                """    "t2"."c4""""
            )
        )

    test("a bare value projection prints a literal select item"):
        assertEquals(
            sql(from(User).map(_ => 1)),
            lines(
                "SELECT",
                """    1 AS "c1"""",
                "FROM",
                """    "user" AS "t1""""
            )
        )
        assertEquals(
            sql(from(User).map(_ => "x")),
            lines(
                "SELECT",
                """    'x' AS "c1"""",
                "FROM",
                """    "user" AS "t1""""
            )
        )
        assertEquals(
            sql(from(User).map(_ => (a = 1, b = "x", c = Option(2)))),
            lines(
                "SELECT",
                """    1 AS "c1",""",
                """    'x' AS "c2",""",
                """    2 AS "c3"""",
                "FROM",
                """    "user" AS "t1""""
            )
        )

    test("a bare value in a subquery projection is referenced by its column alias"):
        assertEquals(
            sql(from(from(User).map(_ => (x = 1))).map(s => s.x)),
            lines(
                "SELECT",
                """    "t2"."c1" AS "c1"""",
                "FROM",
                "    (",
                "        SELECT",
                """            1 AS "c1"""",
                "        FROM",
                """            "user" AS "t1"""",
                """    ) AS "t2""""
            )
        )

    test("a bare value field takes part in an outer expression"):
        assertEquals(
            sql(from(from(User).map(u => (x = 1, y = u.name))).map(s => s.x + 1)),
            lines(
                "SELECT",
                """    "t2"."c1" + 1 AS "c1"""",
                "FROM",
                "    (",
                "        SELECT",
                """            1 AS "c1",""",
                """            "t1"."name" AS "c2"""",
                "        FROM",
                """            "user" AS "t1"""",
                """    ) AS "t2""""
            )
        )

    test("a bare value field can be filtered and sorted"):
        assertEquals(
            sql(from(from(User).map(u => (x = 1, y = u.name))).filter(s => s.x > 0).map(s => s.y)),
            lines(
                "SELECT",
                """    "t2"."c2" AS "c1"""",
                "FROM",
                "    (",
                "        SELECT",
                """            1 AS "c1",""",
                """            "t1"."name" AS "c2"""",
                "        FROM",
                """            "user" AS "t1"""",
                """    ) AS "t2"""",
                "WHERE",
                """    "t2"."c1" > 0"""
            )
        )
        assertEquals(
            sql(from(from(User).map(u => (x = 1, y = u.name))).map(s => s.x).sortBy(s => s.x)),
            lines(
                "SELECT",
                """    "t2"."c1" AS "c1"""",
                "FROM",
                "    (",
                "        SELECT",
                """            1 AS "c1",""",
                """            "t1"."name" AS "c2"""",
                "        FROM",
                """            "user" AS "t1"""",
                """    ) AS "t2"""",
                "ORDER BY",
                """    "t2"."c1" ASC"""
            )
        )

    test("a bare value field can be grouped and aggregated"):
        assertEquals(
            sql(from(from(User).map(u => (x = 1, y = u.name))).groupBy(s => s.x).map((g, _) => (g, count()))),
            lines(
                "SELECT",
                """    "t2"."c1" AS "c1",""",
                """    COUNT(*) AS "c2"""",
                "FROM",
                "    (",
                "        SELECT",
                """            1 AS "c1",""",
                """            "t1"."name" AS "c2"""",
                "        FROM",
                """            "user" AS "t1"""",
                """    ) AS "t2"""",
                "GROUP BY",
                """    "t2"."c1""""
            )
        )
        assertEquals(
            sql(from(from(User).map(u => (x = 1, y = u.name))).map(s => sum(s.x))),
            lines(
                "SELECT",
                """    SUM("t2"."c1") AS "c1"""",
                "FROM",
                "    (",
                "        SELECT",
                """            1 AS "c1",""",
                """            "t1"."name" AS "c2"""",
                "        FROM",
                """            "user" AS "t1"""",
                """    ) AS "t2""""
            )
        )

    test("a bare value field can be projected again"):
        assertEquals(
            sql(from(from(User).map(u => (x = 1, y = u.name))).map(s => (s.x, s.y))),
            lines(
                "SELECT",
                """    "t2"."c1" AS "c1",""",
                """    "t2"."c2" AS "c2"""",
                "FROM",
                "    (",
                "        SELECT",
                """            1 AS "c1",""",
                """            "t1"."name" AS "c2"""",
                "        FROM",
                """            "user" AS "t1"""",
                """    ) AS "t2""""
            )
        )

    test("a subquery table requires a named projection"):
        assert(compileErrors("from(from(User).map(u => 1)).map(s => s)").nonEmpty)
        assert(compileErrors("from(from(User).map(u => u.id)).map(s => s)").nonEmpty)
        assert(compileErrors("from(from(User).map(u => (1, \"x\"))).map(s => s)").nonEmpty)
        assertEquals(compileErrors("from(from(User).map(u => (x = 1))).map(s => s.x)"), "")