package sqala.test

import sqala.static.dsl.*

/**
 * Ordering, paging, distinct, having and the membership predicates.
 */
class Q4_Clauses extends munit.FunSuite:
    import StaticFixture.*

    test("ordering modifiers print direction and nulls placement"):
        assertEquals(
            sql(from(User).map(u => u.age).sortBy(u => (u.age.ascNullsFirst, u.name.descNullsLast))),
            lines(
                "SELECT",
                """    "t1"."age" AS "c1"""",
                "FROM",
                """    "user" AS "t1"""",
                "ORDER BY",
                """    "t1"."age" ASC NULLS FIRST,""",
                """    "t1"."name" DESC NULLS LAST"""
            )
        )

    test("take and drop combine into one limit offset clause"):
        assertEquals(
            sql(from(User).map(u => u.id).sortBy(u => u.id).take(10).drop(5)),
            lines(
                "SELECT",
                """    "t1"."id" AS "c1"""",
                "FROM",
                """    "user" AS "t1"""",
                "ORDER BY",
                """    "t1"."id" ASC""",
                "LIMIT 10 OFFSET 5"
            )
        )

    test("drop alone prints only an offset"):
        assertEquals(
            sql(from(User).map(u => u.id).drop(5)),
            lines(
                "SELECT",
                """    "t1"."id" AS "c1"""",
                "FROM",
                """    "user" AS "t1"""",
                "OFFSET 5"
            )
        )

    test("mapDistinct applies to the whole projection list"):
        assertEquals(
            sql(from(User).mapDistinct(u => u.name)),
            lines(
                "SELECT DISTINCT",
                """    "t1"."name" AS "c1"""",
                "FROM",
                """    "user" AS "t1""""
            )
        )
        assertEquals(
            sql(from(User).mapDistinct(u => (u.name, u.age))),
            lines(
                "SELECT DISTINCT",
                """    "t1"."name" AS "c1",""",
                """    "t1"."age" AS "c2"""",
                "FROM",
                """    "user" AS "t1""""
            )
        )

    test("having follows group by"):
        assertEquals(
            sql(from(User).groupBy(u => u.age).having((_, _) => count() > 1).map((g, _) => (g, count()))),
            lines(
                "SELECT",
                """    "t1"."age" AS "c1",""",
                """    COUNT(*) AS "c2"""",
                "FROM",
                """    "user" AS "t1"""",
                "GROUP BY",
                """    "t1"."age"""",
                "HAVING",
                """    COUNT(*) > 1"""
            )
        )

    test("in accepts a list, a tuple list and a subquery"):
        assertEquals(
            sql(from(User).filter(u => u.id.in(List(1, 2, 3))).map(u => u.name)),
            lines(
                "SELECT",
                """    "t1"."name" AS "c1"""",
                "FROM",
                """    "user" AS "t1"""",
                "WHERE",
                """    "t1"."id" IN (1, 2, 3)"""
            )
        )
        assertEquals(
            sql(from(User).filter(u => (u.id, u.name).in(List((1, "x"), (2, "y")))).map(u => u.name)),
            lines(
                "SELECT",
                """    "t1"."name" AS "c1"""",
                "FROM",
                """    "user" AS "t1"""",
                "WHERE",
                """    ("t1"."id", "t1"."name") IN ((1, 'x'), (2, 'y'))"""
            )
        )
        assertEquals(
            sql(from(User).filter(u => u.id.in(from(Post).map(p => p.userId))).map(u => u.name)),
            lines(
                "SELECT",
                """    "t1"."name" AS "c1"""",
                "FROM",
                """    "user" AS "t1"""",
                "WHERE",
                """    "t1"."id" IN (""",
                "        SELECT",
                """            "t2"."user_id" AS "c1"""",
                "        FROM",
                """            "post" AS "t2"""",
                "    )"
            )
        )

    test("exists correlates the subquery with the outer table"):
        assertEquals(
            sql(from(User).filter(u => exists(from(Post).filter(p => p.userId === u.id).map(p => p.id))).map(u => u.name)),
            lines(
                "SELECT",
                """    "t1"."name" AS "c1"""",
                "FROM",
                """    "user" AS "t1"""",
                "WHERE",
                "    EXISTS(",
                "        SELECT",
                """            "t2"."id" AS "c1"""",
                "        FROM",
                """            "post" AS "t2"""",
                "        WHERE",
                """            "t2"."user_id" = "t1"."id"""",
                "    )"
            )
        )