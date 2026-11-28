package sqala.test

import sqala.dynamic.dsl.*
import sqala.test.DynamicFixture.*

/**
 * Table sources: named tables, derived tables, the eight join shapes,
 * lateral variants, chained joins, and the AST accessor.
 */
class D5_Tables_Joins extends munit.FunSuite:
    test("a named table renders qualified and with an alias"):
        assertEquals(
            pg(select(column("id").as("id")).from(table("user"))),
            lines(
                "SELECT",
                """    "id" AS "id"""",
                "FROM",
                """    "user""""
            )
        )
        assertEquals(
            pg(select(column("id").as("id")).from(table("user").as("u"))),
            lines(
                "SELECT",
                """    "id" AS "id"""",
                "FROM",
                """    "user" AS "u""""
            )
        )

    test("the from clause accepts a list of tables"):
        assertEquals(
            pg(select(column("u", "id").as("id")).from(List(table("user").as("u"), table("role")))),
            lines(
                "SELECT",
                """    "u"."id" AS "id"""",
                "FROM",
                """    "user" AS "u",""",
                """    "role""""
            )
        )

    test("inner join renders the on condition"):
        assertEquals(
            pg(select(column("u", "id").as("id")).from(table("user").as("u").join(table("post").as("p")).on(column("u", "id") === column("p", "user_id")))),
            lines(
                "SELECT",
                """    "u"."id" AS "id"""",
                "FROM",
                """    "user" AS "u"""",
                """    INNER JOIN "post" AS "p" ON "u"."id" = "p"."user_id""""
            )
        )

    test("outer joins render left right and full variants"):
        assertEquals(
            pg(select(column("u", "id").as("id")).from(table("user").as("u").leftJoin(table("post")).on(column("u", "id") === column("post", "user_id")))),
            lines(
                "SELECT",
                """    "u"."id" AS "id"""",
                "FROM",
                """    "user" AS "u"""",
                """    LEFT OUTER JOIN "post" ON "u"."id" = "post"."user_id""""
            )
        )
        assertEquals(
            pg(select(column("u", "id").as("id")).from(table("user").as("u").rightJoin(table("post")).on(column("u", "id") === column("post", "user_id")))),
            lines(
                "SELECT",
                """    "u"."id" AS "id"""",
                "FROM",
                """    "user" AS "u"""",
                """    RIGHT OUTER JOIN "post" ON "u"."id" = "post"."user_id""""
            )
        )
        assertEquals(
            pg(select(column("u", "id").as("id")).from(table("user").as("u").fullJoin(table("post")).on(column("u", "id") === column("post", "user_id")))),
            lines(
                "SELECT",
                """    "u"."id" AS "id"""",
                "FROM",
                """    "user" AS "u"""",
                """    FULL OUTER JOIN "post" ON "u"."id" = "post"."user_id""""
            )
        )

    test("cross join needs no condition"):
        assertEquals(
            pg(select(column("u", "id").as("id")).from(table("user").as("u").crossJoin(table("role")))),
            lines(
                "SELECT",
                """    "u"."id" AS "id"""",
                "FROM",
                """    "user" AS "u"""",
                """    CROSS JOIN "role""""
            )
        )

    test("derived tables render as parenthesized subqueries with an alias"):
        assertEquals(
            pg(select(column("s", "id").as("id")).from(table(select(column("id").as("id")).from(table("user")).where(column("age") > value(18))).as("s"))),
            lines(
                "SELECT",
                """    "s"."id" AS "id"""",
                "FROM",
                "    (",
                "        SELECT",
                """            "id" AS "id"""",
                "        FROM",
                """            "user"""",
                "        WHERE",
                """            "age" > 18""",
                """    ) AS "s""""
            )
        )

    test("lateral joins mark the derived table with LATERAL"):
        assertEquals(
            pg(select(column("u", "id").as("id"), column("s", "c").as("c")).from(table("user").as("u").joinLateral(table(select(column("post", "id").as("c")).from(table("post")).where(column("post", "user_id") === column("u", "id")).limit(1)).as("s")).on(column("u", "id") === column("s", "c")))),
            lines(
                "SELECT",
                """    "u"."id" AS "id",""",
                """    "s"."c" AS "c"""",
                "FROM",
                """    "user" AS "u"""",
                "    INNER JOIN LATERAL (",
                "        SELECT",
                """            "post"."id" AS "c"""",
                "        FROM",
                """            "post"""",
                "        WHERE",
                """            "post"."user_id" = "u"."id"""",
                "        LIMIT 1",
                """    ) AS "s" ON "u"."id" = "s"."c""""
            )
        )
        assertEquals(
            pg(select(column("u", "id").as("id")).from(table("user").as("u").crossJoinLateral(table(select(value(1).as("c"))).as("s")))),
            lines(
                "SELECT",
                """    "u"."id" AS "id"""",
                "FROM",
                """    "user" AS "u"""",
                "    CROSS JOIN LATERAL (",
                "        SELECT",
                """            1 AS "c"""",
                """    ) AS "s""""
            )
        )
        assertEquals(
            pg(select(column("u", "id").as("id")).from(table("user").as("u").leftJoinLateral(table(select(value(1).as("c"))).as("s")).on(column("u", "id") === column("s", "c")))),
            lines(
                "SELECT",
                """    "u"."id" AS "id"""",
                "FROM",
                """    "user" AS "u"""",
                """    LEFT OUTER JOIN LATERAL (""",
                "        SELECT",
                """            1 AS "c"""",
                """    ) AS "s" ON "u"."id" = "s"."c""""
            )
        )

    test("joins chain left associatively"):
        assertEquals(
            pg(select(column("id").as("id")).from(table("a").as("x").join(table("b").as("y")).on(column("x", "id") === column("y", "a_id")).join(table("c").as("z")).on(column("y", "b_id") === column("z", "id")))),
            lines(
                "SELECT",
                """    "id" AS "id"""",
                "FROM",
                """    "a" AS "x"""",
                """    INNER JOIN "b" AS "y" ON "x"."id" = "y"."a_id"""",
                """    INNER JOIN "c" AS "z" ON "y"."b_id" = "z"."id""""
            )
        )

    test("the ast accessors expose the underlying nodes"):
        assert(column("user", "id").ast.toString.contains("Column"))
        assert(column("user", "id").ast.toString.contains("user"))
        assert(table("user").as("u").ast.toString.contains("user"))
        assert(column("id").as("id").ast.toString.contains("id"))
        assert(column("id").asc.ast.toString.contains("id"))
