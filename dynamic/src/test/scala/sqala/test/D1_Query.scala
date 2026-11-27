package sqala.test

import sqala.dynamic.dsl.*
import sqala.test.DynamicFixture.*

/**
 * Query-level builder behavior: projection, from, where, group/having,
 * ordering, pagination, distinct, set operations, immutability, and
 * argument validation.
 */
class D1_Query extends munit.FunSuite:
    test("a select without a from clause renders the projection only"):
        assertEquals(
            pg(select(column("id").as("id"))),
            lines(
                "SELECT",
                """    "id" AS "id""""
            )
        )

    test("qualified and unqualified columns and inline values render in the projection"):
        assertEquals(
            pg(select(column("user", "id").as("id"), value(1).as("one"))),
            lines(
                "SELECT",
                """    "user"."id" AS "id",""",
                """    1 AS "one""""
            )
        )

    test("the list overload of select accepts a list of items"):
        assertEquals(
            pg(select(List(column("id").as("id"), column("name").as("name"))).from(table("user").as("u"))),
            lines(
                "SELECT",
                """    "id" AS "id",""",
                """    "name" AS "name"""",
                "FROM",
                """    "user" AS "u""""
            )
        )

    test("where adds a filter and repeated calls are combined with and"):
        assertEquals(
            pg(select(column("id").as("id")).from(table("user")).where(column("a") === value(1)).where(column("b") === value(2))),
            lines(
                "SELECT",
                """    "id" AS "id"""",
                "FROM",
                """    "user"""",
                "WHERE",
                """    "a" = 1 AND "b" = 2"""
            )
        )

    test("orderBy appends items with asc desc and nulls placement"):
        assertEquals(
            pg(select(column("id").as("id")).from(table("user")).orderBy(column("a").asc, column("b").descNullsFirst)),
            lines(
                "SELECT",
                """    "id" AS "id"""",
                "FROM",
                """    "user"""",
                "ORDER BY",
                """    "a" ASC,""",
                """    "b" DESC NULLS FIRST"""
            )
        )

    test("limit and offset render in either application order"):
        val q = select(column("id").as("id")).from(table("user"))
        val expected = lines(
            "SELECT",
            """    "id" AS "id"""",
            "FROM",
            """    "user"""",
            "LIMIT 10 OFFSET 20"
        )
        assertEquals(pg(q.offset(20).limit(10)), expected)
        assertEquals(pg(q.limit(10).offset(20)), expected)

    test("distinct marks the projection"):
        assertEquals(
            pg(select(column("author").as("a")).from(table("post")).distinct),
            lines(
                "SELECT DISTINCT",
                """    "author" AS "a"""",
                "FROM",
                """    "post""""
            )
        )

    test("groupBy and having render the grouping clause pair"):
        assertEquals(
            pg(select(column("author").as("a"), count().as("c")).from(table("post")).groupBy(column("author")).having(count() > value(10))),
            lines(
                "SELECT",
                """    "author" AS "a",""",
                """    COUNT(*) AS "c"""",
                "FROM",
                """    "post"""",
                "GROUP BY",
                """    "author"""",
                "HAVING",
                """    COUNT(*) > 10"""
            )
        )

    test("an empty groupBy call is a no-op"):
        assertEquals(
            pg(select(column("id").as("id")).from(table("user")).groupBy()),
            lines(
                "SELECT",
                """    "id" AS "id"""",
                "FROM",
                """    "user""""
            )
        )

    test("select appends projection items to an existing query"):
        assertEquals(
            pg(select(column("id").as("id")).from(table("user")).select(column("name").as("name"))),
            lines(
                "SELECT",
                """    "id" AS "id",""",
                """    "name" AS "name"""",
                "FROM",
                """    "user""""
            )
        )

    test("builders return new queries so a base query can be branched"):
        val base = select(column("id").as("id")).from(table("user"))
        val branchA = base.where(column("a") === value(1))
        val branchB = base.limit(5)
        assertNotEquals(pg(base), pg(branchA))
        assertNotEquals(pg(base), pg(branchB))
        assertEquals(pg(base), lines("SELECT", """    "id" AS "id"""", "FROM", """    "user""""))

    test("set operations wrap both operands and expose their own order by and limit"):
        val a = select(column("id").as("id")).from(table("user"))
        val b = select(column("id").as("id")).from(table("admin"))
        assertEquals(
            pg(a.union(b)),
            lines(
                "(",
                "    SELECT",
                """        "id" AS "id"""",
                "    FROM",
                """        "user"""",
                ")",
                "UNION",
                "(",
                "    SELECT",
                """        "id" AS "id"""",
                "    FROM",
                """        "admin"""",
                ")"
            )
        )
        assertEquals(
            pg(a.unionAll(b).orderBy(column("id").asc)),
            lines(
                "(",
                "    SELECT",
                """        "id" AS "id"""",
                "    FROM",
                """        "user"""",
                ")",
                "UNION ALL",
                "(",
                "    SELECT",
                """        "id" AS "id"""",
                "    FROM",
                """        "admin"""",
                ")",
                "ORDER BY",
                """    "id" ASC"""
            )
        )
        assertEquals(
            pg(a.exceptAll(b)),
            lines(
                "(",
                "    SELECT",
                """        "id" AS "id"""",
                "    FROM",
                """        "user"""",
                ")",
                "EXCEPT ALL",
                "(",
                "    SELECT",
                """        "id" AS "id"""",
                "    FROM",
                """        "admin"""",
                ")"
            )
        )
        assertEquals(
            pg(a.intersectAll(b).limit(5)),
            lines(
                "(",
                "    SELECT",
                """        "id" AS "id"""",
                "    FROM",
                """        "user"""",
                ")",
                "INTERSECT ALL",
                "(",
                "    SELECT",
                """        "id" AS "id"""",
                "    FROM",
                """        "admin"""",
                ")",
                "LIMIT 5"
            )
        )

    test("limits inside set operands are kept per operand"):
        val a = select(column("id").as("id")).from(table("user")).limit(1)
        val b = select(column("id").as("id")).from(table("admin")).limit(2)
        assertEquals(
            pg(a.union(b)),
            lines(
                "(",
                "    SELECT",
                """        "id" AS "id"""",
                "    FROM",
                """        "user"""",
                "    LIMIT 1",
                ")",
                "UNION",
                "(",
                "    SELECT",
                """        "id" AS "id"""",
                "    FROM",
                """        "admin"""",
                "    LIMIT 2",
                ")"
            )
        )

    test("select requires at least one item"):
        val caught = intercept[IllegalArgumentException](select())
        assertEquals(caught.getMessage, "requirement failed: Select requires at least one column")

    test("caseWhen requires at least one branch"):
        val caught = intercept[IllegalArgumentException](caseWhen())
        assertEquals(caught.getMessage, "requirement failed: CaseWhen requires at least one (condition, result) pair.")
