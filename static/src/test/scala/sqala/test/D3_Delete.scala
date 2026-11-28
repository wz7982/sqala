package sqala.test

import sqala.dialect.PostgresqlDialect
import sqala.static.dsl.*
import sqala.test.StaticFixture.lines

/**
 * `DELETE` conditions. The condition rules are the same as for a query
 * filter, and repeated calls accumulate.
 */
class D3_Delete extends munit.FunSuite:
    test("a condition restricts the deleted rows"):
        assertEquals(
            delete[User].where(u => u.id === 1).sql(PostgresqlDialect),
            """DELETE FROM "user" AS "t1" WHERE "t1"."id" = 1"""
        )

    test("repeated conditions accumulate with AND"):
        assertEquals(
            delete[User].where(u => u.id === 1).where(u => u.age > 0).sql(PostgresqlDialect),
            """DELETE FROM "user" AS "t1" WHERE "t1"."id" = 1 AND "t1"."age" > 0"""
        )

    test("without a condition the delete covers every row"):
        assertEquals(
            delete[User].sql(PostgresqlDialect),
            """DELETE FROM "user" AS "t1""""
        )

    test("a condition may combine predicates and a subquery"):
        assertEquals(
            delete[User].where(u => u.age > 18 && exists(from(Post).filter(p => p.userId === u.id).map(p => p.id))).sql(PostgresqlDialect),
            lines(
                """DELETE FROM "user" AS "t1" WHERE "t1"."age" > 18 AND EXISTS(""",
                "    SELECT",
                """        "t2"."id" AS "c1"""",
                "    FROM",
                """        "post" AS "t2"""",
                "    WHERE",
                """        "t2"."user_id" = "t1"."id"""",
                ")"
            )
        )

    test("an optional column may be tested for null"):
        assertEquals(
            delete[Wide].where(w => w.score.isEmpty).sql(PostgresqlDialect),
            """DELETE FROM "wide" AS "t1" WHERE "t1"."score" IS NULL"""
        )

    test("a negated null test prints IS NOT NULL"):
        assertEquals(
            delete[Wide].where(w => !w.score.isEmpty).sql(PostgresqlDialect),
            """DELETE FROM "wide" AS "t1" WHERE "t1"."score" IS NOT NULL"""
        )

    test("the condition must be a boolean expression allowed in where"):
        assert(compileErrors("""delete[User].where(u => u.age)""")
            .startsWith("error: The arguments type should be boolean, but got scala.Int."))
        assert(compileErrors("""delete[User].where(u => count(u.id) > 1)""")
            .startsWith("error: Aggregate functions are not allowed in where."))