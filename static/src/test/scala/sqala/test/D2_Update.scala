package sqala.test

import sqala.dialect.PostgresqlDialect
import sqala.static.dsl.*
import sqala.static.dsl.statement.dml.Update
import sqala.test.StaticFixture.lines

/**
 * `UPDATE` assignments and conditions, both through the builder and
 * through an entity whose primary key becomes the condition.
 */
class D2_Update extends munit.FunSuite:
    test("each set call adds one assignment"):
        assertEquals(
            update[User].set(u => u.name := "x").set(u => u.age := 1).where(u => u.id === 1).sql(PostgresqlDialect),
            """UPDATE "user" AS "t1" SET "name" = 'x', "age" = 1 WHERE "t1"."id" = 1"""
        )

    test("the assigned expression may reference the updated table"):
        assertEquals(
            update[User].set(u => u.age := u.age + 1).where(u => u.id === 1).sql(PostgresqlDialect),
            """UPDATE "user" AS "t1" SET "age" = "t1"."age" + 1 WHERE "t1"."id" = 1"""
        )

    test("the assigned expression may be a scalar subquery"):
        assertEquals(
            update[User].set(u => u.age := from(User).map(x => x.age).take(1)).sql(PostgresqlDialect),
            lines(
                """UPDATE "user" AS "t1" SET "age" = (""",
                "    SELECT",
                """        "t2"."age" AS "c1"""",
                "    FROM",
                """        "user" AS "t2"""",
                "    LIMIT 1",
                ")"
            )
        )

    test("an optional column may be set to null"):
        assertEquals(
            update[Wide].set(w => w.score := (None: Option[Long])).where(w => w.id === 1).sql(PostgresqlDialect),
            """UPDATE "wide" AS "t1" SET "score" = CAST(NULL AS BIGINT) WHERE "t1"."id" = 1"""
        )

    test("an optional column may be set to a present value"):
        assertEquals(
            update[Wide].set(w => w.score := (Some(2L): Option[Long])).where(w => w.id === 1).sql(PostgresqlDialect),
            """UPDATE "wide" AS "t1" SET "score" = 2 WHERE "t1"."id" = 1"""
        )

    test("without a condition the update covers every row"):
        assertEquals(
            update[User].set(u => u.age := 0).sql(PostgresqlDialect),
            """UPDATE "user" AS "t1" SET "age" = 0"""
        )

    test("repeated conditions accumulate with AND"):
        assertEquals(
            update[User].set(u => u.age := 0).where(u => u.id === 1).where(u => u.age > 0).sql(PostgresqlDialect),
            """UPDATE "user" AS "t1" SET "age" = 0 WHERE "t1"."id" = 1 AND "t1"."age" > 0"""
        )

    test("an entity updates its non-key fields and matches on its key"):
        assertEquals(
            Update.updateByEntity(Account(1, "a", 10L, Some("m"))).sql(PostgresqlDialect),
            """UPDATE "account" AS "t1" SET "name" = 'a', "balance" = 10, "memo" = 'm' WHERE "t1"."id" = 1"""
        )

    test("a composite key becomes a conjunction"):
        assertEquals(
            Update.updateByEntity(Link(1, 2, "x")).sql(PostgresqlDialect),
            """UPDATE "link" AS "t1" SET "label" = 'x' WHERE "t1"."src" = 1 AND "t1"."dst" = 2"""
        )

    test("an entity update needs a key and a non-key column"):
        interceptMessage[IllegalArgumentException]("requirement failed: Update table must have at least one primary key column."):
            Update.updateByEntity(User(1, "a", 20)).sql(PostgresqlDialect)
        interceptMessage[IllegalArgumentException]("requirement failed: Update table must have at least one set column."):
            Update.updateByEntity(Marker(1)).sql(PostgresqlDialect)

    test("skipNone omits the null assignments"):
        assertEquals(
            Update.updateByEntity(Account(1, "a", 10L, None), true).sql(PostgresqlDialect),
            """UPDATE "account" AS "t1" SET "name" = 'a', "balance" = 10 WHERE "t1"."id" = 1"""
        )
        assertEquals(
            Update.updateByEntity(Profile(1, None, Some(2L)), true).sql(PostgresqlDialect),
            """UPDATE "profile" AS "t1" SET "score" = 2 WHERE "t1"."id" = 1"""
        )

    test("without skipNone the null assignments are kept"):
        assertEquals(
            Update.updateByEntity(Profile(1, None, None)).sql(PostgresqlDialect),
            """UPDATE "profile" AS "t1" SET "nickname" = CAST(NULL AS VARCHAR), "score" = CAST(NULL AS BIGINT) WHERE "t1"."id" = 1"""
        )
        assertEquals(
            Update.updateByEntity(Profile(1, Some("n"), Some(2L)), true).sql(PostgresqlDialect),
            """UPDATE "profile" AS "t1" SET "nickname" = 'n', "score" = 2 WHERE "t1"."id" = 1"""
        )

    test("skipping every assignment leaves nothing to update"):
        interceptMessage[IllegalArgumentException]("requirement failed: Update table must have at least one set column."):
            Update.updateByEntity(Profile(1, None, None), true).sql(PostgresqlDialect)

    test("the assigned type must match the column type"):
        assert(compileErrors("""update[User].set(u => u.name := 1)""").startsWith("error: Cannot compare types java.lang.String and scala.Int."))
        assert(compileErrors("""update[User].set(u => u.age := "x")""").startsWith("error: Cannot compare types scala.Int and java.lang.String."))
        assert(compileErrors("""update[Wide].set(w => w.score := "x")""").startsWith("error: Cannot compare types scala.Option[scala.Long] and java.lang.String."))
        assertEquals(compileErrors("""update[Wide].set(w => w.score := (Some(1L): Option[Long]))"""), "")

    test("a numeric assignment widens like a comparison does"):
        assertEquals(
            update[Wide].set(w => w.score := 1).sql(PostgresqlDialect),
            """UPDATE "wide" AS "t1" SET "score" = 1"""
        )
        assertEquals(
            update[User].set(u => u.id := 1L).sql(PostgresqlDialect),
            """UPDATE "user" AS "t1" SET "id" = 1"""
        )

    test("only a column may be assigned to"):
        assert(compileErrors("""update[User].set(u => (u.age + 1) := 2)""").nonEmpty)
        assert(compileErrors("""update[User].set(u => 1 := 2)""").nonEmpty)

    test("the condition must be a boolean expression allowed in where"):
        assert(compileErrors("""update[User].set(u => u.age := 1).where(u => u.age)""")
            .startsWith("error: The arguments type should be boolean, but got scala.Int."))
        assert(compileErrors("""update[User].set(u => u.age := 1).where(u => count(u.id) > 1)""")
            .startsWith("error: Aggregate functions are not allowed in where."))

    test("an entity update accepts no further assignments"):
        assert(compileErrors("""Update.updateByEntity(Account(1, "a", 1L, None)).set(a => a.name := "b")""")
            .startsWith("error: Cannot prove that sqala.static.dsl.statement.dml.UpdateEntity =:= sqala.static.dsl.statement.dml.UpdateTable."))

    test("an entity update accepts no further conditions"):
        assert(compileErrors("""Update.updateByEntity(Account(1, "a", 1L, None)).where(a => a.id === 1)""")
            .startsWith("error: Cannot prove that sqala.static.dsl.statement.dml.UpdateEntity =:= sqala.static.dsl.statement.dml.UpdateTable."))

    test("an assignment rejects aggregates and window functions"):
        assert(compileErrors("""update[User].set(u => u.age := max(u.age))""")
            .startsWith("error: Aggregate functions are not allowed in set."))
        assert(compileErrors("""update[User].set(u => u.age := rowNumber().over())""")
            .startsWith("error: Window functions are not allowed in set."))
        assert(compileErrors("""update[User].set(u => u.age := count().over(partitionBy(u.id)))""")
            .startsWith("error: Window functions are not allowed in set."))

    test("an update without assignments is rejected"):
        interceptMessage[IllegalArgumentException]("requirement failed: Update table must have at least one set column."):
            update[User].where(u => u.id === 1).sql(PostgresqlDialect)
        interceptMessage[IllegalArgumentException]("requirement failed: Update table must have at least one set column."):
            update[User].sql(PostgresqlDialect)