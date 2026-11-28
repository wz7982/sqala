package sqala.test

import sqala.dialect.PostgresqlDialect
import sqala.static.dsl.*
import sqala.static.dsl.statement.dml.Insert
import sqala.test.StaticFixture.lines

/**
 * `INSERT` in all its data sources: a single row, a batch, entity objects
 * with auto-increment columns excluded, and a query. The builder state
 * machine and the column-only restriction are checked as well.
 */
class D1_Insert extends munit.FunSuite:
    test("a single row inserts the selected columns in selection order"):
        assertEquals(
            insert[User](u => (u.id, u.name)).values((1, "Alice")).sql(PostgresqlDialect),
            """INSERT INTO "user" ("id", "name") VALUES (1, 'Alice')"""
        )

    test("a batch inserts one value list per row"):
        assertEquals(
            insert[User](u => (u.id, u.name)).values(List((1, "Alice"), (2, "Bob"))).sql(PostgresqlDialect),
            """INSERT INTO "user" ("id", "name") VALUES (1, 'Alice'), (2, 'Bob')"""
        )

    test("a single column needs no tuple"):
        assertEquals(
            insert[User](u => u.name).values("Alice").sql(PostgresqlDialect),
            """INSERT INTO "user" ("name") VALUES ('Alice')"""
        )

    test("the column list follows selection order, not declaration order"):
        assertEquals(
            insert[User](u => (u.age, u.id)).values((20, 1)).sql(PostgresqlDialect),
            """INSERT INTO "user" ("age", "id") VALUES (20, 1)"""
        )

    test("an optional column accepts both Some and None"):
        assertEquals(
            insert[Wide](w => (w.id, w.score)).values(List((1, Some(2L)), (2, None))).sql(PostgresqlDialect),
            """INSERT INTO "wide" ("id", "score") VALUES (1, 2), (2, CAST(NULL AS BIGINT))"""
        )

    test("entities exclude the auto-increment column"):
        assertEquals(
            Insert.insertByEntities(Seq(Account(1, "a", 10L, Some("m")), Account(2, "b", 20L, None))).sql(PostgresqlDialect),
            """INSERT INTO "account" ("name", "balance", "memo") VALUES ('a', 10, 'm'), ('b', 20, CAST(NULL AS VARCHAR))"""
        )

    test("an entity without an auto-increment column inserts every field"):
        assertEquals(
            Insert.insertByEntities(Seq(Link(1, 2, "x"))).sql(PostgresqlDialect),
            """INSERT INTO "link" ("src", "dst", "label") VALUES (1, 2, 'x')"""
        )

    test("a query supplies the rows"):
        assertEquals(
            insert[Post](p => (p.userId, p.title, p.viewCount)).query(from(User).map(u => (u.id, u.name, u.age))).sql(PostgresqlDialect),
            lines(
                """INSERT INTO "post" ("user_id", "title", "view_count") SELECT""",
                """    "t1"."id" AS "c1",""",
                """    "t1"."name" AS "c2",""",
                """    "t1"."age" AS "c3"""",
                "FROM",
                """    "user" AS "t1""""
            )
        )

    test("the source query keeps its own clauses"):
        assertEquals(
            insert[Post](p => (p.userId, p.title, p.viewCount))
                .query(from(User).filter(u => u.age > 18).map(u => (u.id, u.name, u.age)).sortBy(u => u.id).take(10))
                .sql(PostgresqlDialect),
            lines(
                """INSERT INTO "post" ("user_id", "title", "view_count") SELECT""",
                """    "t1"."id" AS "c1",""",
                """    "t1"."name" AS "c2",""",
                """    "t1"."age" AS "c3"""",
                "FROM",
                """    "user" AS "t1"""",
                "WHERE",
                """    "t1"."age" > 18""",
                "ORDER BY",
                """    "t1"."id" ASC""",
                "LIMIT 10"
            )
        )

    test("only columns may be selected"):
        assert(compileErrors("""insert[User](u => 1)""").startsWith("error: This value cannot be converted to an insert column list."))
        assert(compileErrors("""insert[User](u => u.id + 1)""").nonEmpty)
        assert(compileErrors("""insert[User](u => max(u.age))""").nonEmpty)
        assert(compileErrors("""insert[User](u => (u.id, u.name + "x"))""").nonEmpty)

    test("the value types must match the selected columns"):
        assert(compileErrors("""insert[User](u => (u.id, u.name)).values((1, 2))""").nonEmpty)
        assert(compileErrors("""insert[User](u => (u.id, u.name)).values((1, "a", 2))""").nonEmpty)
        assert(compileErrors("""insert[User](u => u.name).values(1)""").nonEmpty)
        assertEquals(compileErrors("""insert[User](u => (u.id, u.name)).values((1, "a"))"""), "")

    test("the source query result must match the selected columns"):
        assert(compileErrors("""insert[User](u => (u.id, u.name)).query(from(User).map(u => u.id))""").nonEmpty)
        assert(compileErrors("""insert[User](u => (u.id, u.name)).query(from(User).map(u => (u.name, u.id)))""").nonEmpty)
        assertEquals(compileErrors("""insert[User](u => (u.id, u.name)).query(from(User).map(u => (u.id, u.name)))"""), "")

    test("a data source may only be given once"):
        assert(compileErrors("""insert[User](u => u.name).values("a").values("b")""")
            .startsWith("error: Cannot prove that sqala.static.dsl.statement.dml.InsertValues =:= sqala.static.dsl.statement.dml.InsertTable."))
        assert(compileErrors("""insert[User](u => u.name).values("a").query(from(User).map(u => u.name))""")
            .startsWith("error: Cannot prove that sqala.static.dsl.statement.dml.InsertValues =:= sqala.static.dsl.statement.dml.InsertTable."))
        assert(compileErrors("""insert[User](u => u.name).query(from(User).map(u => u.name)).values("a")""")
            .startsWith("error: Cannot prove that sqala.static.dsl.statement.dml.InsertQuery =:= sqala.static.dsl.statement.dml.InsertTable."))

    test("an empty row list is rejected"):
        interceptMessage[IllegalArgumentException]("requirement failed: Insert table must have at least one row."):
            insert[User](u => (u.id, u.name)).values(Nil).sql(PostgresqlDialect)
        interceptMessage[IllegalArgumentException]("requirement failed: Insert table must have at least one row."):
            Insert.insertByEntities(Seq.empty[Account]).sql(PostgresqlDialect)