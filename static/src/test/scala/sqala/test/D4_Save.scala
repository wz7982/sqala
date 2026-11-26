package sqala.test

import sqala.dialect.PostgresqlDialect
import sqala.static.dsl.statement.dml.Save

/**
 * Upsert from an entity. The conflict target is the primary key and every
 * other column becomes an update assignment, so a table needs both a key
 * and at least one non-key column.
 */
class D4_Save extends munit.FunSuite:
    test("the key is the conflict target and the other columns are updated"):
        assertEquals(
            Save.saveByEntity(Account(1, "a", 10L, Some("m"))).sql(PostgresqlDialect),
            """INSERT INTO "account" ("id", "name", "balance", "memo") VALUES (1, 'a', 10, 'm') ON CONFLICT ("id") DO UPDATE SET "name" = EXCLUDED."name", "balance" = EXCLUDED."balance", "memo" = EXCLUDED."memo""""
        )

    test("a composite key is the conflict target"):
        assertEquals(
            Save.saveByEntity(Link(1, 2, "x")).sql(PostgresqlDialect),
            """INSERT INTO "link" ("src", "dst", "label") VALUES (1, 2, 'x') ON CONFLICT ("src", "dst") DO UPDATE SET "label" = EXCLUDED."label""""
        )

    test("the auto-increment column is still inserted and kept out of the update"):
        assertEquals(
            Save.saveByEntity(Account(1, "a", 10L, None)).sql(PostgresqlDialect),
            """INSERT INTO "account" ("id", "name", "balance", "memo") VALUES (1, 'a', 10, CAST(NULL AS VARCHAR)) ON CONFLICT ("id") DO UPDATE SET "name" = EXCLUDED."name", "balance" = EXCLUDED."balance", "memo" = EXCLUDED."memo""""
        )

    test("a table without a primary key is rejected"):
        interceptMessage[IllegalArgumentException]("requirement failed: Save table must have at least one primary key column."):
            Save.saveByEntity(User(1, "a", 20)).sql(PostgresqlDialect)

    test("a table whose columns are all key columns is rejected"):
        interceptMessage[IllegalArgumentException]("requirement failed: Save table must have at least one update column."):
            Save.saveByEntity(Marker(1)).sql(PostgresqlDialect)