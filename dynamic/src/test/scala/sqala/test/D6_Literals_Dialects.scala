package sqala.test

import sqala.dynamic.dsl.*
import sqala.test.DynamicFixture.*

import java.time.*

/**
 * Literal rendering, the `value` / `inlinedValue` split, string escaping,
 * dialect-specific rendering, and the raw-SQL escape hatch.
 */
class D6_Literals_Dialects extends munit.FunSuite:
    test("values and inlined values render as literals in the core printer"):
        assertEquals(
            pg(select(value(1).as("a"), value("s").as("b"), value(true).as("c"), value(1.5f).as("d"), value(BigDecimal("2.25")).as("e"))),
            lines(
                "SELECT",
                """    1 AS "a",""",
                """    's' AS "b",""",
                """    TRUE AS "c",""",
                """    1.5 AS "d",""",
                """    2.25 AS "e""""
            )
        )
        assertEquals(
            pg(select(inlinedValue(1).as("a"), inlinedValue("s").as("b"), inlinedValue(true).as("c"))),
            lines(
                "SELECT",
                """    1 AS "a",""",
                """    's' AS "b",""",
                """    TRUE AS "c""""
            )
        )

    test("temporal literals render with their typed prefixes"):
        assertEquals(
            pg(select(inlinedValue(LocalDate.of(2026, 2, 1)).as("d"), inlinedValue(LocalDateTime.of(2026, 2, 1, 9, 30, 0)).as("ts"), inlinedValue(OffsetDateTime.parse("2026-02-01T09:30:00+08:00")).as("ots"), inlinedValue(LocalTime.of(9, 30)).as("tm"), inlinedValue(OffsetTime.parse("09:30:00+08:00")).as("otm"))),
            lines(
                "SELECT",
                """    DATE '2026-02-01' AS "d",""",
                """    TIMESTAMP '2026-02-01 09:30:00.000000000' AS "ts",""",
                """    TIMESTAMP WITH TIME ZONE '2026-02-01 09:30:00.000000000 +08:00' AS "ots",""",
                """    TIME '09:30:00.000000000' AS "tm",""",
                """    TIME WITH TIME ZONE '09:30:00.000000000 +08:00' AS "otm""""
            )
        )

    test("intervals and arrays render as typed literals"):
        assertEquals(
            pg(select(inlinedValue(2.year).as("iv"), inlinedValue(5.minute).as("mn"))),
            lines(
                "SELECT",
                """    INTERVAL '2' YEAR AS "iv",""",
                """    INTERVAL '5' MINUTE AS "mn""""
            )
        )
        assertEquals(
            pg(select(inlinedValue(Array(1, 2, 3)).as("arr"), inlinedValue(Array.empty[Int]).as("earr"))),
            lines(
                "SELECT",
                """    ARRAY[1, 2, 3] AS "arr",""",
                """    CAST(ARRAY[] AS INTEGER[]) AS "earr""""
            )
        )

    test("single quotes in string literals are doubled"):
        assertEquals(
            pg(select(inlinedValue("it''s").as("x"))),
            lines("SELECT", """    'it''''s' AS "x"""")
        )

    test("the escape flag controls backslash doubling in string literals"):
        assertEquals(pg(select(inlinedValue("a\\b").as("x"))), lines("SELECT", """    'a\b' AS "x""""))
        assertEquals(pgLegacy(select(inlinedValue("a\\b").as("x"))), lines("SELECT", """    'a\\b' AS "x""""))
        assertEquals(mysql(select(inlinedValue("a\\b").as("x"))), lines("SELECT", """    'a\\b' AS `x`"""))
        assertEquals(mysqlStandard(select(inlinedValue("a\\b").as("x"))), lines("SELECT", """    'a\b' AS `x`"""))

    test("postgres renders backtick-free identifiers with limit and offset"):
        val q = select(column("id").as("id")).from(table("user").as("u")).where(column("id") === value(1)).orderBy(column("id").asc).offset(10).limit(5)
        assertEquals(
            pg(q),
            lines(
                "SELECT",
                """    "id" AS "id"""",
                "FROM",
                """    "user" AS "u"""",
                "WHERE",
                """    "id" = 1""",
                "ORDER BY",
                """    "id" ASC""",
                "LIMIT 5 OFFSET 10"
            )
        )

    test("mysql renders backtick identifiers and offset-comma-limit"):
        val q = select(column("id").as("id")).from(table("user").as("u")).where(column("id") === value(1)).orderBy(column("id").asc).offset(10).limit(5)
        assertEquals(
            mysql(q),
            lines(
                "SELECT",
                """    `id` AS `id`""",
                "FROM",
                """    `user` AS `u`""",
                "WHERE",
                """    `id` = 1""",
                "ORDER BY",
                """    `id` ASC""",
                "LIMIT 10, 5"
            )
        )

    test("oracle drops the as keyword in table aliases and uses fetch syntax"):
        val q = select(column("id").as("id")).from(table("user").as("u")).where(column("id") === value(1)).orderBy(column("id").asc).offset(10).limit(5)
        assertEquals(
            oracle(q),
            lines(
                "SELECT",
                """    "id" AS "id"""",
                "FROM",
                """    "user" "u"""",
                "WHERE",
                """    "id" = 1""",
                "ORDER BY",
                """    "id" ASC""",
                "OFFSET 10 ROWS FETCH NEXT 5 ROWS ONLY"
            )
        )
        assertEquals(
            oracle(select(column("id").as("id")).from(table("user")).limit(5)),
            lines(
                "SELECT",
                """    "id" AS "id"""",
                "FROM",
                """    "user"""",
                "FETCH NEXT 5 ROWS ONLY"
            )
        )

    test("sqlserver renders bracket identifiers and fetch syntax"):
        val q = select(column("id").as("id")).from(table("user").as("u")).where(column("id") === value(1)).orderBy(column("id").asc).offset(10).limit(5)
        assertEquals(
            sqlserver(q),
            lines(
                "SELECT",
                "    [id] AS [id]",
                "FROM",
                "    [user] AS [u]",
                "WHERE",
                "    [id] = 1",
                "ORDER BY",
                "    [id] ASC",
                "OFFSET 10 ROWS FETCH NEXT 5 ROWS ONLY"
            )
        )

    test("sqlite mirrors postgres limit syntax and h2 uses fetch syntax"):
        val q = select(column("id").as("id")).from(table("user").as("u")).where(column("id") === value(1)).orderBy(column("id").asc).offset(10).limit(5)
        assertEquals(
            sqlite(q),
            lines(
                "SELECT",
                """    "id" AS "id"""",
                "FROM",
                """    "user" AS "u"""",
                "WHERE",
                """    "id" = 1""",
                "ORDER BY",
                """    "id" ASC""",
                "LIMIT 5 OFFSET 10"
            )
        )
        assertEquals(
            h2(q),
            lines(
                "SELECT",
                """    "id" AS "id"""",
                "FROM",
                """    "user" AS "u"""",
                "WHERE",
                """    "id" = 1""",
                "ORDER BY",
                """    "id" ASC""",
                "OFFSET 10 ROWS FETCH NEXT 5 ROWS ONLY"
            )
        )

    test("unsafeExpr splices parsed raw SQL as an expression"):
        assertEquals(
            pg(select(unsafeExpr("""lower("u"."name") || '!'""").as("name")).from(table("user").as("u"))),
            lines(
                "SELECT",
                """    lower("u"."name") || '!' AS "name"""",
                "FROM",
                """    "user" AS "u""""
            )
        )
        assertEquals(
            pg(select(unsafeExpr("\"u\".\"age\" + 1").as("a")).from(table("user").as("u"))),
            lines(
                "SELECT",
                """    "u"."age" + 1 AS "a"""",
                "FROM",
                """    "user" AS "u""""
            )
        )

    test("unsafeExpr rejects syntactically invalid fragments at runtime"):
        val caught = intercept[sqala.parser.ParseException](unsafeExpr("from where"))
        assert(caught.getMessage.startsWith("Expected an expression, found keyword 'from'"))
