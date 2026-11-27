package sqala.test

import sqala.dynamic.dsl.*
import sqala.test.DynamicFixture.*

/**
 * Regression tests for previously triaged defects: the `ntile` bucket
 * count argument and the documented `value(n.unit)` interval arithmetic
 * form.
 */
class D7_Defects extends munit.FunSuite:
    test("ntile renders its bucket-count argument"):
        assertEquals(
            pg(select(ntile(value(4)).over(orderBy(column("id").asc)).as("a")).from(table("t"))),
            lines(
                "SELECT",
                """    NTILE(4) OVER (ORDER BY "id" ASC) AS "a"""",
                "FROM",
                """    "t""""
            )
        )

    test("documented interval arithmetic compiles and renders"):
        assert(compileErrors("""select((column("create_time") + value(1.year)).as("x"))""").isEmpty)
        assertEquals(
            pg(select((column("create_time") + value(1.year)).as("x")).from(table("t"))),
            lines(
                "SELECT",
                """    "create_time" + INTERVAL '1' YEAR AS "x"""",
                "FROM",
                """    "t""""
            )
        )
