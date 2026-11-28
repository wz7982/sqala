package sqala.test

import sqala.static.dsl.*
import sqala.test.StaticFixture.*

/**
 * Window frame exclusion modes `EXCLUDE CURRENT ROW`, `EXCLUDE TIES`
 * and `EXCLUDE GROUP`, applied to single-bound and `BETWEEN` frames
 * of every frame unit.
 */
class W4_FrameExclude extends munit.FunSuite:
    test("rows between supports every exclusion mode"):
        assertEquals(
            sql(from(Wide).map(w => (
                a = count().over(partitionBy(w.id).sortBy(w.id).rowsBetween(1.preceding, currentRow).excludeCurrentRow),
                b = count().over(partitionBy(w.id).sortBy(w.id).rowsBetween(1.preceding, currentRow).excludeTies),
                c = count().over(partitionBy(w.id).sortBy(w.id).rowsBetween(1.preceding, currentRow).excludeGroup)
            ))),
            lines(
                "SELECT",
                """    COUNT(*) OVER (PARTITION BY "t1"."id" ORDER BY "t1"."id" ASC ROWS BETWEEN 1 PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW) AS "c1",""",
                """    COUNT(*) OVER (PARTITION BY "t1"."id" ORDER BY "t1"."id" ASC ROWS BETWEEN 1 PRECEDING AND CURRENT ROW EXCLUDE TIES) AS "c2",""",
                """    COUNT(*) OVER (PARTITION BY "t1"."id" ORDER BY "t1"."id" ASC ROWS BETWEEN 1 PRECEDING AND CURRENT ROW EXCLUDE GROUP) AS "c3"""",
                "FROM",
                """    "wide" AS "t1""""
            )
        )

    test("a single bound frame supports exclusion"):
        assertEquals(
            sql(from(Wide).map(w => count().over(sortBy(w.id).rows(currentRow).excludeCurrentRow))),
            lines(
                "SELECT",
                """    COUNT(*) OVER (ORDER BY "t1"."id" ASC ROWS CURRENT ROW EXCLUDE CURRENT ROW) AS "c1"""",
                "FROM",
                """    "wide" AS "t1""""
            )
        )

    test("range frames support exclusion"):
        assertEquals(
            sql(from(Wide).map(w => count().over(sortBy(w.birth).rangeBetween(1.day.preceding, currentRow).excludeTies))),
            lines(
                "SELECT",
                """    COUNT(*) OVER (ORDER BY "t1"."birth" ASC RANGE BETWEEN INTERVAL '1' DAY PRECEDING AND CURRENT ROW EXCLUDE TIES) AS "c1"""",
                "FROM",
                """    "wide" AS "t1""""
            )
        )

    test("groups frames support exclusion"):
        assertEquals(
            sql(from(Wide).map(w => count().over(sortBy(w.id).groupsBetween(1.preceding, currentRow).excludeGroup))),
            lines(
                "SELECT",
                """    COUNT(*) OVER (ORDER BY "t1"."id" ASC GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW EXCLUDE GROUP) AS "c1"""",
                "FROM",
                """    "wide" AS "t1""""
            )
        )

    test("unbounded frames support exclusion"):
        assertEquals(
            sql(from(Wide).map(w => count().over(sortBy(w.id).rowsBetween(unboundedPreceding, unboundedFollowing).excludeGroup))),
            lines(
                "SELECT",
                """    COUNT(*) OVER (ORDER BY "t1"."id" ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE GROUP) AS "c1"""",
                "FROM",
                """    "wide" AS "t1""""
            )
        )

    test("exclusion keeps the window result type"):
        assertEquals(compileErrors("from(Wide).map(w => expect[Long](count().over(sortBy(w.id).rows(currentRow).excludeTies)))"), "")
        assertEquals(compileErrors("from(Wide).map(w => expect[Option[BigDecimal]](sum(w.id).over(sortBy(w.id).rows(currentRow).excludeGroup)))"), "")

    test("exclusion requires a frame"):
        assert(compileErrors("from(Wide).map(w => count().over(sortBy(w.id).excludeCurrentRow))").nonEmpty)
        assert(compileErrors("from(Wide).map(w => count().over(partitionBy(w.id).excludeTies))").nonEmpty)

    test("exclusion cannot be applied twice"):
        assert(compileErrors("from(Wide).map(w => count().over(sortBy(w.id).rows(currentRow).excludeTies.excludeGroup))").nonEmpty)