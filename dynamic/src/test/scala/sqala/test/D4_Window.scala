package sqala.test

import sqala.dynamic.dsl.*
import sqala.test.DynamicFixture.*

/**
 * Window functions: empty and specified OVER clauses, partition and
 * order combination, frame units, bounds, and exclusion modes.
 */
class D4_Window extends munit.FunSuite:
    test("over with no argument renders an empty window"):
        assertEquals(
            pg(select(count().over().as("a")).from(table("t"))),
            lines(
                "SELECT",
                """    COUNT(*) OVER () AS "a"""",
                "FROM",
                """    "t""""
            )
        )

    test("over renders partition and order clauses"):
        assertEquals(
            pg(select(rank().over(partitionBy(column("a")).orderBy(column("b").asc)).as("r")).from(table("t"))),
            lines(
                "SELECT",
                """    RANK() OVER (PARTITION BY "a" ORDER BY "b" ASC) AS "r"""",
                "FROM",
                """    "t""""
            )
        )

    test("over accepts list overloads of partitionBy and orderBy"):
        assertEquals(
            pg(select(sum(column("n")).over(partitionBy(List(column("a"))).orderBy(List(column("b").desc))).as("s")).from(table("t"))),
            lines(
                "SELECT",
                """    SUM("n") OVER (PARTITION BY "a" ORDER BY "b" DESC) AS "s"""",
                "FROM",
                """    "t""""
            )
        )

    test("orderBy composes onto a partitionBy specification"):
        assertEquals(
            pg(select(sum(column("n")).over(partitionBy(column("a")).orderBy(column("b").asc, column("c").desc)).as("s")).from(table("t"))),
            lines(
                "SELECT",
                """    SUM("n") OVER (PARTITION BY "a" ORDER BY "b" ASC, "c" DESC) AS "s"""",
                "FROM",
                """    "t""""
            )
        )

    test("rows frames render start-only and between forms"):
        assertEquals(
            pg(select(sum(column("n")).over(orderBy(column("id").asc).rows(unboundedPreceding)).as("s")).from(table("t"))),
            lines(
                "SELECT",
                """    SUM("n") OVER (ORDER BY "id" ASC ROWS UNBOUNDED PRECEDING) AS "s"""",
                "FROM",
                """    "t""""
            )
        )
        assertEquals(
            pg(select(sum(column("n")).over(orderBy(column("id").asc).rowsBetween(value(1).preceding, value(1).following)).as("s")).from(table("t"))),
            lines(
                "SELECT",
                """    SUM("n") OVER (ORDER BY "id" ASC ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS "s"""",
                "FROM",
                """    "t""""
            )
        )

    test("range and groups frames render their units"):
        assertEquals(
            pg(select(avg(column("n")).over(orderBy(column("id").asc).range(unboundedPreceding)).as("s"), avg(column("n")).over(orderBy(column("id").asc).rangeBetween(unboundedPreceding, currentRow)).as("t")).from(table("t"))),
            lines(
                "SELECT",
                """    AVG("n") OVER (ORDER BY "id" ASC RANGE UNBOUNDED PRECEDING) AS "s",""",
                """    AVG("n") OVER (ORDER BY "id" ASC RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "t"""",
                "FROM",
                """    "t""""
            )
        )
        assertEquals(
            pg(select(sum(column("n")).over(orderBy(column("id").asc).groups(unboundedPreceding)).as("s"), sum(column("n")).over(orderBy(column("id").asc).groupsBetween(currentRow, unboundedFollowing)).as("t")).from(table("t"))),
            lines(
                "SELECT",
                """    SUM("n") OVER (ORDER BY "id" ASC GROUPS UNBOUNDED PRECEDING) AS "s",""",
                """    SUM("n") OVER (ORDER BY "id" ASC GROUPS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS "t"""",
                "FROM",
                """    "t""""
            )
        )

    test("frame exclusion modes render on between frames"):
        assertEquals(
            pg(select(sum(column("n")).over(orderBy(column("id").asc).rowsBetween(unboundedPreceding, currentRow).excludeCurrentRow).as("a"), sum(column("n")).over(orderBy(column("id").asc).rowsBetween(unboundedPreceding, currentRow).excludeTies).as("b"), sum(column("n")).over(orderBy(column("id").asc).rowsBetween(unboundedPreceding, currentRow).excludeGroup).as("c")).from(table("t"))),
            lines(
                "SELECT",
                """    SUM("n") OVER (ORDER BY "id" ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW) AS "a",""",
                """    SUM("n") OVER (ORDER BY "id" ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE TIES) AS "b",""",
                """    SUM("n") OVER (ORDER BY "id" ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE GROUP) AS "c"""",
                "FROM",
                """    "t""""
            )
        )

    test("exclusion modes are dropped when no frame is set"):
        assertEquals(
            pg(select(sum(column("n")).over(orderBy(column("id").asc).excludeGroup).as("s")).from(table("t"))),
            lines(
                "SELECT",
                """    SUM("n") OVER (ORDER BY "id" ASC) AS "s"""",
                "FROM",
                """    "t""""
            )
        )

    test("value and ordinal window functions render ignore-nulls variants"):
        assertEquals(
            pg(select(firstValue(column("n")).over(orderBy(column("id").asc)).as("a"), firstValueIgnoreNulls(column("n")).over(orderBy(column("id").asc)).as("b"), lastValueIgnoreNulls(column("n")).over(orderBy(column("id").asc)).as("c")).from(table("t"))),
            lines(
                "SELECT",
                """    FIRST_VALUE("n") OVER (ORDER BY "id" ASC) AS "a",""",
                """    FIRST_VALUE("n") IGNORE NULLS OVER (ORDER BY "id" ASC) AS "b",""",
                """    LAST_VALUE("n") IGNORE NULLS OVER (ORDER BY "id" ASC) AS "c"""",
                "FROM",
                """    "t""""
            )
        )
        assertEquals(
            pg(select(nthValue(column("n"), value(2)).over(orderBy(column("id").asc)).as("a"), nthValueFromLastIgnoreNulls(column("n"), value(2)).over(orderBy(column("id").asc)).as("b")).from(table("t"))),
            lines(
                "SELECT",
                """    NTH_VALUE("n", 2) OVER (ORDER BY "id" ASC) AS "a",""",
                """    NTH_VALUE("n", 2) FROM LAST IGNORE NULLS OVER (ORDER BY "id" ASC) AS "b"""",
                "FROM",
                """    "t""""
            )
        )

    test("lag and lead render their arity and ignore-nulls variants"):
        assertEquals(
            pg(select(lag(column("n")).over(orderBy(column("id").asc)).as("a"), lag(column("n"), value(2)).over(orderBy(column("id").asc)).as("b"), lagIgnoreNulls(column("n"), value(2), value(0)).over(orderBy(column("id").asc)).as("c"), lead(column("n"), value(2), value(0)).over(orderBy(column("id").asc)).as("d"), leadIgnoreNulls(column("n")).over(orderBy(column("id").asc)).as("e")).from(table("t"))),
            lines(
                "SELECT",
                """    LAG("n") OVER (ORDER BY "id" ASC) AS "a",""",
                """    LAG("n", 2) OVER (ORDER BY "id" ASC) AS "b",""",
                """    LAG("n", 2, 0) IGNORE NULLS OVER (ORDER BY "id" ASC) AS "c",""",
                """    LEAD("n", 2, 0) OVER (ORDER BY "id" ASC) AS "d",""",
                """    LEAD("n") IGNORE NULLS OVER (ORDER BY "id" ASC) AS "e"""",
                "FROM",
                """    "t""""
            )
        )

    test("ordering helpers cover the six asc and desc nulls placements"):
        assertEquals(
            pg(select(column("id").as("id")).from(table("t")).orderBy(column("a").ascNullsLast, column("b").descNullsLast, column("c").descNullsFirst)),
            lines(
                "SELECT",
                """    "id" AS "id"""",
                "FROM",
                """    "t"""",
                "ORDER BY",
                """    "a" ASC NULLS LAST,""",
                """    "b" DESC NULLS LAST,""",
                """    "c" DESC NULLS FIRST"""
            )
        )
