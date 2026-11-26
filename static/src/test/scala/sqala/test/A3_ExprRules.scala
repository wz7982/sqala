package sqala.test

import sqala.static.dsl.*
import sqala.test.StaticFixture.{lines, messages, sql}

/**
 * Expression-level semantic rules: window frame ordering and unit types,
 * variable-free frame bounds, aggregate filters, grouping expressions,
 * and the clause-specific restrictions on scalar values.
 */
class A3_ExprRules extends munit.FunSuite:
    test("a rows frame accepts every non-reversed bound pair"):
        assertEquals(
            sql(from(User).map(u => (
                a = count().over(partitionBy(u.id).sortBy(u.id).rowsBetween(unboundedPreceding, 1.preceding)),
                b = count().over(partitionBy(u.id).sortBy(u.id).rowsBetween(unboundedPreceding, currentRow)),
                c = count().over(partitionBy(u.id).sortBy(u.id).rowsBetween(unboundedPreceding, 1.following)),
                d = count().over(partitionBy(u.id).sortBy(u.id).rowsBetween(unboundedPreceding, unboundedFollowing)),
                e = count().over(partitionBy(u.id).sortBy(u.id).rowsBetween(1.preceding, currentRow)),
                f = count().over(partitionBy(u.id).sortBy(u.id).rowsBetween(1.preceding, 1.following)),
                g = count().over(partitionBy(u.id).sortBy(u.id).rowsBetween(1.preceding, unboundedFollowing)),
                h = count().over(partitionBy(u.id).sortBy(u.id).rowsBetween(currentRow, 1.following)),
                i = count().over(partitionBy(u.id).sortBy(u.id).rowsBetween(currentRow, unboundedFollowing)),
                j = count().over(partitionBy(u.id).sortBy(u.id).rowsBetween(1.following, unboundedFollowing))
            ))),
            lines(
                "SELECT",
                """    COUNT(*) OVER (PARTITION BY "t1"."id" ORDER BY "t1"."id" ASC ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS "c1",""",
                """    COUNT(*) OVER (PARTITION BY "t1"."id" ORDER BY "t1"."id" ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "c2",""",
                """    COUNT(*) OVER (PARTITION BY "t1"."id" ORDER BY "t1"."id" ASC ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) AS "c3",""",
                """    COUNT(*) OVER (PARTITION BY "t1"."id" ORDER BY "t1"."id" ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "c4",""",
                """    COUNT(*) OVER (PARTITION BY "t1"."id" ORDER BY "t1"."id" ASC ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS "c5",""",
                """    COUNT(*) OVER (PARTITION BY "t1"."id" ORDER BY "t1"."id" ASC ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS "c6",""",
                """    COUNT(*) OVER (PARTITION BY "t1"."id" ORDER BY "t1"."id" ASC ROWS BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING) AS "c7",""",
                """    COUNT(*) OVER (PARTITION BY "t1"."id" ORDER BY "t1"."id" ASC ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS "c8",""",
                """    COUNT(*) OVER (PARTITION BY "t1"."id" ORDER BY "t1"."id" ASC ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS "c9",""",
                """    COUNT(*) OVER (PARTITION BY "t1"."id" ORDER BY "t1"."id" ASC ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) AS "c10"""",
                "FROM",
                """    "user" AS "t1""""
            )
        )

    test("a rows frame accepts equal bound kinds"):
        assertEquals(
            sql(from(User).map(u => (
                a = count().over(partitionBy(u.id).sortBy(u.id).rowsBetween(unboundedPreceding, unboundedPreceding)),
                b = count().over(partitionBy(u.id).sortBy(u.id).rowsBetween(2.preceding, 1.preceding)),
                c = count().over(partitionBy(u.id).sortBy(u.id).rowsBetween(currentRow, currentRow)),
                d = count().over(partitionBy(u.id).sortBy(u.id).rowsBetween(1.following, 2.following)),
                e = count().over(partitionBy(u.id).sortBy(u.id).rowsBetween(unboundedFollowing, unboundedFollowing))
            ))),
            lines(
                "SELECT",
                """    COUNT(*) OVER (PARTITION BY "t1"."id" ORDER BY "t1"."id" ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED PRECEDING) AS "c1",""",
                """    COUNT(*) OVER (PARTITION BY "t1"."id" ORDER BY "t1"."id" ASC ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS "c2",""",
                """    COUNT(*) OVER (PARTITION BY "t1"."id" ORDER BY "t1"."id" ASC ROWS BETWEEN CURRENT ROW AND CURRENT ROW) AS "c3",""",
                """    COUNT(*) OVER (PARTITION BY "t1"."id" ORDER BY "t1"."id" ASC ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING) AS "c4",""",
                """    COUNT(*) OVER (PARTITION BY "t1"."id" ORDER BY "t1"."id" ASC ROWS BETWEEN UNBOUNDED FOLLOWING AND UNBOUNDED FOLLOWING) AS "c5"""",
                "FROM",
                """    "user" AS "t1""""
            )
        )

    test("a rows frame rejects every reversed bound pair"):
        assertEquals(
            messages(compileErrors("from(User).map(u => count().over(partitionBy(u.id).sortBy(u.id).rowsBetween(1.preceding, unboundedPreceding)))")),
            List("Frame starting from preceding row cannot end at unbounded preceding.")
        )
        assertEquals(
            messages(compileErrors("from(User).map(u => count().over(partitionBy(u.id).sortBy(u.id).rowsBetween(currentRow, unboundedPreceding)))")),
            List("Frame starting from current row cannot end at unbounded preceding.")
        )
        assertEquals(
            messages(compileErrors("from(User).map(u => count().over(partitionBy(u.id).sortBy(u.id).rowsBetween(currentRow, 1.preceding)))")),
            List("Frame starting from current row cannot end at preceding row.")
        )
        assertEquals(
            messages(compileErrors("from(User).map(u => count().over(partitionBy(u.id).sortBy(u.id).rowsBetween(1.following, unboundedPreceding)))")),
            List("Frame starting from following row cannot end at unbounded preceding.")
        )
        assertEquals(
            messages(compileErrors("from(User).map(u => count().over(partitionBy(u.id).sortBy(u.id).rowsBetween(1.following, 2.preceding)))")),
            List("Frame starting from following row cannot end at preceding row.")
        )
        assertEquals(
            messages(compileErrors("from(User).map(u => count().over(partitionBy(u.id).sortBy(u.id).rowsBetween(1.following, currentRow)))")),
            List("Frame starting from following row cannot end at current row.")
        )
        assertEquals(
            messages(compileErrors("from(User).map(u => count().over(partitionBy(u.id).sortBy(u.id).rowsBetween(unboundedFollowing, unboundedPreceding)))")),
            List("Frame starting from unbounded following cannot end at unbounded preceding.")
        )
        assertEquals(
            messages(compileErrors("from(User).map(u => count().over(partitionBy(u.id).sortBy(u.id).rowsBetween(unboundedFollowing, 1.preceding)))")),
            List("Frame starting from unbounded following cannot end at preceding row.")
        )
        assertEquals(
            messages(compileErrors("from(User).map(u => count().over(partitionBy(u.id).sortBy(u.id).rowsBetween(unboundedFollowing, currentRow)))")),
            List("Frame starting from unbounded following cannot end at current row.")
        )
        assertEquals(
            messages(compileErrors("from(User).map(u => count().over(partitionBy(u.id).sortBy(u.id).rowsBetween(unboundedFollowing, 1.following)))")),
            List("Frame starting from unbounded following cannot end at following row.")
        )

    test("a range frame accepts every non-reversed bound pair"):
        assertEquals(
            sql(from(User).map(u => (
                a = count().over(partitionBy(u.id).sortBy(u.id).rangeBetween(unboundedPreceding, 1.preceding)),
                b = count().over(partitionBy(u.id).sortBy(u.id).rangeBetween(unboundedPreceding, currentRow)),
                c = count().over(partitionBy(u.id).sortBy(u.id).rangeBetween(unboundedPreceding, 1.following)),
                d = count().over(partitionBy(u.id).sortBy(u.id).rangeBetween(unboundedPreceding, unboundedFollowing)),
                e = count().over(partitionBy(u.id).sortBy(u.id).rangeBetween(1.preceding, currentRow)),
                f = count().over(partitionBy(u.id).sortBy(u.id).rangeBetween(1.preceding, 1.following)),
                g = count().over(partitionBy(u.id).sortBy(u.id).rangeBetween(1.preceding, unboundedFollowing)),
                h = count().over(partitionBy(u.id).sortBy(u.id).rangeBetween(currentRow, 1.following)),
                i = count().over(partitionBy(u.id).sortBy(u.id).rangeBetween(currentRow, unboundedFollowing)),
                j = count().over(partitionBy(u.id).sortBy(u.id).rangeBetween(1.following, unboundedFollowing))
            ))),
            lines(
                "SELECT",
                """    COUNT(*) OVER (PARTITION BY "t1"."id" ORDER BY "t1"."id" ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS "c1",""",
                """    COUNT(*) OVER (PARTITION BY "t1"."id" ORDER BY "t1"."id" ASC RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "c2",""",
                """    COUNT(*) OVER (PARTITION BY "t1"."id" ORDER BY "t1"."id" ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) AS "c3",""",
                """    COUNT(*) OVER (PARTITION BY "t1"."id" ORDER BY "t1"."id" ASC RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "c4",""",
                """    COUNT(*) OVER (PARTITION BY "t1"."id" ORDER BY "t1"."id" ASC RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) AS "c5",""",
                """    COUNT(*) OVER (PARTITION BY "t1"."id" ORDER BY "t1"."id" ASC RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS "c6",""",
                """    COUNT(*) OVER (PARTITION BY "t1"."id" ORDER BY "t1"."id" ASC RANGE BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING) AS "c7",""",
                """    COUNT(*) OVER (PARTITION BY "t1"."id" ORDER BY "t1"."id" ASC RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) AS "c8",""",
                """    COUNT(*) OVER (PARTITION BY "t1"."id" ORDER BY "t1"."id" ASC RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS "c9",""",
                """    COUNT(*) OVER (PARTITION BY "t1"."id" ORDER BY "t1"."id" ASC RANGE BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) AS "c10"""",
                "FROM",
                """    "user" AS "t1""""
            )
        )

    test("a range frame accepts equal bound kinds"):
        assertEquals(
            sql(from(User).map(u => (
                a = count().over(partitionBy(u.id).sortBy(u.id).rangeBetween(unboundedPreceding, unboundedPreceding)),
                b = count().over(partitionBy(u.id).sortBy(u.id).rangeBetween(2.preceding, 1.preceding)),
                c = count().over(partitionBy(u.id).sortBy(u.id).rangeBetween(currentRow, currentRow)),
                d = count().over(partitionBy(u.id).sortBy(u.id).rangeBetween(1.following, 2.following)),
                e = count().over(partitionBy(u.id).sortBy(u.id).rangeBetween(unboundedFollowing, unboundedFollowing))
            ))),
            lines(
                "SELECT",
                """    COUNT(*) OVER (PARTITION BY "t1"."id" ORDER BY "t1"."id" ASC RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED PRECEDING) AS "c1",""",
                """    COUNT(*) OVER (PARTITION BY "t1"."id" ORDER BY "t1"."id" ASC RANGE BETWEEN 2 PRECEDING AND 1 PRECEDING) AS "c2",""",
                """    COUNT(*) OVER (PARTITION BY "t1"."id" ORDER BY "t1"."id" ASC RANGE BETWEEN CURRENT ROW AND CURRENT ROW) AS "c3",""",
                """    COUNT(*) OVER (PARTITION BY "t1"."id" ORDER BY "t1"."id" ASC RANGE BETWEEN 1 FOLLOWING AND 2 FOLLOWING) AS "c4",""",
                """    COUNT(*) OVER (PARTITION BY "t1"."id" ORDER BY "t1"."id" ASC RANGE BETWEEN UNBOUNDED FOLLOWING AND UNBOUNDED FOLLOWING) AS "c5"""",
                "FROM",
                """    "user" AS "t1""""
            )
        )

    test("a range frame rejects every reversed bound pair"):
        assertEquals(
            messages(compileErrors("from(User).map(u => count().over(partitionBy(u.id).sortBy(u.id).rangeBetween(1.preceding, unboundedPreceding)))")),
            List("Frame starting from preceding row cannot end at unbounded preceding.")
        )
        assertEquals(
            messages(compileErrors("from(User).map(u => count().over(partitionBy(u.id).sortBy(u.id).rangeBetween(currentRow, unboundedPreceding)))")),
            List("Frame starting from current row cannot end at unbounded preceding.")
        )
        assertEquals(
            messages(compileErrors("from(User).map(u => count().over(partitionBy(u.id).sortBy(u.id).rangeBetween(currentRow, 1.preceding)))")),
            List("Frame starting from current row cannot end at preceding row.")
        )
        assertEquals(
            messages(compileErrors("from(User).map(u => count().over(partitionBy(u.id).sortBy(u.id).rangeBetween(1.following, unboundedPreceding)))")),
            List("Frame starting from following row cannot end at unbounded preceding.")
        )
        assertEquals(
            messages(compileErrors("from(User).map(u => count().over(partitionBy(u.id).sortBy(u.id).rangeBetween(1.following, 2.preceding)))")),
            List("Frame starting from following row cannot end at preceding row.")
        )
        assertEquals(
            messages(compileErrors("from(User).map(u => count().over(partitionBy(u.id).sortBy(u.id).rangeBetween(1.following, currentRow)))")),
            List("Frame starting from following row cannot end at current row.")
        )
        assertEquals(
            messages(compileErrors("from(User).map(u => count().over(partitionBy(u.id).sortBy(u.id).rangeBetween(unboundedFollowing, unboundedPreceding)))")),
            List("Frame starting from unbounded following cannot end at unbounded preceding.")
        )
        assertEquals(
            messages(compileErrors("from(User).map(u => count().over(partitionBy(u.id).sortBy(u.id).rangeBetween(unboundedFollowing, 1.preceding)))")),
            List("Frame starting from unbounded following cannot end at preceding row.")
        )
        assertEquals(
            messages(compileErrors("from(User).map(u => count().over(partitionBy(u.id).sortBy(u.id).rangeBetween(unboundedFollowing, currentRow)))")),
            List("Frame starting from unbounded following cannot end at current row.")
        )
        assertEquals(
            messages(compileErrors("from(User).map(u => count().over(partitionBy(u.id).sortBy(u.id).rangeBetween(unboundedFollowing, 1.following)))")),
            List("Frame starting from unbounded following cannot end at following row.")
        )

    test("a groups frame accepts every non-reversed bound pair"):
        assertEquals(
            sql(from(User).map(u => (
                a = count().over(partitionBy(u.id).sortBy(u.id).groupsBetween(unboundedPreceding, 1.preceding)),
                b = count().over(partitionBy(u.id).sortBy(u.id).groupsBetween(unboundedPreceding, currentRow)),
                c = count().over(partitionBy(u.id).sortBy(u.id).groupsBetween(unboundedPreceding, 1.following)),
                d = count().over(partitionBy(u.id).sortBy(u.id).groupsBetween(unboundedPreceding, unboundedFollowing)),
                e = count().over(partitionBy(u.id).sortBy(u.id).groupsBetween(1.preceding, currentRow)),
                f = count().over(partitionBy(u.id).sortBy(u.id).groupsBetween(1.preceding, 1.following)),
                g = count().over(partitionBy(u.id).sortBy(u.id).groupsBetween(1.preceding, unboundedFollowing)),
                h = count().over(partitionBy(u.id).sortBy(u.id).groupsBetween(currentRow, 1.following)),
                i = count().over(partitionBy(u.id).sortBy(u.id).groupsBetween(currentRow, unboundedFollowing)),
                j = count().over(partitionBy(u.id).sortBy(u.id).groupsBetween(1.following, unboundedFollowing))
            ))),
            lines(
                "SELECT",
                """    COUNT(*) OVER (PARTITION BY "t1"."id" ORDER BY "t1"."id" ASC GROUPS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS "c1",""",
                """    COUNT(*) OVER (PARTITION BY "t1"."id" ORDER BY "t1"."id" ASC GROUPS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "c2",""",
                """    COUNT(*) OVER (PARTITION BY "t1"."id" ORDER BY "t1"."id" ASC GROUPS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) AS "c3",""",
                """    COUNT(*) OVER (PARTITION BY "t1"."id" ORDER BY "t1"."id" ASC GROUPS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "c4",""",
                """    COUNT(*) OVER (PARTITION BY "t1"."id" ORDER BY "t1"."id" ASC GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW) AS "c5",""",
                """    COUNT(*) OVER (PARTITION BY "t1"."id" ORDER BY "t1"."id" ASC GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS "c6",""",
                """    COUNT(*) OVER (PARTITION BY "t1"."id" ORDER BY "t1"."id" ASC GROUPS BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING) AS "c7",""",
                """    COUNT(*) OVER (PARTITION BY "t1"."id" ORDER BY "t1"."id" ASC GROUPS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS "c8",""",
                """    COUNT(*) OVER (PARTITION BY "t1"."id" ORDER BY "t1"."id" ASC GROUPS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS "c9",""",
                """    COUNT(*) OVER (PARTITION BY "t1"."id" ORDER BY "t1"."id" ASC GROUPS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) AS "c10"""",
                "FROM",
                """    "user" AS "t1""""
            )
        )

    test("a groups frame accepts equal bound kinds"):
        assertEquals(
            sql(from(User).map(u => (
                a = count().over(partitionBy(u.id).sortBy(u.id).groupsBetween(unboundedPreceding, unboundedPreceding)),
                b = count().over(partitionBy(u.id).sortBy(u.id).groupsBetween(2.preceding, 1.preceding)),
                c = count().over(partitionBy(u.id).sortBy(u.id).groupsBetween(currentRow, currentRow)),
                d = count().over(partitionBy(u.id).sortBy(u.id).groupsBetween(1.following, 2.following)),
                e = count().over(partitionBy(u.id).sortBy(u.id).groupsBetween(unboundedFollowing, unboundedFollowing))
            ))),
            lines(
                "SELECT",
                """    COUNT(*) OVER (PARTITION BY "t1"."id" ORDER BY "t1"."id" ASC GROUPS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED PRECEDING) AS "c1",""",
                """    COUNT(*) OVER (PARTITION BY "t1"."id" ORDER BY "t1"."id" ASC GROUPS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS "c2",""",
                """    COUNT(*) OVER (PARTITION BY "t1"."id" ORDER BY "t1"."id" ASC GROUPS BETWEEN CURRENT ROW AND CURRENT ROW) AS "c3",""",
                """    COUNT(*) OVER (PARTITION BY "t1"."id" ORDER BY "t1"."id" ASC GROUPS BETWEEN 1 FOLLOWING AND 2 FOLLOWING) AS "c4",""",
                """    COUNT(*) OVER (PARTITION BY "t1"."id" ORDER BY "t1"."id" ASC GROUPS BETWEEN UNBOUNDED FOLLOWING AND UNBOUNDED FOLLOWING) AS "c5"""",
                "FROM",
                """    "user" AS "t1""""
            )
        )

    test("a groups frame rejects every reversed bound pair"):
        assertEquals(
            messages(compileErrors("from(User).map(u => count().over(partitionBy(u.id).sortBy(u.id).groupsBetween(1.preceding, unboundedPreceding)))")),
            List("Frame starting from preceding row cannot end at unbounded preceding.")
        )
        assertEquals(
            messages(compileErrors("from(User).map(u => count().over(partitionBy(u.id).sortBy(u.id).groupsBetween(currentRow, unboundedPreceding)))")),
            List("Frame starting from current row cannot end at unbounded preceding.")
        )
        assertEquals(
            messages(compileErrors("from(User).map(u => count().over(partitionBy(u.id).sortBy(u.id).groupsBetween(currentRow, 1.preceding)))")),
            List("Frame starting from current row cannot end at preceding row.")
        )
        assertEquals(
            messages(compileErrors("from(User).map(u => count().over(partitionBy(u.id).sortBy(u.id).groupsBetween(1.following, unboundedPreceding)))")),
            List("Frame starting from following row cannot end at unbounded preceding.")
        )
        assertEquals(
            messages(compileErrors("from(User).map(u => count().over(partitionBy(u.id).sortBy(u.id).groupsBetween(1.following, 2.preceding)))")),
            List("Frame starting from following row cannot end at preceding row.")
        )
        assertEquals(
            messages(compileErrors("from(User).map(u => count().over(partitionBy(u.id).sortBy(u.id).groupsBetween(1.following, currentRow)))")),
            List("Frame starting from following row cannot end at current row.")
        )
        assertEquals(
            messages(compileErrors("from(User).map(u => count().over(partitionBy(u.id).sortBy(u.id).groupsBetween(unboundedFollowing, unboundedPreceding)))")),
            List("Frame starting from unbounded following cannot end at unbounded preceding.")
        )
        assertEquals(
            messages(compileErrors("from(User).map(u => count().over(partitionBy(u.id).sortBy(u.id).groupsBetween(unboundedFollowing, 1.preceding)))")),
            List("Frame starting from unbounded following cannot end at preceding row.")
        )
        assertEquals(
            messages(compileErrors("from(User).map(u => count().over(partitionBy(u.id).sortBy(u.id).groupsBetween(unboundedFollowing, currentRow)))")),
            List("Frame starting from unbounded following cannot end at current row.")
        )
        assertEquals(
            messages(compileErrors("from(User).map(u => count().over(partitionBy(u.id).sortBy(u.id).groupsBetween(unboundedFollowing, 1.following)))")),
            List("Frame starting from unbounded following cannot end at following row.")
        )

    test("a single-bound frame ends at the current row"):
        assertEquals(
            sql(from(User).map(u => (
                a = count().over(partitionBy(u.id).sortBy(u.id).rows(unboundedPreceding)),
                b = count().over(partitionBy(u.id).sortBy(u.id).range(1.preceding)),
                c = count().over(partitionBy(u.id).sortBy(u.id).groups(1L.preceding)),
                d = count().over(partitionBy(u.id).sortBy(u.id).rows(currentRow)),
                e = count().over(partitionBy(u.id).sortBy(u.id).range(currentRow)),
                f = count().over(partitionBy(u.id).sortBy(u.id).groups(currentRow))
            ))),
            lines(
                "SELECT",
                """    COUNT(*) OVER (PARTITION BY "t1"."id" ORDER BY "t1"."id" ASC ROWS UNBOUNDED PRECEDING) AS "c1",""",
                """    COUNT(*) OVER (PARTITION BY "t1"."id" ORDER BY "t1"."id" ASC RANGE 1 PRECEDING) AS "c2",""",
                """    COUNT(*) OVER (PARTITION BY "t1"."id" ORDER BY "t1"."id" ASC GROUPS 1 PRECEDING) AS "c3",""",
                """    COUNT(*) OVER (PARTITION BY "t1"."id" ORDER BY "t1"."id" ASC ROWS CURRENT ROW) AS "c4",""",
                """    COUNT(*) OVER (PARTITION BY "t1"."id" ORDER BY "t1"."id" ASC RANGE CURRENT ROW) AS "c5",""",
                """    COUNT(*) OVER (PARTITION BY "t1"."id" ORDER BY "t1"."id" ASC GROUPS CURRENT ROW) AS "c6"""",
                "FROM",
                """    "user" AS "t1""""
            )
        )
        assertEquals(
            messages(compileErrors("from(User).map(u => count().over(partitionBy(u.id).sortBy(u.id).rows(1.following)))")),
            List("Frame starting from following row cannot end at current row.")
        )
        assertEquals(
            messages(compileErrors("from(User).map(u => count().over(partitionBy(u.id).sortBy(u.id).range(1.following)))")),
            List("Frame starting from following row cannot end at current row.")
        )
        assertEquals(
            messages(compileErrors("from(User).map(u => count().over(partitionBy(u.id).sortBy(u.id).groups(1.following)))")),
            List("Frame starting from following row cannot end at current row.")
        )
        assertEquals(
            messages(compileErrors("from(User).map(u => count().over(partitionBy(u.id).sortBy(u.id).rows(unboundedFollowing)))")),
            List("Frame starting from unbounded following cannot end at current row.")
        )
        assertEquals(
            messages(compileErrors("from(User).map(u => count().over(partitionBy(u.id).sortBy(u.id).range(unboundedFollowing)))")),
            List("Frame starting from unbounded following cannot end at current row.")
        )
        assertEquals(
            messages(compileErrors("from(User).map(u => count().over(partitionBy(u.id).sortBy(u.id).groups(unboundedFollowing)))")),
            List("Frame starting from unbounded following cannot end at current row.")
        )

    test("a rows or groups frame bound must be an integer"):
        assertEquals(
            messages(compileErrors("from(User).map(u => count().over(partitionBy(u.id).sortBy(u.id).rowsBetween(1.0.preceding, currentRow)))")),
            List("Window frame type is incompatible with the ordering expression.")
        )
        assertEquals(
            messages(compileErrors("from(User).map(u => count().over(partitionBy(u.id).sortBy(u.id).rowsBetween(currentRow, \"x\".following)))")),
            List("Window frame type is incompatible with the ordering expression.")
        )
        assertEquals(
            messages(compileErrors("from(User).map(u => count().over(partitionBy(u.id).sortBy(u.id).groupsBetween(1.day.preceding, currentRow)))")),
            List("Window frame type is incompatible with the ordering expression.")
        )

    test("a range frame bound must match the ordering expression type"):
        assertEquals(
            sql(from(Wide).map(w => count().over(partitionBy(w.id).sortBy(w.birth).rangeBetween(1.day.preceding, currentRow)))),
            lines(
                "SELECT",
                """    COUNT(*) OVER (PARTITION BY "t1"."id" ORDER BY "t1"."birth" ASC RANGE BETWEEN INTERVAL '1' DAY PRECEDING AND CURRENT ROW) AS "c1"""",
                "FROM",
                """    "wide" AS "t1""""
            )
        )
        assertEquals(
            messages(compileErrors("from(Wide).map(w => count().over(partitionBy(w.id).sortBy(w.birth).rangeBetween(1.preceding, currentRow)))")),
            List("Window frame type is incompatible with the ordering expression.")
        )
        assertEquals(
            messages(compileErrors("from(User).map(u => count().over(partitionBy(u.id).sortBy(u.id).rangeBetween(1.day.preceding, currentRow)))")),
            List("Window frame type is incompatible with the ordering expression.")
        )
        assertEquals(
            messages(compileErrors("from(User).map(u => count().over(partitionBy(u.id).sortBy(u.name).rangeBetween(1.preceding, currentRow)))")),
            List("Window frame type is incompatible with the ordering expression.")
        )
        assertEquals(
            messages(compileErrors("from(User).map(u => count().over(partitionBy(u.id).sortBy(u.name).rowsBetween(1.preceding, currentRow)))")),
            List()
        )

    test("a frame bound must not contain variables"):
        assertEquals(
            messages(compileErrors("from(User).map(u => count().over(partitionBy(u.id).sortBy(u.id).rowsBetween(u.id.preceding, currentRow)))")),
            List("Argument must not contain variables.")
        )
        assertEquals(
            messages(compileErrors("from(User).map(u => count().over(partitionBy(u.id).sortBy(u.id).rowsBetween(currentRow, count().following)))")),
            List("Argument must not contain variables.")
        )
        assertEquals(
            messages(compileErrors("from(User).map(u => count().over(partitionBy(u.id).sortBy(u.id).rowsBetween(currentRow, count().over().following)))")),
            List("Argument must not contain variables.")
        )

    test("filter is only available on aggregate functions"):
        assertEquals(
            sql(from(User).map(u => (a = count().filter(u.age > 18), b = sum(u.age).filter(u.age > 18).filter(u.id > 0)))),
            lines(
                "SELECT",
                """    COUNT(*) FILTER (WHERE "t1"."age" > 18) AS "c1",""",
                """    SUM("t1"."age") FILTER (WHERE "t1"."age" > 18 AND "t1"."id" > 0) AS "c2"""",
                "FROM",
                """    "user" AS "t1""""
            )
        )
        assertEquals(
            messages(compileErrors("from(User).map(u => u.id.filter(u.age > 18))")),
            List("Filter specified, but this expression is not an aggregate function.")
        )
        assertEquals(
            messages(compileErrors("from(User).map(u => count().over().filter(u.age > 18))")),
            List("Filter specified, but this expression is not an aggregate function.")
        )
        assert(compileErrors("from(User).map(u => count().filter(u.age))").startsWith("error: The arguments type should be boolean, but got scala.Int."))

    test("grouping requires a grouping expression of the same level"):
        assertEquals(compileErrors("from(User).groupBy(u => (id = u.id)).map((g, u) => grouping(g.id))"), "")
        assertEquals(
            messages(compileErrors("from(User).groupBy(u => (id = u.id)).map((g, u) => grouping(u.name))")),
            List("Arguments must be grouping expressions of the associated query level.")
        )
        assertEquals(compileErrors("from(User).groupBySets(u => (id = u.id, age = u.age))(g => cube(g.id, g.age)).map((g, u) => count())"), "")
        assertEquals(
            messages(compileErrors("from(User).groupBySets(u => (id = u.id, age = u.age))(g => rollup(g.id, 1)).map((g, u) => count())")),
            List("Arguments must be grouping expressions of the associated query level.")
        )

    test("scalar values are not allowed in sortBy or groupBy"):
        assertEquals(
            messages(compileErrors("from(User).sortBy(u => 1)")),
            List("Scalar value are not allowed in sortBy.")
        )
        assertEquals(
            messages(compileErrors("from(User).groupBy(u => 1)")),
            List("Scalar value are not allowed in groupBy.")
        )
        assertEquals(compileErrors("from(User).sortBy(u => u.id)"), "")
        assertEquals(compileErrors("from(User).groupBy(u => u.id).map((g, _) => (g, count()))"), "")

    test("sortBy after mapDistinct must reference a projected expression"):
        assertEquals(compileErrors("from(User).mapDistinct(u => (id = u.id)).sortBy(s => s.id)"), "")
        assertEquals(compileErrors("from(User).mapDistinct(u => (id = u.id)).sortBy(s => s.id.desc)"), "")
        assertEquals(
            messages(compileErrors("from(User).mapDistinct(u => (id = u.id)).sortBy(s => s.id + 1)")),
            List("For mapDistinct, sortBy expressions must appear in map.")
        )
        assertEquals(
            messages(compileErrors("from(User).mapDistinct(u => (id = u.id)).sortBy(s => 1)")),
            List("For mapDistinct, sortBy expressions must appear in map.")
        )
        assertEquals(
            compileErrors("from(User).mapDistinct(u => u.id).sortBy(s => s)"),
            ""
        )

    test("take zero or one yields a single-row subquery"):
        assertEquals(compileErrors("from(User).map(u => from(Post).map(p => p.id).take(0))"), "")
        assertEquals(compileErrors("from(User).map(u => from(Post).map(p => p.id).take(1))"), "")
        assert(compileErrors("from(User).map(u => from(Post).map(p => p.id).take(2))").startsWith("error: More than one row returned by a subquery used as an expression."))