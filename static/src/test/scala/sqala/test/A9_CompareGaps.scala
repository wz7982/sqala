package sqala.test

import java.time.LocalDateTime
import sqala.static.dsl.*
import sqala.test.StaticFixture.*

/**
 * Comparison and ordering forms not covered by the operator matrices:
 * null-safe equality `<=>`, `BETWEEN`, `OVERLAPS`, the remaining null
 * ordering directions, and the interval constructors.
 */
class A9_CompareGaps extends munit.FunSuite:
    test("null-safe equality prints is not distinct from"):
        assertEquals(
            sql(from(Wide).filter(w => w.name <=> "x")),
            lines(
                "SELECT",
                """    "t1"."id" AS "c1",""",
                """    "t1"."name" AS "c2",""",
                """    "t1"."flag" AS "c3",""",
                """    "t1"."birth" AS "c4",""",
                """    "t1"."score" AS "c5"""",
                "FROM",
                """    "wide" AS "t1"""",
                "WHERE",
                """    "t1"."name" IS NOT DISTINCT FROM 'x'"""
            )
        )

    test("null-safe equality accepts columns and optional columns"):
        assertEquals(
            sql(from(Wide).filter(w => w.score <=> w.score)),
            lines(
                "SELECT",
                """    "t1"."id" AS "c1",""",
                """    "t1"."name" AS "c2",""",
                """    "t1"."flag" AS "c3",""",
                """    "t1"."birth" AS "c4",""",
                """    "t1"."score" AS "c5"""",
                "FROM",
                """    "wide" AS "t1"""",
                "WHERE",
                """    "t1"."score" IS NOT DISTINCT FROM "t1"."score""""
            )
        )
        assertEquals(compileErrors("from(Wide).filter(w => w.score <=> (None: Option[Long]))"), "")
        assertEquals(compileErrors("from(Wide).filter(w => w.id <=> from(Wide).map(x => max(x.id)))"), "")

    test("null-safe equality is never optional"):
        assertEquals(compileErrors("""from(Wide).map(w => expect[Boolean](w.name <=> "x"))"""), "")
        assertEquals(compileErrors("from(Wide).map(w => expect[Boolean](w.score <=> w.score))"), "")

    test("null-safe equality rejects incompatible types"):
        assert(compileErrors("""from(Wide).map(w => w.name <=> 1)""").nonEmpty)
        assert(compileErrors("from(Wide).map(w => w.id <=> w.birth)").nonEmpty)

    test("between accepts values, expressions and subqueries"):
        assertEquals(compileErrors("from(Wide).filter(w => w.id.between(1, 10))"), "")
        assertEquals(compileErrors("from(Wide).filter(w => w.id.between(w.id, w.id + 1))"), "")
        assertEquals(compileErrors("from(Wide).filter(w => w.id.between(from(Wide).map(x => min(x.id)), from(Wide).map(x => max(x.id))))"), "")
        assertEquals(compileErrors("""from(Wide).filter(w => w.name.between("a", "b"))"""), "")
        assertEquals(compileErrors("from(Wide).filter(w => w.score.between(1L, 10L))"), "")
        assertEquals(compileErrors("from(Times).filter(t => t.ts.between(t.ts, t.ts + 1.day))"), "")

    test("between yields a boolean, optional when an operand is optional"):
        assertEquals(compileErrors("from(Wide).map(w => expect[Boolean](w.id.between(1, 10)))"), "")
        assertEquals(compileErrors("from(Wide).map(w => expect[Option[Boolean]](w.score.between(1L, 10L)))"), "")

    test("between rejects incompatible bound types"):
        assert(compileErrors("""from(Wide).filter(w => w.id.between("a", "b"))""").nonEmpty)
        assert(compileErrors("from(Wide).filter(w => w.id.between(1, w.name))").nonEmpty)

    test("in accepts a tuple mixing values, expressions and subqueries"):
        assertEquals(
            sql(from(User).filter(u => u.id.in((1, u.age, from(Post).map(p => max(p.userId))))).map(u => u.name)),
            lines(
                "SELECT",
                """    "t1"."name" AS "c1"""",
                "FROM",
                """    "user" AS "t1"""",
                "WHERE",
                """    "t1"."id" IN (1, "t1"."age", (""",
                "        SELECT",
                """            MAX("t2"."user_id") AS "c1"""",
                "        FROM",
                """            "post" AS "t2"""",
                "    ))"
            )
        )

    test("a mixed in tuple yields a boolean, optional when an element is nullable"):
        assertEquals(compileErrors("from(User).map(u => expect[Boolean](u.id.in((1, u.age, 2))))"), "")
        assertEquals(compileErrors("from(User).map(u => expect[Option[Boolean]](u.id.in((1, u.age, from(Post).map(p => count())))))"), "")
        assertEquals(compileErrors("from(User).map(u => expect[Option[Boolean]](u.id.in((1, u.age, from(Post).map(p => max(p.userId))))))"), "")
        assertEquals(compileErrors("from(User).map(u => expect[Option[Boolean]](u.id.in((1, u.age, from(Nums).map(n => max(n.oi))))))"), "")

    test("a mixed in tuple rejects an incompatible element"):
        assert(compileErrors("""from(User).filter(u => u.id.in((1, u.age, "x")))""").nonEmpty)

    test("a subquery element of an in tuple must return at most one row"):
        assert(compileErrors("from(User).filter(u => u.id.in((1, u.age, from(Post).map(p => p.userId))))").nonEmpty)

    test("null ordering directions print the expected sql"):
        assertEquals(
            sql(from(Wide).sortBy(w => (w.name.ascNullsLast, w.id.descNullsFirst))),
            lines(
                "SELECT",
                """    "t1"."id" AS "c1",""",
                """    "t1"."name" AS "c2",""",
                """    "t1"."flag" AS "c3",""",
                """    "t1"."birth" AS "c4",""",
                """    "t1"."score" AS "c5"""",
                "FROM",
                """    "wide" AS "t1"""",
                "ORDER BY",
                """    "t1"."name" ASC NULLS LAST,""",
                """    "t1"."id" DESC NULLS FIRST"""
            )
        )

    test("all four null ordering directions combine"):
        assertEquals(
            sql(from(Wide).sortBy(w => (w.name.ascNullsFirst, w.id.ascNullsLast, w.score.descNullsFirst, w.birth.descNullsLast))),
            lines(
                "SELECT",
                """    "t1"."id" AS "c1",""",
                """    "t1"."name" AS "c2",""",
                """    "t1"."flag" AS "c3",""",
                """    "t1"."birth" AS "c4",""",
                """    "t1"."score" AS "c5"""",
                "FROM",
                """    "wide" AS "t1"""",
                "ORDER BY",
                """    "t1"."name" ASC NULLS FIRST,""",
                """    "t1"."id" ASC NULLS LAST,""",
                """    "t1"."score" DESC NULLS FIRST,""",
                """    "t1"."birth" DESC NULLS LAST"""
            )
        )

    test("null ordering works inside a window"):
        assertEquals(
            sql(from(Wide).map(w => rowNumber().over(sortBy(w.score.descNullsLast)))),
            lines(
                "SELECT",
                """    ROW_NUMBER() OVER (ORDER BY "t1"."score" DESC NULLS LAST) AS "c1"""",
                "FROM",
                """    "wide" AS "t1""""
            )
        )

    test("interval constructors print the expected sql"):
        assertEquals(
            sql(from(Times).map(t => (a = t.dt + 1.year, b = t.dt + 2.month, c = t.dt + 3.day, d = t.ts + 4.hour, e = t.ts + 5.minute, f = t.ts + 6.second))),
            lines(
                "SELECT",
                """    "t1"."dt" + INTERVAL '1' YEAR AS "c1",""",
                """    "t1"."dt" + INTERVAL '2' MONTH AS "c2",""",
                """    "t1"."dt" + INTERVAL '3' DAY AS "c3",""",
                """    "t1"."ts" + INTERVAL '4' HOUR AS "c4",""",
                """    "t1"."ts" + INTERVAL '5' MINUTE AS "c5",""",
                """    "t1"."ts" + INTERVAL '6' SECOND AS "c6"""",
                "FROM",
                """    "times" AS "t1""""
            )
        )

    test("interval constructors work as frame bounds"):
        assertEquals(
            sql(from(Wide).map(w => count().over(sortBy(w.birth).rangeBetween(1.month.preceding, currentRow)))),
            lines(
                "SELECT",
                """    COUNT(*) OVER (ORDER BY "t1"."birth" ASC RANGE BETWEEN INTERVAL '1' MONTH PRECEDING AND CURRENT ROW) AS "c1"""",
                "FROM",
                """    "wide" AS "t1""""
            )
        )

    test("overlaps prints the expected sql"):
        assertEquals(
            sql(from(Times).filter(t => (t.lts, t.lts).overlaps((t.lts, t.lts))).map(t => t.dt)),
            lines(
                "SELECT",
                """    "t1"."dt" AS "c1"""",
                "FROM",
                """    "times" AS "t1"""",
                "WHERE",
                """    ("t1"."lts", "t1"."lts") OVERLAPS ("t1"."lts", "t1"."lts")"""
            )
        )
        assertEquals(
            sql(from(Times).filter(t => (t.ts, t.ts).overlaps((t.ots, t.ots))).map(t => t.dt)),
            lines(
                "SELECT",
                """    "t1"."dt" AS "c1"""",
                "FROM",
                """    "times" AS "t1"""",
                "WHERE",
                """    ("t1"."ts", "t1"."ts") OVERLAPS ("t1"."ots", "t1"."ots")"""
            )
        )
        assertEquals(
            sql(from(Times).filter(t => (t.lts, t.lts).overlaps((LocalDateTime.of(2024, 1, 1, 0, 0), t.lts))).map(t => t.dt)),
            lines(
                "SELECT",
                """    "t1"."dt" AS "c1"""",
                "FROM",
                """    "times" AS "t1"""",
                "WHERE",
                """    ("t1"."lts", "t1"."lts") OVERLAPS (TIMESTAMP '2024-01-01 00:00:00.000000000', "t1"."lts")"""
            )
        )

    test("overlaps projects as a value"):
        assertEquals(
            sql(from(Times).map(t => (a = (t.lts, t.lts).overlaps((t.lts, t.lts)))).sortBy(t => t.lts)),
            lines(
                "SELECT",
                """    ("t1"."lts", "t1"."lts") OVERLAPS ("t1"."lts", "t1"."lts") AS "c1"""",
                "FROM",
                """    "times" AS "t1"""",
                "ORDER BY",
                """    "t1"."lts" ASC"""
            )
        )

    test("overlaps in having accepts aggregate operands"):
        assertEquals(
            sql(from(Times).groupBy(t => t.dt).having((_, t) => (min(t.lts), max(t.lts)).overlaps((min(t.lts), max(t.lts)))).map((_, _) => count())),
            lines(
                "SELECT",
                """    COUNT(*) AS "c1"""",
                "FROM",
                """    "times" AS "t1"""",
                "GROUP BY",
                """    "t1"."dt"""",
                "HAVING",
                """    (MIN("t1"."lts"), MAX("t1"."lts")) OVERLAPS (MIN("t1"."lts"), MAX("t1"."lts"))"""
            )
        )

    test("overlaps yields a boolean, optional when an operand is optional"):
        assertEquals(compileErrors("from(Times).map(t => expect[Boolean]((t.lts, t.lts).overlaps((t.lts, t.lts))))"), "")
        assertEquals(compileErrors("from(Times).map(t => expect[Boolean]((t.lts, t.lts).overlaps((t.ts, t.ts))))"), "")
        assertEquals(compileErrors("from(Times).map(t => expect[Option[Boolean]]((t.ots, t.ots).overlaps((t.ots, t.ots))))"), "")
        assertEquals(compileErrors("from(Times).map(t => expect[Option[Boolean]]((t.olts, t.olts).overlaps((t.olts, t.olts))))"), "")
        assertEquals(compileErrors("from(Times).map(t => expect[Option[Boolean]]((t.lts, t.lts).overlaps((t.ots, t.ots))))"), "")
        assertEquals(compileErrors("from(Times).map(t => expect[Option[Boolean]]((t.olts, t.olts).overlaps((t.lts, t.lts))))"), "")
        assertEquals(compileErrors("from(Times).map(t => expect[Option[Boolean]]((t.lts, t.olts).overlaps((t.olts, t.lts))))"), "")

    test("overlaps accepts literals, expressions and subqueries on either side"):
        assertEquals(compileErrors("from(Times).filter(t => (t.lts, t.lts).overlaps((LocalDateTime.of(2024, 1, 1, 0, 0), t.lts)))"), "")
        assertEquals(compileErrors("from(Times).filter(t => (t.lts, t.lts).overlaps((LocalDateTime.of(2024, 1, 1, 0, 0), LocalDateTime.of(2024, 1, 2, 0, 0))))"), "")
        assertEquals(compileErrors("from(Times).filter(t => (t.lts + 1.day, t.lts).overlaps((t.lts, t.lts)))"), "")
        assertEquals(compileErrors("from(Times).filter(t => (t.lts, t.lts).overlaps((t.lts + 1.day, t.lts)))"), "")
        assertEquals(compileErrors("from(Times).filter(t => (t.lts, t.lts).overlaps((from(Times).map(x => min(x.lts)), t.lts)))"), "")
        assertEquals(compileErrors("from(Times).filter(t => (t.lts, t.lts).overlaps((from(Times).map(x => min(x.lts)), from(Times).map(x => max(x.lts)))))"), "")
        assertEquals(compileErrors("from(Times).filter(t => (t.lts, t.lts).overlaps((from(Times).filter(x => x.dt === t.dt).map(x => min(x.lts)), t.lts)))"), "")
        assertEquals(compileErrors("from(Times).filter(t => (t.lts, t.lts).overlaps((t.lts, t.lts)) && t.dt === t.dt)"), "")

    test("overlaps works as a join condition"):
        assertEquals(compileErrors("from(Times.join(Times).on((a, b) => (a.lts, a.lts).overlaps((b.lts, b.lts)))).map((a, b) => a.dt)"), "")

    test("overlaps rejects non-datetime operands"):
        assertEquals(
            messages(compileErrors("from(Times).filter(t => (1, 2).overlaps((3, 4)))")),
            List.fill(4)("The arguments type should be datetime, but got scala.Int.")
        )
        assertEquals(
            messages(compileErrors("from(Times).filter(t => (t.dt, t.dt).overlaps((t.dt, t.dt)))")),
            List.fill(4)("The arguments type should be datetime, but got java.time.LocalDate.")
        )
        assertEquals(
            messages(compileErrors("from(Times).filter(t => (t.lts, t.lts).overlaps((t.tm, t.tm)))")),
            List.fill(2)("The arguments type should be datetime, but got java.time.OffsetTime.")
        )
        assertEquals(
            messages(compileErrors("from(Times).filter(t => (t.lts, t.lts).overlaps((t.ltm, t.ltm)))")),
            List.fill(2)("The arguments type should be datetime, but got java.time.LocalTime.")
        )
        assertEquals(
            messages(compileErrors("from(Times).filter(t => (t.lts, t.lts).overlaps((t.iv, t.iv)))")),
            List.fill(2)("The arguments type should be datetime, but got sqala.metadata.Interval.")
        )

    test("overlaps rejects operands of different datetime types"):
        assertEquals(
            messages(compileErrors("from(Times).filter(t => (t.lts, t.lts).overlaps((t.lts, t.dt)))")),
            List(
                "Cannot compare types java.time.LocalDateTime and java.time.LocalDate.",
                "The arguments type should be datetime, but got java.time.LocalDate."
            )
        )
        assertEquals(
            messages(compileErrors("from(Times).filter(t => (t.lts, t.dt).overlaps((t.lts, t.lts)))")),
            List(
                "Cannot compare types java.time.LocalDateTime and java.time.LocalDate.",
                "The arguments type should be datetime, but got java.time.LocalDate."
            )
        )

    test("overlaps requires a pair on both sides"):
        assert(compileErrors("from(Times).filter(t => (t.lts, t.lts).overlaps((t.lts, t.lts, t.lts)))").contains("Required: (Any, Any)"))
        assert(compileErrors("from(Times).filter(t => (t.lts, t.lts).overlaps(t.lts))").contains("Required: (Any, Any)"))

    test("overlaps rejects a multi-row subquery operand"):
        assertEquals(
            messages(compileErrors("from(Times).filter(t => (t.lts, t.lts).overlaps((from(Times).map(x => x.lts), t.lts)))")),
            List("More than one row returned by a subquery used as an expression.")
        )
        assertEquals(
            messages(compileErrors("from(Times).filter(t => (t.lts, t.lts).overlaps((from(Times).filter(x => x.dt === t.dt).map(x => x.lts), t.lts)))")),
            List("More than one row returned by a subquery used as an expression.")
        )

    test("overlaps in having rejects ungrouped columns"):
        assertEquals(
            messages(compileErrors("from(Times).groupBy(t => t.dt).having((g, t) => (t.lts, t.lts).overlaps((t.lts, t.lts))).map((g, t) => count())")),
            List("Ungrouped column references must appear in the groupBy clause or be used in an aggregate function.")
        )