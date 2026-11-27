package sqala.test

import sqala.static.dsl.*
import sqala.static.ext.dsl.*
import sqala.test.ExtFixture.*

/**
 * `MATCH_RECOGNIZE`: the clause chain (`partitionBy`, `sortBy`, rows
 * mode, `predefine`, `define`, `pattern`, `measures`), pattern
 * quantifiers, `PERMUTE`, exclusion, anchors, after-match skip modes
 * and the navigation functions `FIRST`, `LAST`, `PREV`, `NEXT`,
 * `CLASSIFIER`, `MATCH_NUMBER`, `FINAL` and `RUNNING`.
 */
class R1_MatchRecognize extends munit.FunSuite:
    test("matchRecognize prints the expected sql"):
        assertEquals(
            sql(from(Event.matchRecognize(s =>
                s.partitionBy(e => e.accountId)
                    .sortBy(e => e.ts)
                    .oneRowPerMatch
                    .predefine("start", "down", "up")
                    .define(d => (
                        start = true,
                        down = d.down.amount < prev(d.down.amount),
                        up = d.up.amount > prev(d.up.amount)
                    ))
                    .pattern(d => d.start ~ d.down.+ ~ d.up.+)
                    .measures(d => (
                        startTime = d.start.ts,
                        endTime = finalized(last(d.up.ts)),
                        matchNum = matchNumber(),
                        matchVar = classifier()
                    ))
            )).map(r => r)),
            lines(
                "SELECT",
                """    "t2"."c1" AS "c1",""",
                """    "t2"."c2" AS "c2",""",
                """    "t2"."c3" AS "c3",""",
                """    "t2"."c4" AS "c4"""",
                "FROM",
                """    "event" AS "t1"""",
                "    MATCH_RECOGNIZE(",
                "        PARTITION BY",
                """            "account_id"""",
                "        ORDER BY",
                """            "ts" ASC""",
                "        MEASURES",
                """            "start"."ts" AS "c1",""",
                """            FINAL LAST("up"."ts") AS "c2",""",
                """            MATCH_NUMBER() AS "c3",""",
                """            CLASSIFIER() AS "c4"""",
                "        ONE ROW PER MATCH",
                """        PATTERN ("start" "down"+ "up"+)""",
                "        DEFINE",
                """            "start" AS TRUE,""",
                """            "down" AS "down"."amount" < PREV("down"."amount"),""",
                """            "up" AS "up"."amount" > PREV("up"."amount")""",
                """    ) AS "t2""""
            )
        )

    test("all rows per match and navigation functions print the expected sql"):
        assertEquals(
            sql(from(Event.matchRecognize(s =>
                s.sortBy(e => e.ts.desc)
                    .allRowsPerMatch
                    .predefine("a", "b")
                    .define(d => (a = true, b = d.b.amount > prev(d.b.amount)))
                    .afterMatchSkipToNextRow
                    .pattern(d => d.a ~ d.b.+)
                    .measures(d => (
                        total = running(sum(d.b.amount)),
                        f = first(d.a.amount),
                        f2 = first(d.a.amount, 2),
                        l2 = last(d.b.amount, 2),
                        p2 = prev(d.b.amount, 2),
                        n2 = next(d.b.amount, 2)
                    ))
            )).map(r => r)),
            lines(
                "SELECT",
                """    "t2"."c1" AS "c1",""",
                """    "t2"."c2" AS "c2",""",
                """    "t2"."c3" AS "c3",""",
                """    "t2"."c4" AS "c4",""",
                """    "t2"."c5" AS "c5",""",
                """    "t2"."c6" AS "c6"""",
                "FROM",
                """    "event" AS "t1"""",
                "    MATCH_RECOGNIZE(",
                "        ORDER BY",
                """            "ts" DESC""",
                "        MEASURES",
                """            RUNNING SUM("b"."amount") AS "c1",""",
                """            FIRST("a"."amount") AS "c2",""",
                """            FIRST("a"."amount", 2) AS "c3",""",
                """            LAST("b"."amount", 2) AS "c4",""",
                """            PREV("b"."amount", 2) AS "c5",""",
                """            NEXT("b"."amount", 2) AS "c6"""",
                "        ALL ROWS PER MATCH",
                "        AFTER MATCH SKIP TO NEXT ROW",
                """        PATTERN ("a" "b"+)""",
                "        DEFINE",
                """            "a" AS TRUE,""",
                """            "b" AS "b"."amount" > PREV("b"."amount")""",
                """    ) AS "t2""""
            )
        )

    test("pattern quantifiers print the expected sql"):
        assertEquals(
            sql(from(Event.matchRecognize(s =>
                s.sortBy(e => e.ts)
                    .predefine("a", "b", "c", "d", "e", "f")
                    .define(_ => (a = true, b = true, c = true, d = true, e = true, f = true))
                    .pattern(d => d.a.? ~ d.b.* ~ d.c.between(1, 10) ~ d.d.least(3) ~ d.e.most(10) ~ d.f.at(3))
                    .measures(d => (m = d.a.id))
            )).map(r => r)),
            lines(
                "SELECT",
                """    "t2"."c1" AS "c1"""",
                "FROM",
                """    "event" AS "t1"""",
                "    MATCH_RECOGNIZE(",
                "        ORDER BY",
                """            "ts" ASC""",
                "        MEASURES",
                """            "a"."id" AS "c1"""",
                """        PATTERN ("a"? "b"* "c"{1,10} "d"{3,} "e"{,10} "f"{3})""",
                "        DEFINE",
                """            "a" AS TRUE,""",
                """            "b" AS TRUE,""",
                """            "c" AS TRUE,""",
                """            "d" AS TRUE,""",
                """            "e" AS TRUE,""",
                """            "f" AS TRUE""",
                """    ) AS "t2""""
            )
        )

    test("permute, exclusion and anchors print the expected sql"):
        assertEquals(
            sql(from(Event.matchRecognize(s =>
                s.sortBy(e => e.ts)
                    .predefine("a", "b")
                    .define(_ => (a = true, b = true))
                    .pattern(d => ^ ~ permute(d.a, d.b) ~ exclusion(d.a) ~ $)
                    .measures(d => (m = d.a.id))
            )).map(r => r)),
            lines(
                "SELECT",
                """    "t2"."c1" AS "c1"""",
                "FROM",
                """    "event" AS "t1"""",
                "    MATCH_RECOGNIZE(",
                "        ORDER BY",
                """            "ts" ASC""",
                "        MEASURES",
                """            "a"."id" AS "c1"""",
                """        PATTERN (^ PERMUTE("a", "b") {-"a"-} $)""",
                "        DEFINE",
                """            "a" AS TRUE,""",
                """            "b" AS TRUE""",
                """    ) AS "t2""""
            )
        )

    test("pattern alternation prints the expected sql"):
        assertEquals(
            sql(from(Event.matchRecognize(s =>
                s.sortBy(e => e.ts)
                    .predefine("a", "b")
                    .define(_ => (a = true, b = true))
                    .pattern(d => d.a | d.b)
                    .measures(d => (m = d.a.id))
            )).map(r => r)),
            lines(
                "SELECT",
                """    "t2"."c1" AS "c1"""",
                "FROM",
                """    "event" AS "t1"""",
                "    MATCH_RECOGNIZE(",
                "        ORDER BY",
                """            "ts" ASC""",
                "        MEASURES",
                """            "a"."id" AS "c1"""",
                """        PATTERN ("a" | "b")""",
                "        DEFINE",
                """            "a" AS TRUE,""",
                """            "b" AS TRUE""",
                """    ) AS "t2""""
            )
        )

    test("multiple partition and sort keys print the expected sql"):
        assertEquals(
            sql(from(Event.matchRecognize(s =>
                s.partitionBy(e => (e.accountId, e.kind))
                    .sortBy(e => (e.ts, e.id.desc))
                    .predefine("a")
                    .define(_ => (a = true))
                    .pattern(d => d.a.+)
                    .measures(d => (m = d.a.id))
            )).map(r => r)),
            lines(
                "SELECT",
                """    "t2"."c1" AS "c1"""",
                "FROM",
                """    "event" AS "t1"""",
                "    MATCH_RECOGNIZE(",
                "        PARTITION BY",
                """            "account_id",""",
                """            "kind"""",
                "        ORDER BY",
                """            "ts" ASC,""",
                """            "id" DESC""",
                "        MEASURES",
                """            "a"."id" AS "c1"""",
                """        PATTERN ("a"+)""",
                "        DEFINE",
                """            "a" AS TRUE""",
                """    ) AS "t2""""
            )
        )

    test("after match skip modes print the expected sql"):
        assertEquals(
            sql(from(Event.matchRecognize(s =>
                s.sortBy(e => e.ts)
                    .predefine("a", "b")
                    .define(_ => (a = true, b = true))
                    .afterMatchSkipPastLastRow
                    .pattern(d => d.a ~ d.b)
                    .measures(d => (m = d.a.id))
            )).map(r => r)),
            lines(
                "SELECT",
                """    "t2"."c1" AS "c1"""",
                "FROM",
                """    "event" AS "t1"""",
                "    MATCH_RECOGNIZE(",
                "        ORDER BY",
                """            "ts" ASC""",
                "        MEASURES",
                """            "a"."id" AS "c1"""",
                "        AFTER MATCH SKIP PAST LAST ROW",
                """        PATTERN ("a" "b")""",
                "        DEFINE",
                """            "a" AS TRUE,""",
                """            "b" AS TRUE""",
                """    ) AS "t2""""
            )
        )
        assertEquals(
            sql(from(Event.matchRecognize(s =>
                s.sortBy(e => e.ts)
                    .predefine("a", "b")
                    .define(_ => (a = true, b = true))
                    .afterMatchSkipToFirst(d => d.b)
                    .pattern(d => d.a ~ d.b)
                    .measures(d => (m = d.a.id))
            )).map(r => r)),
            lines(
                "SELECT",
                """    "t2"."c1" AS "c1"""",
                "FROM",
                """    "event" AS "t1"""",
                "    MATCH_RECOGNIZE(",
                "        ORDER BY",
                """            "ts" ASC""",
                "        MEASURES",
                """            "a"."id" AS "c1"""",
                """        AFTER MATCH SKIP TO FIRST "b"""",
                """        PATTERN ("a" "b")""",
                "        DEFINE",
                """            "a" AS TRUE,""",
                """            "b" AS TRUE""",
                """    ) AS "t2""""
            )
        )
        assertEquals(
            sql(from(Event.matchRecognize(s =>
                s.sortBy(e => e.ts)
                    .predefine("a", "b")
                    .define(_ => (a = true, b = true))
                    .afterMatchSkipToLast(d => d.b)
                    .pattern(d => d.a ~ d.b)
                    .measures(d => (m = d.a.id))
            )).map(r => r)),
            lines(
                "SELECT",
                """    "t2"."c1" AS "c1"""",
                "FROM",
                """    "event" AS "t1"""",
                "    MATCH_RECOGNIZE(",
                "        ORDER BY",
                """            "ts" ASC""",
                "        MEASURES",
                """            "a"."id" AS "c1"""",
                """        AFTER MATCH SKIP TO LAST "b"""",
                """        PATTERN ("a" "b")""",
                "        DEFINE",
                """            "a" AS TRUE,""",
                """            "b" AS TRUE""",
                """    ) AS "t2""""
            )
        )
        assertEquals(
            sql(from(Event.matchRecognize(s =>
                s.sortBy(e => e.ts)
                    .predefine("a", "b")
                    .define(_ => (a = true, b = true))
                    .afterMatchSkipTo(d => d.b)
                    .pattern(d => d.a ~ d.b)
                    .measures(d => (m = d.a.id))
            )).map(r => r)),
            lines(
                "SELECT",
                """    "t2"."c1" AS "c1"""",
                "FROM",
                """    "event" AS "t1"""",
                "    MATCH_RECOGNIZE(",
                "        ORDER BY",
                """            "ts" ASC""",
                "        MEASURES",
                """            "a"."id" AS "c1"""",
                """        AFTER MATCH SKIP TO "b"""",
                """        PATTERN ("a" "b")""",
                "        DEFINE",
                """            "a" AS TRUE,""",
                """            "b" AS TRUE""",
                """    ) AS "t2""""
            )
        )

    test("matchRecognize accepts a subquery source"):
        assertEquals(
            sql(from(from(Event).map(e => (acc = e.accountId, amt = e.amount)).matchRecognize(s =>
                s.sortBy(e => e.acc)
                    .predefine("a")
                    .define(_ => (a = true))
                    .pattern(d => d.a.+)
                    .measures(d => (m = d.a.amt))
            )).map(r => r)),
            lines(
                "SELECT",
                """    "t3"."c1" AS "c1"""",
                "FROM",
                "    (",
                "        SELECT",
                """            "t1"."account_id" AS "c1",""",
                """            "t1"."amount" AS "c2"""",
                "        FROM",
                """            "event" AS "t1"""",
                """    ) AS "t2"""",
                "    MATCH_RECOGNIZE(",
                "        ORDER BY",
                """            "c1" ASC""",
                "        MEASURES",
                """            "a"."c2" AS "c1"""",
                """        PATTERN ("a"+)""",
                "        DEFINE",
                """            "a" AS TRUE""",
                """    ) AS "t3""""
            )
        )

    test("measures columns can be selected in the outer query"):
        assertEquals(
            compileErrors("""from(Event.matchRecognize(s =>
                s.sortBy(e => e.ts)
                    .predefine("a")
                    .define(_ => (a = true))
                    .pattern(d => d.a)
                    .measures(d => (m = d.a.id))
            )).map(r => expect[Int](r.m))"""),
            ""
        )

    test("navigation functions yield the expected types"):
        assertEquals(
            compileErrors("""from(Event.matchRecognize(s =>
                s.sortBy(e => e.ts)
                    .predefine("a")
                    .define(_ => (a = true))
                    .pattern(d => d.a)
                    .measures(d => (
                        m = classifier(),
                        n = matchNumber(),
                        f = first(d.a.amount),
                        l = last(d.a.amount, 2),
                        p = prev(d.a.amount),
                        nx = next(d.a.amount),
                        t = running(sum(d.a.amount)),
                        fin = finalized(last(d.a.amount))
                    ))
            )).map(r => (
                expect[String](r.m),
                expect[Int](r.n),
                expect[Option[Long]](r.f),
                expect[Option[Long]](r.l),
                expect[Option[Long]](r.p),
                expect[Option[Long]](r.nx),
                expect[Option[BigDecimal]](r.t),
                expect[Option[Long]](r.fin)
            ))"""),
            ""
        )

    test("predefine rejects duplicated names"):
        assert(compileErrors("""from(Event.matchRecognize(s =>
            s.sortBy(e => e.ts)
                .predefine("a", "a")
                .define(_ => (a = true))
                .pattern(d => d.a)
                .measures(d => (m = d.a.id))
        )).map(r => r)""").contains("Predefine names cannot be duplicated."))

    test("predefine rejects non-literal names"):
        assert(compileErrors("""val n = "a"; from(Event.matchRecognize(s => s.sortBy(e => e.ts).predefine(n)))""").contains("Predefine names must be string literals."))

    test("predefine rejects an empty name list"):
        assert(compileErrors("from(Event.matchRecognize(s => s.sortBy(e => e.ts).predefine()))").contains("Predefine requires at least one name."))

    test("navigation functions require a matchRecognize context"):
        assert(compileErrors("from(Event).map(e => classifier())").contains("MatchRecognizeContext"))
        assert(compileErrors("from(Event).map(e => matchNumber())").contains("MatchRecognizeContext"))