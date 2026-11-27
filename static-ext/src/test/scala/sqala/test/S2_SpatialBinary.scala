package sqala.test

import sqala.static.dsl.*
import sqala.static.ext.dsl.*
import sqala.test.ExtFixture.*

/**
 * Binary spatial functions: the seven relationship predicates, the four
 * set operations and `ST_Distance`. Covers the generated sql, the inferred
 * result type, the operand forms, and the argument rules.
 */
class S2_SpatialBinary extends munit.FunSuite:
    test("spatial predicates print the expected sql"):
        assertEquals(
            sql(from(Geo).map(t => (
                a = stContains(t.p, t.pg),
                b = stWithin(t.p, t.pg),
                c = stIntersects(t.p, t.pg),
                d = stTouches(t.p, t.pg),
                e = stOverlaps(t.p, t.pg),
                f = stCrosses(t.p, t.pg),
                g = stDisjoint(t.p, t.pg)
            ))),
            lines(
                "SELECT",
                """    ST_Contains("t1"."p", "t1"."pg") AS "c1",""",
                """    ST_Within("t1"."p", "t1"."pg") AS "c2",""",
                """    ST_Intersects("t1"."p", "t1"."pg") AS "c3",""",
                """    ST_Touches("t1"."p", "t1"."pg") AS "c4",""",
                """    ST_Overlaps("t1"."p", "t1"."pg") AS "c5",""",
                """    ST_Crosses("t1"."p", "t1"."pg") AS "c6",""",
                """    ST_Disjoint("t1"."p", "t1"."pg") AS "c7"""",
                "FROM",
                """    "geo" AS "t1""""
            )
        )

    test("spatial set operations and distance print the expected sql"):
        assertEquals(
            sql(from(Geo).map(t => (
                a = stIntersection(t.p, t.pg),
                b = stUnion(t.p, t.pg),
                c = stDifference(t.p, t.pg),
                d = stSymDifference(t.p, t.pg),
                e = stDistance(t.p, t.pg)
            ))),
            lines(
                "SELECT",
                """    ST_Intersection("t1"."p", "t1"."pg") AS "c1",""",
                """    ST_Union("t1"."p", "t1"."pg") AS "c2",""",
                """    ST_Difference("t1"."p", "t1"."pg") AS "c3",""",
                """    ST_SymDifference("t1"."p", "t1"."pg") AS "c4",""",
                """    ST_Distance("t1"."p", "t1"."pg") AS "c5"""",
                "FROM",
                """    "geo" AS "t1""""
            )
        )

    test("spatial predicates yield an optional boolean"):
        assertEquals(compileErrors("from(Geo).map(t => expect[Option[Boolean]](stContains(t.p, t.pg)))"), "")
        assertEquals(compileErrors("from(Geo).map(t => expect[Option[Boolean]](stWithin(t.p, t.pg)))"), "")
        assertEquals(compileErrors("from(Geo).map(t => expect[Option[Boolean]](stIntersects(t.p, t.pg)))"), "")
        assertEquals(compileErrors("from(Geo).map(t => expect[Option[Boolean]](stTouches(t.p, t.pg)))"), "")
        assertEquals(compileErrors("from(Geo).map(t => expect[Option[Boolean]](stOverlaps(t.p, t.pg)))"), "")
        assertEquals(compileErrors("from(Geo).map(t => expect[Option[Boolean]](stCrosses(t.p, t.pg)))"), "")
        assertEquals(compileErrors("from(Geo).map(t => expect[Option[Boolean]](stDisjoint(t.p, t.pg)))"), "")

    test("spatial set operations yield an optional geometry"):
        assertEquals(compileErrors("from(Geo).map(t => expect[Option[sqala.metadata.Geometry]](stUnion(t.p, t.pg)))"), "")
        assertEquals(compileErrors("from(Geo).map(t => expect[Option[sqala.metadata.Geometry]](stDifference(t.p, t.pg)))"), "")
        assertEquals(compileErrors("from(Geo).map(t => expect[Option[sqala.metadata.Geometry]](stSymDifference(t.p, t.pg)))"), "")
        assertEquals(compileErrors("from(Geo).map(t => expect[Option[sqala.metadata.Geometry]](stIntersection(t.p, t.pg)))"), "")

    test("stIntersection composes with the geometry accessors"):
        assertEquals(compileErrors("from(Geo).map(t => stAsText(stIntersection(t.p, t.pg)))"), "")
        assertEquals(compileErrors("from(Geo).map(t => stArea(stIntersection(t.p, t.pg)))"), "")
        assertEquals(compileErrors("from(Geo).map(t => stContains(stIntersection(t.p, t.pg), t.pg))"), "")

    test("stDistance yields an optional decimal"):
        assertEquals(compileErrors("from(Geo).map(t => expect[Option[BigDecimal]](stDistance(t.p, t.pg)))"), "")

    test("binary spatial functions mix geometry subtypes"):
        assertEquals(compileErrors("from(Geo).map(t => stContains(t.g, t.p))"), "")
        assertEquals(compileErrors("from(Geo).map(t => stContains(t.mp, t.mpg))"), "")
        assertEquals(compileErrors("from(Geo).map(t => stDistance(t.ls, t.gc))"), "")
        assertEquals(compileErrors("from(Geo).map(t => stUnion(t.p, t.op))"), "")

    test("binary spatial functions accept optional columns and subqueries"):
        assertEquals(compileErrors("from(Geo).map(t => stContains(t.op, t.pg))"), "")
        assertEquals(compileErrors("from(Geo).map(t => stContains(t.p, t.op))"), "")
        assertEquals(compileErrors("from(Geo).map(t => stContains(t.p, from(Geo).map(x => max(x.pg))))"), "")
        assertEquals(compileErrors("from(Geo).map(t => stDistance(from(Geo).map(x => max(x.p)), t.pg))"), "")
        assertEquals(compileErrors("from(Geo).map(t => stContains(t.p, stGeomFromText(\"POINT(1 1)\", 4326)))"), "")

    test("spatial predicates combine with boolean operators"):
        assertEquals(compileErrors("from(Geo).filter(t => stContains(t.p, t.pg) && stDisjoint(t.p, t.pg)).map(t => t.p)"), "")
        assertEquals(compileErrors("from(Geo).groupBy(t => t.p).having((g, t) => stContains(g, max(t.pg))).map((g, t) => count())"), "")

    test("binary spatial functions reject a non-geometry operand"):
        assertEquals(
            messages(compileErrors("from(Num).map(n => stContains(n.i, n.i))")),
            List.fill(2)("The arguments type should be geometry, but got scala.Int.")
        )
        assertEquals(compileErrors("from(Geo).map(t => stDistance(t.p, t.ls))"), "")