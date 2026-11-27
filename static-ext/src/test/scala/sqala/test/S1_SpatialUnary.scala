package sqala.test

import sqala.static.dsl.*
import sqala.static.ext.dsl.*
import sqala.test.ExtFixture.*

/**
 * Unary spatial functions: `ST_GeomFromText`, `ST_AsText`, `ST_AsGeoJson`,
 * `ST_GeometryType`, `ST_X`, `ST_Y`, `ST_Area` and `ST_Length`. Covers the
 * generated sql, the inferred result type, the operand forms, and the
 * argument rules.
 */
class S1_SpatialUnary extends munit.FunSuite:
    test("unary spatial functions print the expected sql"):
        assertEquals(
            sql(from(Geo).map(t => (
                a = stAsText(t.p),
                b = stAsGeoJson(t.p),
                c = stGeometryType(t.p),
                d = stX(t.p),
                e = stY(t.p),
                f = stArea(t.pg),
                g = stLength(t.ls)
            ))),
            lines(
                "SELECT",
                """    ST_AsText("t1"."p") AS "c1",""",
                """    ST_AsGeoJson("t1"."p") AS "c2",""",
                """    ST_GeometryType("t1"."p") AS "c3",""",
                """    ST_X("t1"."p") AS "c4",""",
                """    ST_Y("t1"."p") AS "c5",""",
                """    ST_Area("t1"."pg") AS "c6",""",
                """    ST_Length("t1"."ls") AS "c7"""",
                "FROM",
                """    "geo" AS "t1""""
            )
        )

    test("stGeomFromText prints the wkt and srid"):
        assertEquals(
            sql(from(Geo).map(_ => stGeomFromText("POINT(1 1)", 4326))),
            lines(
                "SELECT",
                """    ST_GeomFromText('POINT(1 1)', 4326) AS "c1"""",
                "FROM",
                """    "geo" AS "t1""""
            )
        )

    test("text accessors yield an optional string"):
        assertEquals(compileErrors("from(Geo).map(t => expect[Option[String]](stAsText(t.p)))"), "")
        assertEquals(compileErrors("from(Geo).map(t => expect[Option[String]](stAsGeoJson(t.p)))"), "")
        assertEquals(compileErrors("from(Geo).map(t => expect[Option[String]](stGeometryType(t.p)))"), "")

    test("measure accessors yield an optional decimal"):
        assertEquals(compileErrors("from(Geo).map(t => expect[Option[BigDecimal]](stX(t.p)))"), "")
        assertEquals(compileErrors("from(Geo).map(t => expect[Option[BigDecimal]](stY(t.p)))"), "")
        assertEquals(compileErrors("from(Geo).map(t => expect[Option[BigDecimal]](stArea(t.pg)))"), "")
        assertEquals(compileErrors("from(Geo).map(t => expect[Option[BigDecimal]](stLength(t.ls)))"), "")

    test("stGeomFromText yields an optional geometry"):
        assertEquals(compileErrors("""from(Geo).map(t => expect[Option[sqala.metadata.Geometry]](stGeomFromText("POINT(1 1)", 4326)))"""), "")

    test("unary spatial functions accept every geometry subtype"):
        assertEquals(compileErrors("from(Geo).map(t => stAsText(t.g))"), "")
        assertEquals(compileErrors("from(Geo).map(t => stAsText(t.ls))"), "")
        assertEquals(compileErrors("from(Geo).map(t => stAsText(t.pg))"), "")
        assertEquals(compileErrors("from(Geo).map(t => stAsText(t.mp))"), "")
        assertEquals(compileErrors("from(Geo).map(t => stAsText(t.mls))"), "")
        assertEquals(compileErrors("from(Geo).map(t => stAsText(t.mpg))"), "")
        assertEquals(compileErrors("from(Geo).map(t => stAsText(t.gc))"), "")

    test("unary spatial functions accept optional columns and subqueries"):
        assertEquals(compileErrors("from(Geo).map(t => stAsText(t.op))"), "")
        assertEquals(compileErrors("from(Geo).map(t => stX(t.op))"), "")
        assertEquals(compileErrors("from(Geo).map(t => stAsText(from(Geo).map(x => max(x.p))))"), "")
        assertEquals(compileErrors("from(Geo).map(t => stAsText(stGeomFromText(\"POINT(1 1)\", 4326)))"), "")
        assertEquals(compileErrors("from(Geo).groupBy(t => t.p).map((g, t) => stAsText(g))"), "")

    test("unary spatial functions reject a non-geometry operand"):
        assertEquals(
            messages(compileErrors("from(Num).map(n => stAsText(n.i))")),
            List("The arguments type should be geometry, but got scala.Int.")
        )
        assertEquals(
            messages(compileErrors("""from(Num).map(n => stAsText("x"))""")),
            List("The arguments type should be geometry, but got java.lang.String.")
        )

    test("stGeomFromText requires a string wkt and a numeric srid"):
        assertEquals(
            messages(compileErrors("from(Geo).map(t => stGeomFromText(t.p, 4326))")),
            List("The arguments type should be string, but got sqala.metadata.Point.")
        )
        assertEquals(
            messages(compileErrors("""from(Geo).map(t => stGeomFromText("POINT(1 1)", "x"))""")),
            List("The arguments type should be numeric, but got java.lang.String.")
        )
        assertEquals(
            messages(compileErrors("""from(Geo).map(t => stGeomFromText("POINT(1 1)", t.op))""")),
            List("The arguments type should be numeric, but got scala.Option[sqala.metadata.Point].")
        )