package sqala.test

import sqala.static.dsl.*
import sqala.test.StaticFixture.*

/**
 * Trigonometric, hyperbolic, exponential and logarithmic functions,
 * plus the histogram helper `WIDTH_BUCKET`. Every function is checked
 * for its sql, its result type, and its numeric argument requirement.
 */
class F2_MathFunc extends munit.FunSuite:
    test("trigonometric functions print the expected sql"):
        assertEquals(
            sql(from(Wide).map(w => (a = cos(w.id), b = tan(w.id), c = asin(w.id), d = acos(w.id), e = atan(w.id)))),
            lines(
                "SELECT",
                """    COS("t1"."id") AS "c1",""",
                """    TAN("t1"."id") AS "c2",""",
                """    ASIN("t1"."id") AS "c3",""",
                """    ACOS("t1"."id") AS "c4",""",
                """    ATAN("t1"."id") AS "c5"""",
                "FROM",
                """    "wide" AS "t1""""
            )
        )

    test("hyperbolic functions print the expected sql"):
        assertEquals(
            sql(from(Wide).map(w => (a = sinh(w.id), b = cosh(w.id), c = tanh(w.id)))),
            lines(
                "SELECT",
                """    SINH("t1"."id") AS "c1",""",
                """    COSH("t1"."id") AS "c2",""",
                """    TANH("t1"."id") AS "c3"""",
                "FROM",
                """    "wide" AS "t1""""
            )
        )

    test("logarithmic and exponential functions print the expected sql"):
        assertEquals(
            sql(from(Wide).map(w => (a = log(w.id, 2), b = log10(w.id), c = exp(w.id)))),
            lines(
                "SELECT",
                """    LOG("t1"."id", 2) AS "c1",""",
                """    LOG10("t1"."id") AS "c2",""",
                """    EXP("t1"."id") AS "c3"""",
                "FROM",
                """    "wide" AS "t1""""
            )
        )

    test("width bucket prints the expected sql"):
        assertEquals(
            sql(from(Wide).map(w => widthBucket(w.id, 1, 10, 3))),
            lines(
                "SELECT",
                """    WIDTH_BUCKET("t1"."id", 1, 10, 3) AS "c1"""",
                "FROM",
                """    "wide" AS "t1""""
            )
        )

    test("abs, mod, sin and ln print the expected sql"):
        assertEquals(
            sql(from(Wide).map(w => (a = abs(w.id), b = mod(w.id, 2), c = sin(w.id), d = ln(w.id)))),
            lines(
                "SELECT",
                """    ABS("t1"."id") AS "c1",""",
                """    MOD("t1"."id", 2) AS "c2",""",
                """    SIN("t1"."id") AS "c3",""",
                """    LN("t1"."id") AS "c4"""",
                "FROM",
                """    "wide" AS "t1""""
            )
        )

    test("power, sqrt, floor, ceil and round print the expected sql"):
        assertEquals(
            sql(from(Wide).map(w => (a = power(w.id, 2), b = sqrt(w.id), c = floor(w.id), d = ceil(w.id), e = round(w.id, 2)))),
            lines(
                "SELECT",
                """    POWER("t1"."id", 2) AS "c1",""",
                """    SQRT("t1"."id") AS "c2",""",
                """    FLOOR("t1"."id") AS "c3",""",
                """    CEIL("t1"."id") AS "c4",""",
                """    ROUND("t1"."id", 2) AS "c5"""",
                "FROM",
                """    "wide" AS "t1""""
            )
        )

    test("transcendental functions yield an optional decimal"):
        assertEquals(compileErrors("from(Nums).map(n => expect[Option[BigDecimal]](cos(n.i)))"), "")
        assertEquals(compileErrors("from(Nums).map(n => expect[Option[BigDecimal]](tan(n.d)))"), "")
        assertEquals(compileErrors("from(Nums).map(n => expect[Option[BigDecimal]](sinh(n.f)))"), "")
        assertEquals(compileErrors("from(Nums).map(n => expect[Option[BigDecimal]](cosh(n.l)))"), "")
        assertEquals(compileErrors("from(Nums).map(n => expect[Option[BigDecimal]](tanh(n.bd)))"), "")
        assertEquals(compileErrors("from(Nums).map(n => expect[Option[BigDecimal]](asin(n.i)))"), "")
        assertEquals(compileErrors("from(Nums).map(n => expect[Option[BigDecimal]](acos(n.i)))"), "")
        assertEquals(compileErrors("from(Nums).map(n => expect[Option[BigDecimal]](atan(n.i)))"), "")
        assertEquals(compileErrors("from(Nums).map(n => expect[Option[BigDecimal]](log(n.i, 2)))"), "")
        assertEquals(compileErrors("from(Nums).map(n => expect[Option[BigDecimal]](log10(n.i)))"), "")
        assertEquals(compileErrors("from(Nums).map(n => expect[Option[BigDecimal]](exp(n.i)))"), "")

    test("width bucket yields an optional int"):
        assertEquals(compileErrors("from(Wide).map(w => expect[Option[Int]](widthBucket(w.id, 1, 10, 3)))"), "")

    test("math functions accept numeric operand forms"):
        assertEquals(compileErrors("from(Nums).map(n => cos(1))"), "")
        assertEquals(compileErrors("from(Nums).map(n => cos(n.i))"), "")
        assertEquals(compileErrors("from(Nums).map(n => cos(n.oi))"), "")
        assertEquals(compileErrors("from(Nums).map(n => cos(n.i + 1))"), "")
        assertEquals(compileErrors("from(Nums).map(n => cos(from(Nums).map(x => max(x.i))))"), "")
        assertEquals(compileErrors("from(Nums).map(n => log(n.i, n.l))"), "")
        assertEquals(compileErrors("from(Nums).map(n => widthBucket(n.i, n.l, n.d, n.bd))"), "")

    test("math functions reject a non-numeric operand"):
        assertEquals(
            messages(compileErrors("from(Texts).map(t => cos(t.s))")),
            List("The arguments type should be numeric, but got java.lang.String.")
        )
        assertEquals(
            messages(compileErrors("from(Wide).map(w => tan(w.flag))")),
            List("The arguments type should be numeric, but got scala.Boolean.")
        )
        assertEquals(
            messages(compileErrors("from(Times).map(t => exp(t.dt))")),
            List("The arguments type should be numeric, but got java.time.LocalDate.")
        )
        assertEquals(
            messages(compileErrors("from(Texts).map(t => sinh(t.s))")),
            List("The arguments type should be numeric, but got java.lang.String.")
        )
        assertEquals(
            messages(compileErrors("from(Texts).map(t => cosh(t.s))")),
            List("The arguments type should be numeric, but got java.lang.String.")
        )
        assertEquals(
            messages(compileErrors("from(Texts).map(t => tanh(t.s))")),
            List("The arguments type should be numeric, but got java.lang.String.")
        )
        assertEquals(
            messages(compileErrors("from(Texts).map(t => asin(t.s))")),
            List("The arguments type should be numeric, but got java.lang.String.")
        )
        assertEquals(
            messages(compileErrors("from(Texts).map(t => acos(t.s))")),
            List("The arguments type should be numeric, but got java.lang.String.")
        )
        assertEquals(
            messages(compileErrors("from(Texts).map(t => atan(t.s))")),
            List("The arguments type should be numeric, but got java.lang.String.")
        )
        assertEquals(
            messages(compileErrors("from(Texts).map(t => log10(t.s))")),
            List("The arguments type should be numeric, but got java.lang.String.")
        )

    test("log checks both argument positions"):
        assertEquals(
            messages(compileErrors("from(Texts).map(t => log(t.s, 2))")),
            List("The arguments type should be numeric, but got java.lang.String.")
        )
        assertEquals(
            messages(compileErrors("""from(Wide).map(w => log(w.id, "x"))""")),
            List("The arguments type should be numeric, but got java.lang.String.")
        )

    test("width bucket checks all four argument positions"):
        assertEquals(
            messages(compileErrors("""from(Wide).map(w => widthBucket(w.name, 1, 10, 3))""")),
            List("The arguments type should be numeric, but got java.lang.String.")
        )
        assertEquals(
            messages(compileErrors("""from(Wide).map(w => widthBucket(w.id, "x", 10, 3))""")),
            List("The arguments type should be numeric, but got java.lang.String.")
        )
        assertEquals(
            messages(compileErrors("""from(Wide).map(w => widthBucket(w.id, 1, "x", 3))""")),
            List("The arguments type should be numeric, but got java.lang.String.")
        )
        assertEquals(
            messages(compileErrors("""from(Wide).map(w => widthBucket(w.id, 1, 10, "x"))""")),
            List("The arguments type should be numeric, but got java.lang.String.")
        )