package sqala.test

import java.time.LocalDateTime
import sqala.metadata.{Geometry, Interval, Json}
import sqala.static.dsl.*
import sqala.test.StaticFixture.expect

/**
 * Conditional expressions. `caseWhen` and `nullIf` widen with `Return`,
 * `coalesce` additionally drops the `Option` wrapper when the fallback is
 * non-null, and `as` always yields an optional target type.
 */
class T3_Conditional extends munit.FunSuite:
    test("a conditional widens numeric branches"):
        from(Nums).map(n => expect[Int](caseWhen(n.i > 0)(n.i).otherwise(n.i)))
        from(Nums).map(n => expect[Long](caseWhen(n.i > 0)(n.i).otherwise(n.l)))
        from(Nums).map(n => expect[BigDecimal](caseWhen(n.i > 0)(n.d).otherwise(n.bd)))
        from(Nums).map(n => expect[Option[Long]](caseWhen(n.i > 0)(n.oi).otherwise(n.l)))
        from(Nums).map(n => expect[Option[Int]](caseWhen(n.i > 0)(n.i).otherwise(n.oi)))

    test("each further branch widens the accumulated type"):
        from(Nums).map(n => expect[Long](caseWhen(n.i > 0)(n.i).when(n.i > 1)(n.l).otherwise(n.i)))
        from(Nums).map(n => expect[BigDecimal](caseWhen(n.i > 0)(n.i).when(n.i > 1)(n.l).otherwise(n.bd)))
        from(Nums).map(n => expect[Option[Double]](caseWhen(n.i > 0)(n.i).when(n.i > 1)(n.od).otherwise(n.f)))

    test("a conditional keeps the family type for non-numeric branches"):
        from(Texts).map(t => expect[String](caseWhen(t.b)(t.s).otherwise(t.s)))
        from(Texts).map(t => expect[Boolean](caseWhen(t.b)(t.b).otherwise(t.b)))
        from(Texts).map(t => expect[Option[String]](caseWhen(t.b)(t.os).otherwise(t.s)))
        from(Extras).map(e => expect[Json](caseWhen(e.js === e.js)(e.js).otherwise(e.js)))
        from(Times).map(t => expect[Interval](caseWhen(t.dt > t.dt)(t.iv).otherwise(t.iv)))

    test("a conditional widens geometry branches to the geometry supertype"):
        from(Geo).map(g => expect[Geometry](caseWhen(g.p === g.p)(g.p).otherwise(g.ls)))
        from(Geo).map(g => expect[Geometry](caseWhen(g.p === g.p)(g.mp).otherwise(g.gc)))
        from(Geo).map(g => expect[Option[Geometry]](caseWhen(g.p === g.p)(g.op).otherwise(g.p)))

    test("a conditional keeps a custom field type"):
        from(Customs).map(c => expect[Status](caseWhen(c.st === c.st)(c.st).otherwise(c.st)))
        from(Customs).map(c => expect[Option[Status]](caseWhen(c.st === c.st)(c.ost).otherwise(c.st)))
        from(Customs).map(c => expect[Option[Status]](caseWhen(c.st === c.st)(c.st).otherwise(c.ost)))

    test("a conditional widens array branches element by element"):
        from(Extras).map(e => expect[Array[Int]](caseWhen(e.js === e.js)(e.arr).otherwise(e.arr)))
        from(Extras).map(e => expect[Array[Double]](caseWhen(e.js === e.js)(e.arr).otherwise(e.arrd)))
        from(Extras).map(e => expect[Option[Array[Int]]](caseWhen(e.js === e.js)(e.oarr).otherwise(e.arr)))
        from(Extras).map(e => expect[Option[Array[Int]]](caseWhen(e.js === e.js)(e.arr).otherwise(e.oarr)))

    test("coalesce drops the Option wrapper when the fallback is non-null"):
        from(Nums).map(n => expect[Int](coalesce(n.oi, 1)))
        from(Nums).map(n => expect[Int](coalesce(n.oi, n.i)))
        from(Nums).map(n => expect[Long](coalesce(n.oi, n.l)))
        from(Nums).map(n => expect[Double](coalesce(n.od, 1)))
        from(Texts).map(t => expect[String](coalesce(t.os, "x")))
        from(Customs).map(c => expect[Status](coalesce(c.ost, c.st)))

    test("coalesce keeps the Option wrapper when the fallback is optional"):
        from(Nums).map(n => expect[Option[Int]](coalesce(n.oi, n.oi)))
        from(Nums).map(n => expect[Option[Int]](coalesce(n.i, n.oi)))
        from(Nums).map(n => expect[Option[Long]](coalesce(n.oi, Option(1L))))
        from(Texts).map(t => expect[Option[String]](coalesce(t.os, t.os)))

    test("ifNull and getOrElse behave like coalesce"):
        from(Nums).map(n => expect[Int](ifNull(n.oi, 1)))
        from(Nums).map(n => expect[Option[Int]](ifNull(n.oi, n.oi)))
        from(Nums).map(n => expect[Int](n.oi.getOrElse(1)))
        from(Nums).map(n => expect[Long](n.oi.getOrElse(n.l)))
        from(Nums).map(n => expect[Option[Int]](n.oi.getOrElse(n.oi)))

    test("nullIf always yields an optional result"):
        from(Nums).map(n => expect[Option[Int]](nullIf(n.i, 1)))
        from(Nums).map(n => expect[Option[Int]](nullIf(n.oi, 1)))
        from(Nums).map(n => expect[Option[Long]](nullIf(n.i, n.l)))
        from(Texts).map(t => expect[Option[String]](nullIf(t.s, "x")))

    test("concatenation keeps the string type and its optionality"):
        from(Texts).map(t => expect[String](t.s ++ t.s))
        from(Texts).map(t => expect[String](t.s ++ "x"))
        from(Texts).map(t => expect[Option[String]](t.os ++ t.s))
        from(Texts).map(t => expect[Option[String]](t.s ++ t.os))

    test("a cast always yields an optional target type"):
        from(Nums).map(n => expect[Option[String]](n.i.as[String]))
        from(Nums).map(n => expect[Option[Int]](n.d.as[Int]))
        from(Nums).map(n => expect[Option[Long]](n.i.as[Long]))
        from(Nums).map(n => expect[Option[BigDecimal]](n.i.as[BigDecimal]))
        from(Nums).map(n => expect[Option[String]](n.oi.as[String]))
        from(Texts).map(t => expect[Option[Json]](t.s.as[Json]))
        from(Texts).map(t => expect[Option[LocalDateTime]](t.s.as[LocalDateTime]))

    test("incompatible branch types are rejected"):
        assert(compileErrors("""from(Nums).map(n => coalesce(n.i, "x"))""").startsWith("error: Types scala.Int and java.lang.String are incompatible."))
        assert(compileErrors("""from(Nums).map(n => caseWhen(n.i > 0)(n.i).otherwise("x"))""").startsWith("error: Types scala.Int and java.lang.String are incompatible."))
        assert(compileErrors("""from(Nums).map(n => nullIf(n.i, "x"))""").startsWith("error: Types scala.Int and java.lang.String are incompatible."))
        assert(compileErrors("""from(Customs).map(c => coalesce(c.st, "Active"))""").startsWith("error: Types sqala.test.Status and java.lang.String are incompatible."))
        assert(compileErrors("""from(Times).map(t => coalesce(t.dt, t.tm))""").nonEmpty)

    test("a cast only targets a type the source can convert to"):
        assert(compileErrors("""from(Texts).map(t => t.s.as[Int])""").startsWith("error: Cannot convert type java.lang.String to type scala.Int."))
        assert(compileErrors("""from(Nums).map(n => n.i.as[Json])""").startsWith("error: Cannot convert type scala.Int to type sqala.metadata.Json."))
        assert(compileErrors("""from(Nums).map(n => n.i.as[LocalDateTime])""").nonEmpty)