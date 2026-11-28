package sqala.test

import sqala.static.dsl.*
import sqala.test.StaticFixture.messages

/**
 * Type compatibility of the comparison operators, organised by the
 * `CanCompare` instance that admits each pair. Optional operands are
 * covered on both sides, and the result type must widen to
 * `Option[Boolean]` whenever either side is optional.
 */
class A8_CompareTypes extends munit.FunSuite:
    private def plain[K <: ExprKind](e: Expr[Boolean, K]): Expr[Boolean, K] = e

    private def optional[K <: ExprKind](e: Expr[Option[Boolean], K]): Expr[Option[Boolean], K] = e

    test("numeric operands compare across every numeric type"):
        assertEquals(compileErrors("from(Nums).map(t => t.i === t.i)"), "")
        assertEquals(compileErrors("from(Nums).map(t => t.i === t.l)"), "")
        assertEquals(compileErrors("from(Nums).map(t => t.i === t.f)"), "")
        assertEquals(compileErrors("from(Nums).map(t => t.i === t.d)"), "")
        assertEquals(compileErrors("from(Nums).map(t => t.i === t.bd)"), "")
        assertEquals(compileErrors("from(Nums).map(t => t.l === t.i)"), "")
        assertEquals(compileErrors("from(Nums).map(t => t.l === t.f)"), "")
        assertEquals(compileErrors("from(Nums).map(t => t.l === t.d)"), "")
        assertEquals(compileErrors("from(Nums).map(t => t.l === t.bd)"), "")
        assertEquals(compileErrors("from(Nums).map(t => t.f === t.i)"), "")
        assertEquals(compileErrors("from(Nums).map(t => t.f === t.d)"), "")
        assertEquals(compileErrors("from(Nums).map(t => t.f === t.bd)"), "")
        assertEquals(compileErrors("from(Nums).map(t => t.d === t.i)"), "")
        assertEquals(compileErrors("from(Nums).map(t => t.d === t.bd)"), "")
        assertEquals(compileErrors("from(Nums).map(t => t.bd === t.i)"), "")
        assertEquals(compileErrors("from(Nums).map(t => t.bd === t.bd)"), "")

    test("numeric operands accept an optional expression on either side"):
        assertEquals(compileErrors("from(Nums).map(t => t.oi === t.i)"), "")
        assertEquals(compileErrors("from(Nums).map(t => t.i === t.oi)"), "")
        assertEquals(compileErrors("from(Nums).map(t => t.oi === t.oi)"), "")
        assertEquals(compileErrors("from(Nums).map(t => t.oi === t.od)"), "")
        assertEquals(compileErrors("from(Nums).map(t => t.od === t.bd)"), "")
        assertEquals(compileErrors("from(Nums).map(t => t.oi === 1)"), "")
        assertEquals(compileErrors("from(Nums).map(t => t.i === Option(1))"), "")
        assertEquals(compileErrors("from(Nums).map(t => t.oi === Option(1))"), "")
        assertEquals(compileErrors("from(Nums).map(t => t.i === Option(1.0))"), "")

    test("an optional operand widens the comparison result to Option[Boolean]"):
        from(Nums).map(t => plain(t.i === 1))
        from(Nums).map(t => plain(t.i === t.l))
        from(Nums).map(t => optional(t.oi === 1))
        from(Nums).map(t => optional(t.i === t.oi))
        from(Nums).map(t => optional(t.oi === t.oi))
        from(Nums).map(t => optional(t.i === Option(1)))

    test("string and boolean operands compare within their own family"):
        assertEquals(compileErrors("from(Texts).map(t => t.s === t.s)"), "")
        assertEquals(compileErrors("from(Texts).map(t => t.s === \"x\")"), "")
        assertEquals(compileErrors("from(Texts).map(t => t.os === t.s)"), "")
        assertEquals(compileErrors("from(Texts).map(t => t.s === t.os)"), "")
        assertEquals(compileErrors("from(Texts).map(t => t.os === t.os)"), "")
        assertEquals(compileErrors("from(Texts).map(t => t.os === Option(\"x\"))"), "")
        assertEquals(compileErrors("from(Texts).map(t => t.b === t.b)"), "")
        assertEquals(compileErrors("from(Texts).map(t => t.b === true)"), "")
        assertEquals(compileErrors("from(Texts).map(t => t.ob === t.b)"), "")
        assertEquals(compileErrors("from(Texts).map(t => t.b === t.ob)"), "")
        assertEquals(compileErrors("from(Texts).map(t => t.ob === t.ob)"), "")

    test("cross-family operands are rejected"):
        assertEquals(
            messages(compileErrors("from(Texts).map(t => t.s === t.b)")),
            List("Cannot compare types java.lang.String and scala.Boolean.")
        )
        assertEquals(
            messages(compileErrors("from(Wide).map(t => t.id === t.name)")),
            List("Cannot compare types scala.Int and java.lang.String.")
        )
        assertEquals(
            messages(compileErrors("from(Wide).map(t => t.id === t.birth)")),
            List("Cannot compare types scala.Int and java.time.LocalDate.")
        )
        assertEquals(
            messages(compileErrors("from(Wide).map(t => t.flag === t.id)")),
            List("Cannot compare types scala.Boolean and scala.Int.")
        )

    test("date operands compare with an optional expression on either side"):
        assertEquals(compileErrors("from(Times).map(t => t.dt === t.dt)"), "")
        assertEquals(compileErrors("from(Times).map(t => t.dt === t.odt)"), "")
        assertEquals(compileErrors("from(Times).map(t => t.odt === t.dt)"), "")
        assertEquals(compileErrors("from(Times).map(t => t.odt === t.odt)"), "")
        from(Times).map(t => plain(t.dt === t.dt))
        from(Times).map(t => optional(t.dt === t.odt))
        from(Times).map(t => optional(t.odt === t.dt))
        from(Times).map(t => optional(t.odt === t.odt))

    test("date-time operands compare across both representations"):
        assertEquals(compileErrors("from(Times).map(t => t.ts === t.ts)"), "")
        assertEquals(compileErrors("from(Times).map(t => t.ts === t.lts)"), "")
        assertEquals(compileErrors("from(Times).map(t => t.lts === t.ts)"), "")
        assertEquals(compileErrors("from(Times).map(t => t.lts === t.lts)"), "")
        assertEquals(compileErrors("from(Times).map(t => t.ts === t.olts)"), "")
        assertEquals(compileErrors("from(Times).map(t => t.olts === t.ts)"), "")
        assertEquals(compileErrors("from(Times).map(t => t.ots === t.lts)"), "")
        assertEquals(compileErrors("from(Times).map(t => t.lts === t.ots)"), "")
        assertEquals(compileErrors("from(Times).map(t => t.ots === t.olts)"), "")
        from(Times).map(t => plain(t.ts === t.lts))
        from(Times).map(t => optional(t.ts === t.olts))

    test("time operands compare across both representations"):
        assertEquals(compileErrors("from(Times).map(t => t.tm === t.tm)"), "")
        assertEquals(compileErrors("from(Times).map(t => t.tm === t.ltm)"), "")
        assertEquals(compileErrors("from(Times).map(t => t.ltm === t.tm)"), "")
        assertEquals(compileErrors("from(Times).map(t => t.ltm === t.ltm)"), "")
        assertEquals(compileErrors("from(Times).map(t => t.tm === t.oltm)"), "")
        assertEquals(compileErrors("from(Times).map(t => t.oltm === t.tm)"), "")
        assertEquals(compileErrors("from(Times).map(t => t.otm === t.ltm)"), "")
        assertEquals(compileErrors("from(Times).map(t => t.ltm === t.otm)"), "")
        assertEquals(compileErrors("from(Times).map(t => t.otm === t.oltm)"), "")
        from(Times).map(t => plain(t.tm === t.ltm))
        from(Times).map(t => optional(t.tm === t.oltm))

    test("date and time families do not mix with each other"):
        assertEquals(
            messages(compileErrors("from(Times).map(t => t.dt === t.ts)")),
            List("Cannot compare types java.time.LocalDate and java.time.OffsetDateTime.")
        )
        assertEquals(
            messages(compileErrors("from(Times).map(t => t.dt === t.tm)")),
            List("Cannot compare types java.time.LocalDate and java.time.OffsetTime.")
        )
        assertEquals(
            messages(compileErrors("from(Times).map(t => t.ts === t.tm)")),
            List("Cannot compare types java.time.OffsetDateTime and java.time.OffsetTime.")
        )

    test("date, date-time and time operands compare with strings in both directions"):
        assertEquals(compileErrors("from(Cross).map(t => t.dt === t.s)"), "")
        assertEquals(compileErrors("from(Cross).map(t => t.s === t.dt)"), "")
        assertEquals(compileErrors("from(Cross).map(t => t.ts === t.s)"), "")
        assertEquals(compileErrors("from(Cross).map(t => t.s === t.ts)"), "")
        assertEquals(compileErrors("from(Cross).map(t => t.tm === t.s)"), "")
        assertEquals(compileErrors("from(Cross).map(t => t.s === t.tm)"), "")

    test("a string comparison against a date accepts an optional operand on either side"):
        assertEquals(compileErrors("from(Cross).map(t => t.odt === t.s)"), "")
        assertEquals(compileErrors("from(Cross).map(t => t.dt === t.os)"), "")
        assertEquals(compileErrors("from(Cross).map(t => t.odt === t.os)"), "")
        assertEquals(compileErrors("from(Cross).map(t => t.os === t.dt)"), "")
        assertEquals(compileErrors("from(Cross).map(t => t.s === t.odt)"), "")
        assertEquals(compileErrors("from(Cross).map(t => t.os === t.odt)"), "")
        from(Cross).map(t => plain(t.dt === t.s))
        from(Cross).map(t => optional(t.dt === t.os))
        from(Cross).map(t => optional(t.odt === t.s))

    test("interval operands compare with an optional expression on either side"):
        assertEquals(compileErrors("from(Times).map(t => t.iv === t.iv)"), "")
        assertEquals(compileErrors("from(Times).map(t => t.iv === t.oiv)"), "")
        assertEquals(compileErrors("from(Times).map(t => t.oiv === t.iv)"), "")
        assertEquals(compileErrors("from(Times).map(t => t.oiv === t.oiv)"), "")
        assertEquals(compileErrors("from(Times).map(t => t.iv === StaticFixture.interval)"), "")
        assertEquals(compileErrors("from(Times).map(t => t.oiv === Option(StaticFixture.interval))"), "")
        from(Times).map(t => plain(t.iv === t.iv))
        from(Times).map(t => optional(t.iv === t.oiv))

    test("json operands compare with an optional expression on either side"):
        assertEquals(compileErrors("from(Extras).map(t => t.js === t.js)"), "")
        assertEquals(compileErrors("from(Extras).map(t => t.js === t.ojs)"), "")
        assertEquals(compileErrors("from(Extras).map(t => t.ojs === t.js)"), "")
        assertEquals(compileErrors("from(Extras).map(t => t.ojs === t.ojs)"), "")
        assertEquals(compileErrors("from(Extras).map(t => t.js === StaticFixture.json)"), "")
        assertEquals(compileErrors("from(Extras).map(t => t.ojs === Option(StaticFixture.json))"), "")
        from(Extras).map(t => plain(t.js === t.js))
        from(Extras).map(t => optional(t.js === t.ojs))
        from(Extras).map(t => optional(t.ojs === t.js))
        from(Extras).map(t => optional(t.ojs === t.ojs))

    test("a json comparison result composes with other boolean expressions"):
        assertEquals(compileErrors("from(Extras).map(t => (t.js === t.js) && t.oarr.isEmpty)"), "")
        assertEquals(compileErrors("from(Extras).map(t => (t.js === t.ojs) || t.ojs.isEmpty)"), "")
        assertEquals(compileErrors("from(Extras).filter(t => t.js === t.js)"), "")
        assertEquals(compileErrors("from(Extras).filter(t => t.js === t.ojs)"), "")
        from(Extras).map(t => plain((t.js === t.js) && t.oarr.isEmpty))
        from(Extras).map(t => optional((t.js === t.ojs) && t.oarr.isEmpty))

    test("every geometry type compares with every other geometry type"):
        assertEquals(compileErrors("from(Geo).map(t => t.g === t.p)"), "")
        assertEquals(compileErrors("from(Geo).map(t => t.p === t.g)"), "")
        assertEquals(compileErrors("from(Geo).map(t => t.ls === t.p)"), "")
        assertEquals(compileErrors("from(Geo).map(t => t.p === t.ls)"), "")
        assertEquals(compileErrors("from(Geo).map(t => t.pg === t.p)"), "")
        assertEquals(compileErrors("from(Geo).map(t => t.p === t.pg)"), "")
        assertEquals(compileErrors("from(Geo).map(t => t.mp === t.p)"), "")
        assertEquals(compileErrors("from(Geo).map(t => t.p === t.mp)"), "")
        assertEquals(compileErrors("from(Geo).map(t => t.mls === t.p)"), "")
        assertEquals(compileErrors("from(Geo).map(t => t.p === t.mls)"), "")
        assertEquals(compileErrors("from(Geo).map(t => t.mpg === t.p)"), "")
        assertEquals(compileErrors("from(Geo).map(t => t.p === t.mpg)"), "")
        assertEquals(compileErrors("from(Geo).map(t => t.gc === t.p)"), "")
        assertEquals(compileErrors("from(Geo).map(t => t.p === t.gc)"), "")
        assertEquals(compileErrors("from(Geo).map(t => t.p === t.p)"), "")
        assertEquals(compileErrors("from(Geo).map(t => t.op === t.p)"), "")
        assertEquals(compileErrors("from(Geo).map(t => t.p === t.op)"), "")
        assertEquals(compileErrors("from(Geo).map(t => t.op === t.op)"), "")
        assertEquals(compileErrors("from(Geo).map(t => t.op === t.g)"), "")
        from(Geo).map(t => plain(t.p === t.g))
        from(Geo).map(t => optional(t.p === t.op))
        from(Geo).map(t => optional(t.op === t.p))

    test("array operands compare element by element"):
        assertEquals(compileErrors("from(Extras).map(t => t.arr === t.arr)"), "")
        assertEquals(compileErrors("from(Extras).map(t => t.arr === t.arrd)"), "")
        assertEquals(compileErrors("from(Extras).map(t => t.arrd === t.arr)"), "")
        assertEquals(compileErrors("from(Extras).map(t => t.arr === Array(1, 2))"), "")
        assertEquals(compileErrors("from(Extras).map(t => Array(1, 2) === t.arr)"), "")
        from(Extras).map(t => plain(t.arr === t.arr))
        from(Extras).map(t => plain(t.arr === t.arrd))

    test("array operands accept an optional array on either side"):
        assertEquals(compileErrors("from(Extras).map(t => t.arr === t.oarr)"), "")
        assertEquals(compileErrors("from(Extras).map(t => t.oarr === t.arr)"), "")
        assertEquals(compileErrors("from(Extras).map(t => t.oarr === t.oarr)"), "")
        assertEquals(compileErrors("from(Extras).map(t => t.arr === Option(Array(1, 2)))"), "")
        assertEquals(compileErrors("from(Extras).map(t => t.oarr === Array(1, 2))"), "")
        from(Extras).map(t => optional(t.arr === t.oarr))
        from(Extras).map(t => optional(t.oarr === t.arr))
        from(Extras).map(t => optional(t.oarr === t.oarr))

    test("array comparison rejects incompatible element types"):
        assert(compileErrors("from(Extras).map(t => t.arr === Array(\"x\"))").startsWith("error: Cannot compare types scala.Int and java.lang.String."))
        assert(compileErrors("from(Extras).map(t => t.arr === t.geo)").startsWith("error: Cannot compare types scala.Array[scala.Int] and sqala.metadata.Point."))

    test("a custom field compares with its own type in every optional position"):
        assertEquals(compileErrors("from(Customs).map(t => t.st === t.st)"), "")
        assertEquals(compileErrors("from(Customs).map(t => t.st === t.ost)"), "")
        assertEquals(compileErrors("from(Customs).map(t => t.ost === t.st)"), "")
        assertEquals(compileErrors("from(Customs).map(t => t.ost === t.ost)"), "")
        assertEquals(compileErrors("from(Customs).map(t => t.st === Status.Active)"), "")
        assertEquals(compileErrors("from(Customs).map(t => Status.Active === t.st)"), "")
        assertEquals(compileErrors("from(Customs).map(t => t.st === Option(Status.Active))"), "")
        assertEquals(compileErrors("from(Customs).map(t => t.ost === Status.Active)"), "")
        from(Customs).map(t => plain(t.st === t.st))
        from(Customs).map(t => optional(t.st === t.ost))
        from(Customs).map(t => optional(t.ost === t.st))
        from(Customs).map(t => optional(t.ost === t.ost))

    test("a custom field does not compare with its underlying representation"):
        assert(compileErrors("from(Customs).map(t => t.st === \"Active\")").startsWith("error: Cannot compare types sqala.test.Status and java.lang.String."))
        assert(compileErrors("from(Customs).map(t => \"Active\" === t.st)").startsWith("error: Cannot compare types java.lang.String and sqala.test.Status."))

    test("a tuple compares element by element across numeric types"):
        assertEquals(compileErrors("from(Wide).map(w => (w.id, w.score) === (1, 2L))"), "")
        assertEquals(compileErrors("from(Wide).map(w => (w.id, w.name) === (w.id, w.name))"), "")
        assertEquals(compileErrors("from(Wide).map(w => (w.id, w.name) > (1, \"x\"))"), "")
        assertEquals(compileErrors("from(Wide).map(w => (w.id, w.name, w.flag) === (1, \"x\", true))"), "")
        from(Wide).map(w => plain((w.id, w.name) === (1, "x")))
        from(Wide).map(w => optional((w.id, w.score) === (1, 2L)))

    test("a single-element tuple compares by its element type"):
        assertEquals(compileErrors("from(Wide).map(w => Tuple1(w.id) === Tuple1(1))"), "")
        assertEquals(compileErrors("from(Wide).map(w => Tuple1(w.id) === Tuple1(2L))"), "")
        assertEquals(compileErrors("from(Wide).map(w => Tuple1(w.name) === Tuple1(\"x\"))"), "")
        assertEquals(compileErrors("from(Wide).map(w => Tuple1(w.score) === Tuple1(1L))"), "")
        from(Wide).map(w => plain(Tuple1(w.id) === Tuple1(1)))
        from(Wide).map(w => optional(Tuple1(w.score) === Tuple1(1L)))

    test("a tuple comparison rejects an arity or element mismatch"):
        assert(compileErrors("from(Wide).map(w => (w.id, w.name) === (1, \"x\", 2))").nonEmpty)
        assert(compileErrors("from(Wide).map(w => (w.id, w.name) === Tuple1(1))").nonEmpty)
        assert(compileErrors("from(Wide).map(w => Tuple1(w.id) === Tuple1(\"x\"))").startsWith("error: Cannot compare types scala.Int and java.lang.String."))