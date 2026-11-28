package sqala.test

import sqala.static.dsl.*
import sqala.test.StaticFixture.{lines, sql}

/**
 * Comparison operators across every operand form: literals, optional
 * literals, columns, computed expressions, aggregates, windows, scalar
 * subqueries, quantified subqueries and tuples.
 */
class A4_CompareMatrix extends munit.FunSuite:
    test("comparison operators print the expected sql"):
        assertEquals(
            sql(from(Wide).map(w => (a = w.id === 1, b = w.id <> 1, c = w.id > 1, d = w.id >= 1, e = w.id < 1, f = w.id <= 1))),
            lines(
                "SELECT",
                """    "t1"."id" = 1 AS "c1",""",
                """    "t1"."id" <> 1 AS "c2",""",
                """    "t1"."id" > 1 AS "c3",""",
                """    "t1"."id" >= 1 AS "c4",""",
                """    "t1"."id" < 1 AS "c5",""",
                """    "t1"."id" <= 1 AS "c6"""",
                "FROM",
                """    "wide" AS "t1""""
            )
        )

    test("equality accepts every numeric operand form"):
        assertEquals(compileErrors("from(Wide).map(w => w.id === 1)"), "")
        assertEquals(compileErrors("from(Wide).map(w => w.id === 1L)"), "")
        assertEquals(compileErrors("from(Wide).map(w => w.id === 1.0)"), "")
        assertEquals(compileErrors("from(Wide).map(w => w.id === Option(1))"), "")
        assertEquals(compileErrors("from(Wide).map(w => w.id === w.id)"), "")
        assertEquals(compileErrors("from(Wide).map(w => w.id === w.score)"), "")
        assertEquals(compileErrors("from(Wide).map(w => w.score === w.id)"), "")
        assertEquals(compileErrors("from(Wide).map(w => w.id === w.id + 1)"), "")
        assertEquals(compileErrors("from(Wide).map(w => w.id === from(Wide).map(x => max(x.id)))"), "")
        assertEquals(compileErrors("from(Wide).map(w => w.id === any(from(Wide).map(x => x.id)))"), "")
        assertEquals(compileErrors("from(Wide).map(w => w.id === all(from(Wide).map(x => x.id)))"), "")
        assertEquals(compileErrors("from(Wide).map(w => 1 === w.id)"), "")

    test("equality rejects mismatched types in every operand form"):
        assert(compileErrors("from(Wide).map(w => w.id === \"x\")").startsWith("error: Cannot compare types scala.Int and java.lang.String."))
        assert(compileErrors("from(Wide).map(w => w.id === true)").startsWith("error: Cannot compare types scala.Int and scala.Boolean."))
        assert(compileErrors("from(Wide).map(w => w.id === Option(\"x\"))").startsWith("error: Cannot compare types scala.Int and scala.Option[java.lang.String]."))
        assert(compileErrors("from(Wide).map(w => w.id === w.name)").startsWith("error: Cannot compare types scala.Int and java.lang.String."))
        assert(compileErrors("from(Wide).map(w => w.id === w.flag)").startsWith("error: Cannot compare types scala.Int and scala.Boolean."))
        assert(compileErrors("from(Wide).map(w => w.id === w.birth)").startsWith("error: Cannot compare types scala.Int and java.time.LocalDate."))
        assert(compileErrors("from(Wide).map(w => w.id === w.name ++ \"x\")").startsWith("error: Cannot compare types scala.Int and java.lang.String."))
        assert(compileErrors("from(Wide).map(w => w.id === upper(w.name))").startsWith("error: Cannot compare types scala.Int and java.lang.String."))
        assert(compileErrors("from(Wide).map(w => w.id === from(Wide).map(x => max(x.name)))").startsWith("error: Cannot compare types scala.Int and scala.Option[java.lang.String]."))
        assert(compileErrors("from(Wide).map(w => w.id === any(from(Wide).map(x => x.name)))").startsWith("error: Cannot compare types scala.Int and java.lang.String."))
        assert(compileErrors("from(Wide).map(w => w.id === all(from(Wide).map(x => x.name)))").startsWith("error: Cannot compare types scala.Int and java.lang.String."))

    test("inequality follows the same rules as equality"):
        assertEquals(compileErrors("from(Wide).map(w => w.id <> 1L)"), "")
        assertEquals(compileErrors("from(Wide).map(w => w.id <> w.score)"), "")
        assertEquals(compileErrors("from(Wide).map(w => w.id <> from(Wide).map(x => max(x.id)))"), "")
        assertEquals(compileErrors("from(Wide).map(w => w.id <> any(from(Wide).map(x => x.id)))"), "")
        assert(compileErrors("from(Wide).map(w => w.id <> w.name)").startsWith("error: Cannot compare types scala.Int and java.lang.String."))
        assert(compileErrors("from(Wide).map(w => w.id <> any(from(Wide).map(x => x.name)))").startsWith("error: Cannot compare types scala.Int and java.lang.String."))

    test("ordering operators accept numeric operand forms"):
        assertEquals(compileErrors("from(Wide).map(w => w.id > 1)"), "")
        assertEquals(compileErrors("from(Wide).map(w => w.id >= 1L)"), "")
        assertEquals(compileErrors("from(Wide).map(w => w.id < w.score)"), "")
        assertEquals(compileErrors("from(Wide).map(w => w.id <= w.id + 1)"), "")
        assertEquals(compileErrors("from(Wide).map(w => w.id > from(Wide).map(x => max(x.id)))"), "")
        assertEquals(compileErrors("from(Wide).map(w => w.id > any(from(Wide).map(x => x.id)))"), "")
        assertEquals(compileErrors("from(Wide).map(w => w.id > all(from(Wide).map(x => x.id)))"), "")
        assertEquals(compileErrors("from(Wide).map(w => 1 < w.id)"), "")

    test("ordering operators reject mismatched types in every operand form"):
        assert(compileErrors("from(Wide).map(w => w.id > \"x\")").startsWith("error: Cannot compare types scala.Int and java.lang.String."))
        assert(compileErrors("from(Wide).map(w => w.id >= w.name)").startsWith("error: Cannot compare types scala.Int and java.lang.String."))
        assert(compileErrors("from(Wide).map(w => w.id < w.flag)").startsWith("error: Cannot compare types scala.Int and scala.Boolean."))
        assert(compileErrors("from(Wide).map(w => w.id <= w.birth)").startsWith("error: Cannot compare types scala.Int and java.time.LocalDate."))
        assert(compileErrors("from(Wide).map(w => w.id > from(Wide).map(x => max(x.name)))").startsWith("error: Cannot compare types scala.Int and scala.Option[java.lang.String]."))
        assert(compileErrors("from(Wide).map(w => w.id > any(from(Wide).map(x => x.name)))").startsWith("error: Cannot compare types scala.Int and java.lang.String."))
        assert(compileErrors("from(Wide).map(w => w.id > all(from(Wide).map(x => x.name)))").startsWith("error: Cannot compare types scala.Int and java.lang.String."))

    test("aggregates and windows compare within a grouped projection"):
        assertEquals(compileErrors("from(Wide).groupBy(w => w.id).map((g, w) => g === sum(w.id))"), "")
        assertEquals(compileErrors("from(Wide).groupBy(w => w.id).map((g, w) => g > count())"), "")
        assertEquals(compileErrors("from(Wide).groupBy(w => w.id).map((g, w) => g <= count().over())"), "")
        assertEquals(compileErrors("from(Wide).groupBy(w => w.id).map((g, w) => sum(w.id) === count())"), "")
        assertEquals(compileErrors("from(Wide).map(w => count() === 1)"), "")
        assertEquals(compileErrors("from(Wide).map(w => count().over() === 1L)"), "")
        assert(compileErrors("from(Wide).groupBy(w => w.id).map((g, w) => g === max(w.name))").startsWith("error: Cannot compare types scala.Int and scala.Option[java.lang.String]."))
        assert(compileErrors("from(Wide).map(w => count() === \"x\")").startsWith("error: Cannot compare types scala.Long and java.lang.String."))

    test("a plain column cannot be compared with an aggregate in the same projection"):
        assert(compileErrors("from(Wide).map(w => w.id > sum(w.id))").startsWith("error: Ungrouped column references must appear in the groupBy clause or be used in an aggregate function."))
        assert(compileErrors("from(Wide).map(w => count() === w.id)").startsWith("error: Ungrouped column references must appear in the groupBy clause or be used in an aggregate function."))

    test("strings compare with strings and with temporal values"):
        assertEquals(compileErrors("from(Wide).map(w => w.name === \"x\")"), "")
        assertEquals(compileErrors("from(Wide).map(w => w.name > \"x\")"), "")
        assertEquals(compileErrors("from(Wide).map(w => w.name === upper(w.name))"), "")
        assertEquals(compileErrors("from(Wide).map(w => w.name === any(from(Wide).map(x => x.name)))"), "")
        assertEquals(compileErrors("from(Wide).map(w => w.birth === \"2026-01-01\")"), "")
        assertEquals(compileErrors("from(Wide).map(w => w.birth > w.birth)"), "")
        assert(compileErrors("from(Wide).map(w => w.name === 1)").startsWith("error: Cannot compare types java.lang.String and scala.Int."))
        assert(compileErrors("from(Wide).map(w => w.birth === 1)").startsWith("error: Cannot compare types java.time.LocalDate and scala.Int."))

    test("booleans compare only with booleans"):
        assertEquals(compileErrors("from(Wide).map(w => w.flag === true)"), "")
        assertEquals(compileErrors("from(Wide).map(w => w.flag === w.flag)"), "")
        assertEquals(compileErrors("from(Wide).map(w => w.flag === (w.id > 1))"), "")
        assert(compileErrors("from(Wide).map(w => w.flag === 1)").startsWith("error: Cannot compare types scala.Boolean and scala.Int."))
        assert(compileErrors("from(Wide).map(w => w.flag === \"x\")").startsWith("error: Cannot compare types scala.Boolean and java.lang.String."))

    test("optional operands are accepted on both sides"):
        assertEquals(compileErrors("from(Wide).map(w => w.score === 1L)"), "")
        assertEquals(compileErrors("from(Wide).map(w => w.score === Option(1L))"), "")
        assertEquals(compileErrors("from(Wide).map(w => w.score === w.id)"), "")
        assertEquals(compileErrors("from(Wide).map(w => w.score === w.score)"), "")
        assertEquals(compileErrors("from(Wide).map(w => w.score > any(from(Wide).map(x => x.id)))"), "")
        assert(compileErrors("from(Wide).map(w => w.score === w.name)").startsWith("error: Cannot compare types scala.Option[scala.Long] and java.lang.String."))

    test("a null test needs no type match"):
        assertEquals(compileErrors("from(Wide).map(w => w.score.isEmpty)"), "")
        assertEquals(compileErrors("from(Wide).map(w => w.id.isEmpty)"), "")
        assertEquals(compileErrors("from(Wide).map(w => w.name.isEmpty)"), "")
        assertEquals(compileErrors("from(Wide).map(w => !w.id.isEmpty)"), "")

    test("tuples compare element by element"):
        assertEquals(compileErrors("from(Wide).map(w => (w.id, w.name) === (1, \"x\"))"), "")
        assertEquals(compileErrors("from(Wide).map(w => (w.id, w.name) === (w.id, w.name))"), "")
        assertEquals(compileErrors("from(Wide).map(w => (w.id, w.score) === (1L, 2L))"), "")
        assert(compileErrors("from(Wide).map(w => (w.id, w.name) === (1, 2))").startsWith("error: Cannot compare types java.lang.String and scala.Int."))
        assert(compileErrors("from(Wide).map(w => (w.id, w.name) === (\"x\", \"y\"))").startsWith("error: Cannot compare types scala.Int and java.lang.String."))