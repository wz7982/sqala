package sqala.test

import sqala.static.dsl.*
import sqala.test.StaticFixture.{lines, messages, sql}

/**
 * Arithmetic operators across operand forms and inner types, including
 * optional operands and scalar subqueries.
 */
class A5_ArithmeticMatrix extends munit.FunSuite:
    test("arithmetic operators print the expected sql"):
        assertEquals(
            sql(from(Wide).map(w => (a = w.id + 1, b = w.id - 1L, c = w.id * 1.0, d = w.id / 2, e = w.id % 2, f = -w.id))),
            lines(
                "SELECT",
                """    "t1"."id" + 1 AS "c1",""",
                """    "t1"."id" - 1 AS "c2",""",
                """    "t1"."id" * 1.0 AS "c3",""",
                """    CAST("t1"."id" AS DECIMAL(38, 10)) / CAST(2 AS DECIMAL(38, 10)) AS "c4",""",
                """    MOD("t1"."id", 2) AS "c5",""",
                """    -("t1"."id") AS "c6"""",
                "FROM",
                """    "wide" AS "t1""""
            )
        )

    test("arithmetic accepts every numeric operand form"):
        assertEquals(compileErrors("from(Wide).map(w => w.id + 1)"), "")
        assertEquals(compileErrors("from(Wide).map(w => w.id - 1L)"), "")
        assertEquals(compileErrors("from(Wide).map(w => w.id * 1.0)"), "")
        assertEquals(compileErrors("from(Wide).map(w => w.id / 2)"), "")
        assertEquals(compileErrors("from(Wide).map(w => w.id % 2)"), "")
        assertEquals(compileErrors("from(Wide).map(w => w.id + w.score)"), "")
        assertEquals(compileErrors("from(Wide).map(w => w.score * w.id)"), "")
        assertEquals(compileErrors("from(Wide).map(w => w.id + Option(1))"), "")
        assertEquals(compileErrors("from(Wide).map(w => w.id + w.id * 2)"), "")
        assertEquals(compileErrors("from(Wide).map(w => w.id + from(Wide).map(x => max(x.id)))"), "")
        assertEquals(compileErrors("from(Wide).map(w => 1 + w.id)"), "")

    test("arithmetic accepts aggregates and windows in a legal projection"):
        assertEquals(compileErrors("from(Wide).groupBy(w => w.id).map((g, w) => sum(w.id) + count())"), "")
        assertEquals(compileErrors("from(Wide).map(w => count() + 1)"), "")
        assertEquals(compileErrors("from(Wide).map(w => w.id + count().over())"), "")
        assertEquals(compileErrors("from(Wide).map(w => count().over() * 2L)"), "")

    test("addition rejects non-numeric operands"):
        assertEquals(
            messages(compileErrors("from(Wide).map(w => w.id + \"x\")")),
            List("Types scala.Int and java.lang.String cannot be added.")
        )
        assertEquals(
            messages(compileErrors("from(Wide).map(w => w.id + w.name)")),
            List("Types scala.Int and java.lang.String cannot be added.")
        )
        assertEquals(
            messages(compileErrors("from(Wide).map(w => w.birth + 1)")),
            List("Types java.time.LocalDate and scala.Int cannot be added.")
        )

    test("subtraction rejects non-numeric operands but allows date pairs"):
        assertEquals(compileErrors("from(Wide).map(w => w.birth - w.birth)"), "")
        assertEquals(
            messages(compileErrors("from(Wide).map(w => w.id - w.name)")),
            List("Types scala.Int and java.lang.String cannot be subtracted.")
        )

    test("multiplication, division and modulo require numeric operands"):
        assert(compileErrors("from(Wide).map(w => w.id / w.name)").startsWith("error: The arguments type should be numeric, but got java.lang.String."))
        assert(compileErrors("from(Wide).map(w => w.id % w.name)").startsWith("error: The arguments type should be numeric, but got java.lang.String."))
        assert(compileErrors("from(Wide).map(w => w.id / w.flag)").startsWith("error: The arguments type should be numeric, but got scala.Boolean."))
        assert(compileErrors("from(Wide).map(w => w.id % w.birth)").startsWith("error: The arguments type should be numeric, but got java.time.LocalDate."))
        assertEquals(
            messages(compileErrors("from(Wide).map(w => w.id * w.name)")),
            List(
                "Types scala.Int and java.lang.String are incompatible.",
                "The arguments type should be numeric, but got java.lang.String."
            )
        )

    test("unary minus requires a numeric operand"):
        assertEquals(compileErrors("from(Wide).map(w => -w.id)"), "")
        assertEquals(compileErrors("from(Wide).map(w => -w.score)"), "")
        assertEquals(compileErrors("from(Wide).map(w => -1)"), "")
        assert(compileErrors("from(Wide).map(w => -w.name)").startsWith("error: The arguments type should be numeric, but got java.lang.String."))
        assert(compileErrors("from(Wide).map(w => -w.flag)").startsWith("error: The arguments type should be numeric, but got scala.Boolean."))

    test("a quantified subquery is not an arithmetic operand"):
        assert(compileErrors("from(Wide).map(w => w.id + any(from(Wide).map(x => x.id)))").nonEmpty)
        assert(compileErrors("from(Wide).map(w => w.id * all(from(Wide).map(x => x.id)))").nonEmpty)

    test("a multi-row subquery is not an arithmetic operand"):
        assert(compileErrors("from(Wide).map(w => w.id + from(Wide).map(x => x.id))").startsWith("error: More than one row returned by a subquery used as an expression."))
        assertEquals(compileErrors("from(Wide).map(w => w.id + from(Wide).map(x => x.id).take(1))"), "")