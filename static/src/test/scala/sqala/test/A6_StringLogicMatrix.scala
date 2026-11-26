package sqala.test

import sqala.static.dsl.*
import sqala.test.StaticFixture.{lines, messages, sql}

/**
 * Logical and string operators across operand forms. Both operands must
 * satisfy the operator's type requirement, not only the left one.
 */
class A6_StringLogicMatrix extends munit.FunSuite:
    test("string operators print the expected sql"):
        assertEquals(
            sql(from(Wide).map(w => (a = w.name ++ "x", b = w.name.like("%x%"), c = w.name.contains("x"), d = w.name.startsWith("x"), e = w.name.endsWith("x")))),
            lines(
                "SELECT",
                """    "t1"."name" || 'x' AS "c1",""",
                """    "t1"."name" LIKE '%x%' AS "c2",""",
                """    "t1"."name" LIKE '%x%' AS "c3",""",
                """    "t1"."name" LIKE 'x%' AS "c4",""",
                """    "t1"."name" LIKE '%x' AS "c5"""",
                "FROM",
                """    "wide" AS "t1""""
            )
        )

    test("logical operators print the expected sql"):
        assertEquals(
            sql(from(Wide).filter(w => w.flag && (w.id > 1) || !w.flag).map(w => w.name)),
            lines(
                "SELECT",
                """    "t1"."name" AS "c1"""",
                "FROM",
                """    "wide" AS "t1"""",
                "WHERE",
                """    "t1"."flag" AND "t1"."id" > 1 OR NOT("t1"."flag")"""
            )
        )

    test("logical operators accept boolean operand forms"):
        assertEquals(compileErrors("from(Wide).map(w => w.flag && true)"), "")
        assertEquals(compileErrors("from(Wide).map(w => w.flag || false)"), "")
        assertEquals(compileErrors("from(Wide).map(w => w.flag && w.flag)"), "")
        assertEquals(compileErrors("from(Wide).map(w => w.flag && (w.id > 1))"), "")
        assertEquals(compileErrors("from(Wide).map(w => (w.id > 1) || (w.name === \"x\"))"), "")
        assertEquals(compileErrors("from(Wide).map(w => w.flag && exists(from(Wide).filter(x => x.id === w.id).map(x => x.id)))"), "")
        assertEquals(compileErrors("from(Wide).map(w => true && w.flag)"), "")

    test("logical operators reject a non-boolean right operand"):
        assert(compileErrors("from(Wide).map(w => w.flag && w.id)").startsWith("error: Cannot compare types scala.Boolean and scala.Int."))
        assert(compileErrors("from(Wide).map(w => w.flag || w.name)").startsWith("error: Cannot compare types scala.Boolean and java.lang.String."))
        assert(compileErrors("from(Wide).map(w => w.flag && w.birth)").startsWith("error: Cannot compare types scala.Boolean and java.time.LocalDate."))

    test("logical operators reject a non-boolean left operand"):
        assertEquals(
            messages(compileErrors("from(Wide).map(w => w.id && w.flag)")),
            List(
                "Cannot compare types scala.Int and scala.Boolean.",
                "The arguments type should be boolean, but got scala.Int."
            )
        )
        assertEquals(
            messages(compileErrors("from(Wide).map(w => w.name || w.flag)")),
            List(
                "Cannot compare types java.lang.String and scala.Boolean.",
                "The arguments type should be boolean, but got java.lang.String."
            )
        )

    test("negation requires a boolean operand"):
        assertEquals(compileErrors("from(Wide).map(w => !w.flag)"), "")
        assertEquals(compileErrors("from(Wide).map(w => !(w.id > 1))"), "")
        assert(compileErrors("from(Wide).map(w => !w.id)").startsWith("error: The arguments type should be boolean, but got scala.Int."))
        assert(compileErrors("from(Wide).map(w => !w.name)").startsWith("error: The arguments type should be boolean, but got java.lang.String."))

    test("like accepts string operand forms"):
        assertEquals(compileErrors("from(Wide).map(w => w.name.like(\"%x%\"))"), "")
        assertEquals(compileErrors("from(Wide).map(w => w.name.like(w.name))"), "")
        assertEquals(compileErrors("from(Wide).map(w => w.name.like(upper(w.name)))"), "")
        assertEquals(compileErrors("from(Wide).map(w => w.name.like(from(Wide).map(x => max(x.name))))"), "")
        assertEquals(compileErrors("from(Wide).map(w => w.name.like(from(Wide).map(x => x.name).take(1)))"), "")

    test("like rejects a non-string right operand"):
        assert(compileErrors("from(Wide).map(w => w.name.like(1))").startsWith("error: Cannot compare types java.lang.String and scala.Int."))
        assert(compileErrors("from(Wide).map(w => w.name.like(w.flag))").startsWith("error: Cannot compare types java.lang.String and scala.Boolean."))
        assert(compileErrors("from(Wide).map(w => w.name.like(from(Wide).map(x => x.id).take(1)))").startsWith("error: Cannot compare types java.lang.String and scala.Option[scala.Int]."))
        assert(compileErrors("from(Wide).map(w => w.name.like(w.birth))").startsWith("error: The arguments type should be string, but got java.time.LocalDate."))

    test("like rejects a non-string left operand"):
        assertEquals(
            messages(compileErrors("from(Wide).map(w => w.id.like(\"%x%\"))")),
            List(
                "Cannot compare types scala.Int and java.lang.String.",
                "The arguments type should be string, but got scala.Int."
            )
        )

    test("substring predicates take a plain string pattern"):
        assertEquals(compileErrors("from(Wide).map(w => w.name.contains(\"x\"))"), "")
        assertEquals(compileErrors("from(Wide).map(w => w.name.startsWith(\"x\"))"), "")
        assertEquals(compileErrors("from(Wide).map(w => w.name.endsWith(\"x\"))"), "")
        assert(compileErrors("from(Wide).map(w => w.name.contains(1))").nonEmpty)
        assertEquals(
            messages(compileErrors("from(Wide).map(w => w.id.contains(\"x\"))")),
            List(
                "Cannot compare types scala.Int and java.lang.String.",
                "The arguments type should be string, but got scala.Int."
            )
        )

    test("concatenation accepts string operand forms"):
        assertEquals(compileErrors("from(Wide).map(w => w.name ++ \"x\")"), "")
        assertEquals(compileErrors("from(Wide).map(w => w.name ++ w.name)"), "")
        assertEquals(compileErrors("from(Wide).map(w => w.name ++ upper(w.name))"), "")
        assertEquals(compileErrors("from(Wide).map(w => w.name ++ from(Wide).map(x => max(x.name)))"), "")
        assertEquals(compileErrors("from(Wide).map(w => \"x\" ++ w.name)"), "")

    test("concatenation rejects a non-string right operand"):
        assertEquals(
            messages(compileErrors("from(Wide).map(w => w.name ++ 1)")),
            List(
                "Types java.lang.String and scala.Int are incompatible.",
                "The arguments type should be string, but got scala.Int."
            )
        )
        assert(compileErrors("from(Wide).map(w => w.name ++ w.flag)").nonEmpty)
        assert(compileErrors("from(Wide).map(w => w.name ++ w.birth)").startsWith("error: The arguments type should be string, but got java.time.LocalDate."))

    test("concatenation rejects a non-string left operand"):
        assertEquals(
            messages(compileErrors("from(Wide).map(w => w.id ++ \"x\")")),
            List(
                "Types scala.Int and java.lang.String are incompatible.",
                "The arguments type should be string, but got scala.Int."
            )
        )