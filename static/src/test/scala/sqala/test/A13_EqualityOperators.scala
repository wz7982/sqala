package sqala.test

import sqala.static.dsl.*
import sqala.test.StaticFixture.{lines, messages, sql}

/**
 * The intuitive `==` and `!=` operators on `Expr`: same sql and typing
 * rules as `===` and `<>`, the value-on-left and tuple fallbacks rejected
 * by universal equality, and the `WindowPart` interception. `===` and `<>`
 * remain available and produce identical sql.
 */
class A13_EqualityOperators extends munit.FunSuite:
    test("== and != print the same sql as === and <>"):
        assertEquals(
            sql(from(Wide).filter(w => w.id == 1)),
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
                """    "t1"."id" = 1"""
            )
        )
        assertEquals(
            sql(from(Wide).map(w => (a = w.id == 1, b = w.id != 1))),
            lines(
                "SELECT",
                """    "t1"."id" = 1 AS "c1",""",
                """    "t1"."id" <> 1 AS "c2"""",
                "FROM",
                """    "wide" AS "t1""""
            )
        )
        assertEquals(
            sql(from(Wide).filter(w => w.id == 1)),
            sql(from(Wide).filter(w => w.id === 1))
        )
        assertEquals(
            sql(from(Wide).filter(w => w.id != 1)),
            sql(from(Wide).filter(w => w.id <> 1))
        )

    test("== works in a join condition"):
        assertEquals(
            sql(from(User.join(Post).on((u, p) => u.id == p.userId)).map((u, p) => (u.name, p.title))),
            lines(
                "SELECT",
                """    "t1"."name" AS "c1",""",
                """    "t2"."title" AS "c2"""",
                "FROM",
                """    "user" AS "t1"""",
                """    INNER JOIN "post" AS "t2" ON "t1"."id" = "t2"."user_id""""
            )
        )
        assertEquals(
            sql(from(User.join(Post).on((u, p) => u.id != p.userId)).map((u, p) => (u.name, p.title))),
            lines(
                "SELECT",
                """    "t1"."name" AS "c1",""",
                """    "t2"."title" AS "c2"""",
                "FROM",
                """    "user" AS "t1"""",
                """    INNER JOIN "post" AS "t2" ON "t1"."id" <> "t2"."user_id""""
            )
        )

    test("== accepts a scalar subquery operand"):
        assertEquals(
            sql(from(Wide).filter(w => w.score == from(Wide).map(x => max(x.id)))),
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
                """    "t1"."score" = (""",
                "        SELECT",
                """            MAX("t2"."id") AS "c1"""",
                "        FROM",
                """            "wide" AS "t2"""",
                "    )"
            )
        )

    test("== accepts every operand form that === accepts"):
        assertEquals(compileErrors("from(Wide).map(w => w.id == 1)"), "")
        assertEquals(compileErrors("from(Wide).map(w => w.id == 1L)"), "")
        assertEquals(compileErrors("from(Wide).map(w => w.id == 1.0)"), "")
        assertEquals(compileErrors("from(Wide).map(w => w.id == Option(1))"), "")
        assertEquals(compileErrors("from(Wide).map(w => w.id == w.id)"), "")
        assertEquals(compileErrors("from(Wide).map(w => w.id == w.score)"), "")
        assertEquals(compileErrors("from(Wide).map(w => w.score == w.id)"), "")
        assertEquals(compileErrors("from(Wide).map(w => w.id == w.id + 1)"), "")
        assertEquals(compileErrors("from(Wide).map(w => w.id == from(Wide).map(x => max(x.id)))"), "")
        assertEquals(compileErrors("from(Wide).map(w => w.id == any(from(Wide).map(x => x.id)))"), "")
        assertEquals(compileErrors("from(Wide).map(w => w.id == all(from(Wide).map(x => x.id)))"), "")
        assertEquals(compileErrors("from(Wide).map(w => w.name == \"x\")"), "")
        assertEquals(compileErrors("from(Wide).map(w => w.flag == true)"), "")
        assertEquals(compileErrors("from(Wide).map(w => w.birth == \"2026-01-01\")"), "")

    test("!= follows the same operand rules"):
        assertEquals(compileErrors("from(Wide).map(w => w.id != 1L)"), "")
        assertEquals(compileErrors("from(Wide).map(w => w.id != Option(1))"), "")
        assertEquals(compileErrors("from(Wide).map(w => w.id != w.score)"), "")
        assertEquals(compileErrors("from(Wide).map(w => w.id != from(Wide).map(x => max(x.id)))"), "")
        assertEquals(compileErrors("from(Wide).map(w => w.id != any(from(Wide).map(x => x.id)))"), "")
        assertEquals(compileErrors("from(Wide).map(w => w.name != \"x\")"), "")

    test("== and != reject mismatched types with the same message"):
        assert(compileErrors("from(Wide).map(w => w.id == \"x\")").startsWith("error: Cannot compare types scala.Int and java.lang.String."))
        assert(compileErrors("from(Wide).map(w => w.id == true)").startsWith("error: Cannot compare types scala.Int and scala.Boolean."))
        assert(compileErrors("from(Wide).map(w => w.id == w.name)").startsWith("error: Cannot compare types scala.Int and java.lang.String."))
        assert(compileErrors("from(Wide).map(w => w.id == from(Wide).map(x => max(x.name)))").startsWith("error: Cannot compare types scala.Int and scala.Option[java.lang.String]."))
        assert(compileErrors("from(Wide).map(w => w.id != \"x\")").startsWith("error: Cannot compare types scala.Int and java.lang.String."))
        assert(compileErrors("from(Wide).map(w => w.id != w.name)").startsWith("error: Cannot compare types scala.Int and java.lang.String."))
        assert(compileErrors("from(Wide).map(w => w.id != any(from(Wide).map(x => x.name)))").startsWith("error: Cannot compare types scala.Int and java.lang.String."))

    test("a value on the left is rejected instead of yielding a constant"):
        assert(compileErrors("from(Wide).map(w => 1 == w.id)").contains("cannot be compared with == or !="))
        assert(compileErrors("from(Wide).map(w => 1 != w.id)").contains("cannot be compared with == or !="))
        assert(compileErrors("from(Wide).filter(w => 1 == w.id)").contains("cannot be compared with == or !="))

    test("tuples keep === and <>; raw == on tuples is rejected"):
        assertEquals(compileErrors("from(Wide).map(w => (w.id, w.name) === (1, \"x\"))"), "")
        assertEquals(compileErrors("from(Wide).map(w => (w.id, w.name) <> (1, \"x\"))"), "")
        assert(compileErrors("from(Wide).map(w => (w.id, w.name) == (1, \"x\"))").contains("cannot be compared with == or !="))
        assert(compileErrors("from(Wide).map(w => (w.id, w.name) != (1, \"x\"))").contains("cannot be compared with == or !="))

    test("window functions without over are intercepted"):
        assertEquals(
            messages(compileErrors("from(Wide).map(w => rank() == 1)")),
            List("Window function requires an over clause.")
        )
        assertEquals(
            messages(compileErrors("from(Wide).map(w => rank() != 1)")),
            List("Window function requires an over clause.")
        )
        assertEquals(compileErrors("from(Wide).map(w => count().over() == 1L)"), "")
        assertEquals(compileErrors("from(Wide).map(w => count().over() != 1L)"), "")

    test("aggregates compare under the same grouping rules"):
        assertEquals(compileErrors("from(Wide).groupBy(w => w.id).having((g, w) => sum(w.id) == 1)"), "")
        assertEquals(compileErrors("from(Wide).groupBy(w => w.id).map((g, w) => g == sum(w.id))"), "")
        assertEquals(compileErrors("from(Wide).map(w => count() == 1)"), "")
        assert(compileErrors("from(Wide).map(w => count() == w.id)").startsWith("error: Ungrouped column references must appear in the groupBy clause or be used in an aggregate function."))
        assert(compileErrors("from(Wide).map(w => w.id != sum(w.id))").startsWith("error: Ungrouped column references must appear in the groupBy clause or be used in an aggregate function."))