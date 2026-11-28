package sqala.test

import sqala.static.dsl.*
import sqala.test.StaticFixture.{lines, sql}

/**
 * Function arguments across operand forms: literals, columns, optional
 * columns, computed expressions and scalar subqueries. Each argument
 * position is checked against its own type requirement.
 */
class A7_FuncArgMatrix extends munit.FunSuite:
    test("functions print the expected sql"):
        assertEquals(
            sql(from(Wide).map(w => (a = upper(w.name), b = charLength(w.name), c = position("x", w.name), d = substring(w.name, 1, 2), e = abs(w.id), f = mod(w.id, 2), g = power(w.id, 2L), h = round(w.id, 1)))),
            lines(
                "SELECT",
                """    UPPER("t1"."name") AS "c1",""",
                """    CHAR_LENGTH("t1"."name") AS "c2",""",
                """    POSITION('x' IN "t1"."name") AS "c3",""",
                """    SUBSTRING("t1"."name" FROM 1 FOR 2) AS "c4",""",
                """    ABS("t1"."id") AS "c5",""",
                """    MOD("t1"."id", 2) AS "c6",""",
                """    POWER("t1"."id", 2) AS "c7",""",
                """    ROUND("t1"."id", 1) AS "c8"""",
                "FROM",
                """    "wide" AS "t1""""
            )
        )

    test("numeric aggregates accept numeric operand forms"):
        assertEquals(compileErrors("from(Wide).map(w => sum(1))"), "")
        assertEquals(compileErrors("from(Wide).map(w => sum(w.id))"), "")
        assertEquals(compileErrors("from(Wide).map(w => sum(w.score))"), "")
        assertEquals(compileErrors("from(Wide).map(w => sum(w.id + 1))"), "")
        assertEquals(compileErrors("from(Wide).map(w => sum(Option(1)))"), "")
        assertEquals(compileErrors("from(Wide).map(w => sum(from(Wide).map(x => max(x.id))))"), "")
        assertEquals(compileErrors("from(Wide).map(w => sum(from(Wide).map(x => x.id).take(1)))"), "")
        assertEquals(compileErrors("from(Wide).map(w => avg(w.score))"), "")
        assertEquals(compileErrors("from(Wide).map(w => stddevPop(w.id))"), "")
        assertEquals(compileErrors("from(Wide).map(w => varSamp(w.score))"), "")

    test("numeric aggregates reject non-numeric arguments"):
        assert(compileErrors("from(Wide).map(w => sum(w.name))").startsWith("error: The arguments type should be numeric, but got java.lang.String."))
        assert(compileErrors("from(Wide).map(w => sum(\"x\"))").startsWith("error: The arguments type should be numeric, but got java.lang.String."))
        assert(compileErrors("from(Wide).map(w => avg(w.flag))").startsWith("error: The arguments type should be numeric, but got scala.Boolean."))
        assert(compileErrors("from(Wide).map(w => avg(w.birth))").startsWith("error: The arguments type should be numeric, but got java.time.LocalDate."))
        assert(compileErrors("from(Wide).map(w => sum(from(Wide).map(x => max(x.name))))").startsWith("error: The arguments type should be numeric, but got scala.Option[java.lang.String]."))

    test("count and max accept any scalar argument"):
        assertEquals(compileErrors("from(Wide).map(w => count(w.name))"), "")
        assertEquals(compileErrors("from(Wide).map(w => count(w.birth))"), "")
        assertEquals(compileErrors("from(Wide).map(w => countDistinct(w.name))"), "")
        assertEquals(compileErrors("from(Wide).map(w => max(w.birth))"), "")
        assertEquals(compileErrors("from(Wide).map(w => min(w.name))"), "")
        assertEquals(compileErrors("from(Wide).map(w => anyValue(w.flag))"), "")

    test("two-argument statistical aggregates check both arguments"):
        assertEquals(compileErrors("from(Wide).map(w => corr(w.id, w.score))"), "")
        assertEquals(compileErrors("from(Wide).map(w => covarPop(w.id, w.id + 1))"), "")
        assertEquals(compileErrors("from(Wide).map(w => regrSlope(w.id, w.score))"), "")
        assert(compileErrors("from(Wide).map(w => corr(w.id, w.name))").startsWith("error: The arguments type should be numeric, but got java.lang.String."))
        assert(compileErrors("from(Wide).map(w => corr(w.name, w.id))").startsWith("error: The arguments type should be numeric, but got java.lang.String."))
        assert(compileErrors("from(Wide).map(w => covarPop(w.id, w.birth))").startsWith("error: The arguments type should be numeric, but got java.time.LocalDate."))

    test("ordered-set aggregates check the fraction and the sort key"):
        assertEquals(compileErrors("from(Wide).map(w => percentileCont(0.5, w.id.asc))"), "")
        assertEquals(compileErrors("from(Wide).map(w => percentileDisc(0.5, w.score.desc))"), "")
        assert(compileErrors("from(Wide).map(w => percentileCont(\"x\", w.id.asc))").startsWith("error: The arguments type should be numeric, but got java.lang.String."))

    test("string aggregates check the value and the separator"):
        assertEquals(compileErrors("from(Wide).map(w => listAgg(w.name, \",\", w.id.asc))"), "")
        assertEquals(compileErrors("from(Wide).map(w => stringAgg(w.name, \",\", w.id.asc))"), "")
        assert(compileErrors("from(Wide).map(w => listAgg(w.id, \",\", w.id.asc))").startsWith("error: The arguments type should be string, but got scala.Int."))
        assert(compileErrors("from(Wide).map(w => listAgg(w.name, 1, w.id.asc))").startsWith("error: The arguments type should be string, but got scala.Int."))

    test("string functions accept string operand forms"):
        assertEquals(compileErrors("from(Wide).map(w => upper(\"x\"))"), "")
        assertEquals(compileErrors("from(Wide).map(w => upper(w.name))"), "")
        assertEquals(compileErrors("from(Wide).map(w => lower(upper(w.name)))"), "")
        assertEquals(compileErrors("from(Wide).map(w => charLength(w.name))"), "")
        assertEquals(compileErrors("from(Wide).map(w => upper(from(Wide).map(x => x.name).take(1)))"), "")
        assertEquals(compileErrors("from(Wide).map(w => position(w.name, \"x\"))"), "")
        assertEquals(compileErrors("from(Wide).map(w => btrim(w.name))"), "")

    test("string functions reject non-string arguments in every position"):
        assert(compileErrors("from(Wide).map(w => upper(1))").startsWith("error: The arguments type should be string, but got scala.Int."))
        assert(compileErrors("from(Wide).map(w => upper(w.birth))").startsWith("error: The arguments type should be string, but got java.time.LocalDate."))
        assert(compileErrors("from(Wide).map(w => charLength(w.id))").startsWith("error: The arguments type should be string, but got scala.Int."))
        assert(compileErrors("from(Wide).map(w => position(w.name, 1))").startsWith("error: The arguments type should be string, but got scala.Int."))
        assert(compileErrors("from(Wide).map(w => position(1, w.name))").startsWith("error: The arguments type should be string, but got scala.Int."))

    test("multi-argument string functions check each position"):
        assertEquals(compileErrors("from(Wide).map(w => substring(w.name, 1))"), "")
        assertEquals(compileErrors("from(Wide).map(w => substring(w.name, 1, 2))"), "")
        assertEquals(compileErrors("from(Wide).map(w => lpad(w.name, 2))"), "")
        assertEquals(compileErrors("from(Wide).map(w => lpad(w.name, 2, \"0\"))"), "")
        assertEquals(compileErrors("from(Wide).map(w => overlay(w.name, \"x\", 1))"), "")
        assert(compileErrors("from(Wide).map(w => substring(w.id, 1))").startsWith("error: The arguments type should be string, but got scala.Int."))
        assert(compileErrors("from(Wide).map(w => substring(w.name, \"x\"))").startsWith("error: The arguments type should be numeric, but got java.lang.String."))
        assert(compileErrors("from(Wide).map(w => lpad(w.name, \"x\"))").startsWith("error: The arguments type should be numeric, but got java.lang.String."))
        assert(compileErrors("from(Wide).map(w => lpad(w.name, 2, 3))").startsWith("error: The arguments type should be string, but got scala.Int."))

    test("numeric functions reject non-numeric arguments in every position"):
        assertEquals(compileErrors("from(Wide).map(w => abs(w.score))"), "")
        assertEquals(compileErrors("from(Wide).map(w => mod(w.id, 2))"), "")
        assertEquals(compileErrors("from(Wide).map(w => power(w.id, 2L))"), "")
        assertEquals(compileErrors("from(Wide).map(w => round(w.id, 1))"), "")
        assert(compileErrors("from(Wide).map(w => abs(w.name))").startsWith("error: The arguments type should be numeric, but got java.lang.String."))
        assert(compileErrors("from(Wide).map(w => sqrt(w.flag))").startsWith("error: The arguments type should be numeric, but got scala.Boolean."))
        assert(compileErrors("from(Wide).map(w => mod(w.id, w.name))").startsWith("error: The arguments type should be numeric, but got java.lang.String."))
        assert(compileErrors("from(Wide).map(w => power(w.name, w.id))").startsWith("error: The arguments type should be numeric, but got java.lang.String."))
        assert(compileErrors("from(Wide).map(w => round(w.id, \"x\"))").startsWith("error: The arguments type should be numeric, but got java.lang.String."))

    test("a quantified subquery is not a function argument"):
        assert(compileErrors("from(Wide).map(w => sum(any(from(Wide).map(x => x.id))))").nonEmpty)
        assert(compileErrors("from(Wide).map(w => upper(any(from(Wide).map(x => x.name))))").nonEmpty)
        assert(compileErrors("from(Wide).map(w => abs(all(from(Wide).map(x => x.id))))").nonEmpty)

    test("a multi-row subquery is not a function argument"):
        assert(compileErrors("from(Wide).map(w => sum(from(Wide).map(x => x.id)))").startsWith("error: More than one row returned by a subquery used as an expression."))
        assert(compileErrors("from(Wide).map(w => upper(from(Wide).map(x => x.name)))").startsWith("error: More than one row returned by a subquery used as an expression."))