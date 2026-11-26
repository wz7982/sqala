package sqala.test

import sqala.static.dsl.*
import sqala.test.StaticFixture.{lines, messages, sql}

/**
 * Escape hatches for non-standard sql: `rawExpr`, `createFunc`,
 * `createBinaryExpr`, `asInlinedExpr` and `groupingSets`.
 *
 * The escape hatches produce column-like expressions, so they must stay
 * usable in `groupBy` and `sortBy`. Plain scalar values are rejected in
 * those clauses; the tests below pin both halves of that rule so a later
 * change to the kind inference cannot silently ban the escape hatches.
 */
class E1_EscapeHatch extends munit.FunSuite:
    test("rawExpr interpolates columns and values"):
        assertEquals(
            sql(from(Wide).filter(w => rawExpr"MATCH(${w.name}) AGAINST(${"x"})".as[Boolean])),
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
                """    (MATCH("t1"."name") AGAINST('x'))"""
            )
        )

    test("rawExpr escapes string values"):
        assertEquals(
            sql(from(Wide).filter(_ => rawExpr"${"a'b"}".as[Boolean])),
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
                """    ('a''b')"""
            )
        )

    test("rawExpr accepts a subquery argument"):
        assertEquals(compileErrors("""from(Wide).filter(w => rawExpr"${w.id} = ${from(Wide).map(x => max(x.id))}".as[Boolean])"""), "")

    test("a multi-row subquery argument reports an error instead of crashing the macro"):
        val out = compileErrors("""from(Wide).filter(w => rawExpr"${w.id} IN ${from(Wide).map(x => x.id)}".as[Boolean])""")
        assert(out.nonEmpty)
        assert(!out.contains("NoSuchElementException"))

    test("rawExpr requires an explicit result type"):
        assert(compileErrors("""from(Wide).filter(w => rawExpr"1 = 1")""").nonEmpty)

    test("createFunc prints a custom function call"):
        assertEquals(
            sql(from(Wide).map(w => createFunc[Option[String], 1]("LEFT", w.name.asExpr :: 2.asExpr :: Nil))),
            lines(
                "SELECT",
                """    LEFT("t1"."name", 2) AS "c1"""",
                "FROM",
                """    "wide" AS "t1""""
            )
        )

    test("createBinaryExpr prints a custom operator"):
        assertEquals(
            sql(from(Wide).map(w => createBinaryExpr[String, 1](w.name.asExpr, "||", "x".asExpr))),
            lines(
                "SELECT",
                """    "t1"."name" || 'x' AS "c1"""",
                "FROM",
                """    "wide" AS "t1""""
            )
        )

    test("createFunc is allowed in groupBy"):
        assertEquals(
            sql(from(Wide).groupBy(w => createFunc[Int, 1]("f", w.id.asExpr :: Nil)).map((_, _) => count())),
            lines(
                "SELECT",
                """    COUNT(*) AS "c1"""",
                "FROM",
                """    "wide" AS "t1"""",
                "GROUP BY",
                """    f("t1"."id")"""
            )
        )

    test("rawExpr is allowed in groupBy"):
        assertEquals(
            sql(from(Wide).groupBy(w => rawExpr"f(${w.id})".as[Int]).map((_, _) => count())),
            lines(
                "SELECT",
                """    COUNT(*) AS "c1"""",
                "FROM",
                """    "wide" AS "t1"""",
                "GROUP BY",
                """    (f("t1"."id"))"""
            )
        )

    test("createBinaryExpr is allowed in groupBy"):
        assertEquals(
            sql(from(Wide).groupBy(w => createBinaryExpr[Int, 1](w.id.asExpr, "+", 1.asExpr)).map((_, _) => count())),
            lines(
                "SELECT",
                """    COUNT(*) AS "c1"""",
                "FROM",
                """    "wide" AS "t1"""",
                "GROUP BY",
                """    "t1"."id" + 1"""
            )
        )

    test("createFunc is allowed in sortBy"):
        assertEquals(
            sql(from(Wide).sortBy(w => createFunc[Int, 1]("f", w.id.asExpr :: Nil))),
            lines(
                "SELECT",
                """    "t1"."id" AS "c1",""",
                """    "t1"."name" AS "c2",""",
                """    "t1"."flag" AS "c3",""",
                """    "t1"."birth" AS "c4",""",
                """    "t1"."score" AS "c5"""",
                "FROM",
                """    "wide" AS "t1"""",
                "ORDER BY",
                """    f("t1"."id") ASC"""
            )
        )

    test("rawExpr is allowed in sortBy"):
        assertEquals(
            sql(from(Wide).sortBy(w => rawExpr"f(${w.id})".as[Int])),
            lines(
                "SELECT",
                """    "t1"."id" AS "c1",""",
                """    "t1"."name" AS "c2",""",
                """    "t1"."flag" AS "c3",""",
                """    "t1"."birth" AS "c4",""",
                """    "t1"."score" AS "c5"""",
                "FROM",
                """    "wide" AS "t1"""",
                "ORDER BY",
                """    (f("t1"."id")) ASC"""
            )
        )

    test("createBinaryExpr is allowed in sortBy"):
        assertEquals(
            sql(from(Wide).sortBy(w => createBinaryExpr[Int, 1](w.id.asExpr, "+", 1.asExpr))),
            lines(
                "SELECT",
                """    "t1"."id" AS "c1",""",
                """    "t1"."name" AS "c2",""",
                """    "t1"."flag" AS "c3",""",
                """    "t1"."birth" AS "c4",""",
                """    "t1"."score" AS "c5"""",
                "FROM",
                """    "wide" AS "t1"""",
                "ORDER BY",
                """    "t1"."id" + 1 ASC"""
            )
        )

    test("escape hatches combine with ordering directions in sortBy"):
        assertEquals(compileErrors("""from(Wide).sortBy(w => createFunc[Int, 1]("f", w.id.asExpr :: Nil).desc)"""), "")
        assertEquals(compileErrors("""from(Wide).sortBy(w => rawExpr"f(${w.id})".as[Int].ascNullsLast)"""), "")
        assertEquals(compileErrors("""from(Wide).sortBy(w => createBinaryExpr[Int, 1](w.id.asExpr, "+", 1.asExpr).descNullsFirst)"""), "")

    test("escape hatches are allowed in having and in a window partition"):
        assertEquals(compileErrors("""from(Wide).groupBy(w => w.id).having((g, w) => createFunc[Int, 1]("f", w.id.asExpr :: Nil) > 1).map((g, w) => count())"""), "")
        assertEquals(compileErrors("""from(Wide).map(w => count().over(partitionBy(createFunc[Int, 1]("f", w.id.asExpr :: Nil))))"""), "")

    test("plain scalar values are still rejected in groupBy and sortBy"):
        assertEquals(
            messages(compileErrors("from(Wide).groupBy(w => 1).map((g, w) => count())")),
            List("Scalar value are not allowed in groupBy.")
        )
        assertEquals(
            messages(compileErrors("from(Wide).sortBy(w => 1)")),
            List("Scalar value are not allowed in sortBy.")
        )
        assertEquals(
            messages(compileErrors("from(Wide).groupBy(w => sum(w.id)).map((g, w) => count())")),
            List("Aggregate functions are not allowed in groupBy.")
        )

    test("asInlinedExpr renders a literal inline"):
        assertEquals(
            sql(from(User).filter(u => u.id === 1.asInlinedExpr)),
            lines(
                "SELECT",
                """    "t1"."id" AS "c1",""",
                """    "t1"."name" AS "c2",""",
                """    "t1"."age" AS "c3"""",
                "FROM",
                """    "user" AS "t1"""",
                "WHERE",
                """    "t1"."id" = 1"""
            )
        )
        assertEquals(
            sql(from(User).filter(u => u.id === 1.asInlinedExpr)),
            sql(from(User).filter(u => u.id === 1))
        )

    test("groupingSets prints empty, single and tuple grouping sets"):
        assertEquals(
            sql(from(User).groupBySets(u => (id = u.id, age = u.age))(g => groupingSets((), (g.id, g.age), g.id)).map((_, _) => count())),
            lines(
                "SELECT",
                """    COUNT(*) AS "c1"""",
                "FROM",
                """    "user" AS "t1"""",
                "GROUP BY",
                """    GROUPING SETS((), ("t1"."id", "t1"."age"), "t1"."id")"""
            )
        )

    test("a tuple grouping set keeps its parentheses in every position"):
        assertEquals(
            sql(from(User).groupBySets(u => (id = u.id, age = u.age))(g => groupingSets((), g.id, (g.id, g.age))).map((_, _) => count())),
            lines(
                "SELECT",
                """    COUNT(*) AS "c1"""",
                "FROM",
                """    "user" AS "t1"""",
                "GROUP BY",
                """    GROUPING SETS((), "t1"."id", ("t1"."id", "t1"."age"))"""
            )
        )

    test("groupingSets nests cube and rollup"):
        assertEquals(
            sql(from(User).groupBySets(u => (id = u.id, age = u.age))(g => groupingSets(cube(g.id, g.age), ())).map((_, _) => count())),
            lines(
                "SELECT",
                """    COUNT(*) AS "c1"""",
                "FROM",
                """    "user" AS "t1"""",
                "GROUP BY",
                """    GROUPING SETS(CUBE("t1"."id", "t1"."age"), ())"""
            )
        )
        assertEquals(
            sql(from(User).groupBySets(u => (id = u.id, age = u.age))(g => groupingSets(rollup(g.id, g.age))).map((_, _) => count())),
            lines(
                "SELECT",
                """    COUNT(*) AS "c1"""",
                "FROM",
                """    "user" AS "t1"""",
                "GROUP BY",
                """    GROUPING SETS(ROLLUP("t1"."id", "t1"."age"))"""
            )
        )

    test("groupingSets accepts a single grouping expression"):
        assertEquals(
            sql(from(User).groupBySets(u => (id = u.id, age = u.age))(g => groupingSets(g.id)).map((_, _) => count())),
            lines(
                "SELECT",
                """    COUNT(*) AS "c1"""",
                "FROM",
                """    "user" AS "t1"""",
                "GROUP BY",
                """    GROUPING SETS("t1"."id")"""
            )
        )

    test("groupingSets rejects ungrouped columns and aggregates"):
        assert(compileErrors("from(User).groupBySets(u => (id = u.id))(g => groupingSets(u.age)).map((g, u) => count())").nonEmpty)
        assert(compileErrors("from(User).groupBySets(u => (id = u.id))(g => groupingSets(sum(g.id))).map((g, u) => count())").nonEmpty)