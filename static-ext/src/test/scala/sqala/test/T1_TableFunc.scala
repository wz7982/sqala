package sqala.test

import sqala.static.dsl.*
import sqala.static.ext.dsl.*
import sqala.test.ExtFixture.*

case class Series(x: Int)
case class GenRow[T](x: T)

def generateSeries[L <: Int](start: Int, end: Int)(using QueryContext[L]) =
    createTableFunc[Series, L]("generate_series", start.asExpr :: end.asExpr :: Nil, false)

/**
 * Table functions: `unnest`, `unnestWithOrdinal`, `jsonTable` and
 * `createTableFunc`. Covers the generated sql, lateral joins and
 * the row shapes.
 */
class T1_TableFunc extends munit.FunSuite:
    test("unnest prints the array operand with a column alias"):
        assertEquals(
            sql(from(unnest(Array(1, 2, 3)))),
            lines(
                "SELECT",
                """    "t1"."x" AS "c1"""",
                "FROM",
                """    UNNEST(ARRAY[1, 2, 3]) AS "t1"("x")"""
            )
        )

    test("unnest rows expose their element field"):
        assertEquals(
            sql(from(unnest(Array(1, 2, 3))).map(t => t.x)),
            lines(
                "SELECT",
                """    "t1"."x" AS "c1"""",
                "FROM",
                """    UNNEST(ARRAY[1, 2, 3]) AS "t1"("x")"""
            )
        )
        assertEquals(
            sql(from(unnest(Array(1, 2, 3))).filter(t => t.x > 1)),
            lines(
                "SELECT",
                """    "t1"."x" AS "c1"""",
                "FROM",
                """    UNNEST(ARRAY[1, 2, 3]) AS "t1"("x")""",
                "WHERE",
                """    "t1"."x" > 1"""
            )
        )

    test("unnestWithOrdinal adds the ordinal column"):
        assertEquals(
            sql(from(unnestWithOrdinal(Array(1, 2, 3)))),
            lines(
                "SELECT",
                """    "t1"."x" AS "c1",""",
                """    "t1"."ordinal" AS "c2"""",
                "FROM",
                """    UNNEST(ARRAY[1, 2, 3]) WITH ORDINALITY AS "t1"("x", "ordinal")"""
            )
        )

    test("unnestWithOrdinal rows expose their fields"):
        assertEquals(
            sql(from(unnestWithOrdinal(Array(1, 2, 3))).map(t => (t.x, t.ordinal))),
            lines(
                "SELECT",
                """    "t1"."x" AS "c1",""",
                """    "t1"."ordinal" AS "c2"""",
                "FROM",
                """    UNNEST(ARRAY[1, 2, 3]) WITH ORDINALITY AS "t1"("x", "ordinal")"""
            )
        )
        assertEquals(
            sql(from(unnestWithOrdinal(Array(1, 2, 3))).filter(t => t.ordinal > 1)),
            lines(
                "SELECT",
                """    "t1"."x" AS "c1",""",
                """    "t1"."ordinal" AS "c2"""",
                "FROM",
                """    UNNEST(ARRAY[1, 2, 3]) WITH ORDINALITY AS "t1"("x", "ordinal")""",
                "WHERE",
                """    "t1"."ordinal" > 1"""
            )
        )

    test("unnest joins laterally against array columns"):
        assertEquals(
            sql(from(Arr.crossJoinLateral(a => unnest(a.nums)))),
            lines(
                "SELECT",
                """    "t1"."id" AS "c1",""",
                """    "t1"."tags" AS "c2",""",
                """    "t1"."nums" AS "c3",""",
                """    "t1"."onums" AS "c4",""",
                """    "t2"."x" AS "c5"""",
                "FROM",
                """    "arr" AS "t1"""",
                """    CROSS JOIN LATERAL UNNEST("t1"."nums") AS "t2"("x")"""
            )
        )
        assertEquals(
            sql(from(Arr.crossJoinLateral(a => unnest(a.onums)))),
            lines(
                "SELECT",
                """    "t1"."id" AS "c1",""",
                """    "t1"."tags" AS "c2",""",
                """    "t1"."nums" AS "c3",""",
                """    "t1"."onums" AS "c4",""",
                """    "t2"."x" AS "c5"""",
                "FROM",
                """    "arr" AS "t1"""",
                """    CROSS JOIN LATERAL UNNEST("t1"."onums") AS "t2"("x")"""
            )
        )
        assertEquals(
            sql(from(Arr.crossJoinLateral(a => unnest(a.tags)))),
            lines(
                "SELECT",
                """    "t1"."id" AS "c1",""",
                """    "t1"."tags" AS "c2",""",
                """    "t1"."nums" AS "c3",""",
                """    "t1"."onums" AS "c4",""",
                """    "t2"."x" AS "c5"""",
                "FROM",
                """    "arr" AS "t1"""",
                """    CROSS JOIN LATERAL UNNEST("t1"."tags") AS "t2"("x")"""
            )
        )

    test("createTableFunc prints a custom function call"):
        assertEquals(
            sql(from(createTableFunc[Series, 1]("generate_series", 1.asExpr :: 10.asExpr :: Nil, false))),
            lines(
                "SELECT",
                """    "t1"."x" AS "c1"""",
                "FROM",
                """    generate_series(1, 10) AS "t1"("x")"""
            )
        )
        assertEquals(
            sql(from(createTableFunc[Series, 1]("generate_series", 1.asExpr :: 10.asExpr :: Nil, true))),
            lines(
                "SELECT",
                """    "t1"."x" AS "c1"""",
                "FROM",
                """    generate_series(1, 10) WITH ORDINALITY AS "t1"("x")"""
            )
        )

    test("generic table function rows expose their fields"):
        assertEquals(
            sql(from(createTableFunc[GenRow[Int], 1]("f", Nil, false)).map(t => t.x)),
            lines(
                "SELECT",
                """    "t1"."x" AS "c1"""",
                "FROM",
                """    f() AS "t1"("x")"""
            )
        )

    test("createTableFunc wraps with a query context parameter"):
        assertEquals(
            sql(from(generateSeries(1, 10)).filter(t => t.x > 1)),
            lines(
                "SELECT",
                """    "t1"."x" AS "c1"""",
                "FROM",
                """    generate_series(1, 10) AS "t1"("x")""",
                "WHERE",
                """    "t1"."x" > 1"""
            )
        )

    test("createTableFunc requires both type arguments"):
        assert(compileErrors("""from(createTableFunc[Series]("generate_series", 1.asExpr :: 10.asExpr :: Nil, false))""").contains("Not enough type arguments"))

    test("jsonTable prints path columns"):
        assertEquals(
            sql(from(jsonTable("{}", "$", columns((a = pathColumn[Int]("$.a"))))).map(t => t.a)),
            lines(
                "SELECT",
                """    "t1"."c1" AS "c1"""",
                "FROM",
                "    JSON_TABLE(",
                "        '{}', '$' COLUMNS(",
                """            "c1" INTEGER PATH '$.a'""",
                "        )",
                """    ) AS "t1""""
            )
        )

    test("jsonTable prints ordinality and nested columns"):
        assertEquals(
            sql(from(jsonTable("""{"a": 1, "obj": {"b": "abc", "c": 2}}""", "$", columns((o = ordinalColumn, a = pathColumn[Int]("$.a"), n = nestedColumns("$.obj")((b = pathColumn[String]("$.b"), c = existsColumn("$.c"))))))).map(t => (t.o, t.a, t.b, t.c))),
            lines(
                "SELECT",
                """    "t1"."c1" AS "c1",""",
                """    "t1"."c2" AS "c2",""",
                """    "t1"."c3" AS "c3",""",
                """    "t1"."c4" AS "c4"""",
                "FROM",
                "    JSON_TABLE(",
                """        '{"a": 1, "obj": {"b": "abc", "c": 2}}', '$' COLUMNS(""",
                """            "c1" FOR ORDINALITY,""",
                """            "c2" INTEGER PATH '$.a',""",
                "            NESTED PATH '$.obj' COLUMNS(",
                """                "c3" VARCHAR PATH '$.b',""",
                """                "c4" BOOLEAN EXISTS PATH '$.c'""",
                "            )",
                "        )",
                """    ) AS "t1""""
            )
        )
    test("createTableFunc works as a lateral source"):
        assertEquals(
            sql(from(Arr.crossJoinLateral(a => createTableFunc[Series, 2]("f", a.id.asExpr :: Nil, false))).map((a, f) => (a.id, f.x))),
            lines(
                "SELECT",
                """    "t1"."id" AS "c1",""",
                """    "t2"."x" AS "c2"""",
                "FROM",
                """    "arr" AS "t1"""",
                """    CROSS JOIN LATERAL f("t1"."id") AS "t2"("x")"""
            )
        )

    test("lateral jsonTable columns can be selected in the outer query"):
        assertEquals(
            compileErrors("""from(Doc.crossJoinLateral(d => jsonTable(d.body, "$", columns((a = pathColumn[Int]("$.a")))))).map((d, j) => (d.id, j.a))"""),
            ""
        )

    test("lateral jsonTable prints the expected sql"):
        assertEquals(
            sql(from(Doc.crossJoinLateral(d => jsonTable(d.body, "$", columns((a = pathColumn[Int]("$.a")))))).map((d, j) => (d.id, j.a))),
            lines(
                "SELECT",
                """    "t1"."id" AS "c1",""",
                """    "t2"."c1" AS "c2"""",
                "FROM",
                """    "doc" AS "t1"""",
                "    CROSS JOIN LATERAL JSON_TABLE(",
                """        "t1"."body", '$' COLUMNS(""",
                """            "c1" INTEGER PATH '$.a'""",
                "        )",
                """    ) AS "t2""""
            )
        )
