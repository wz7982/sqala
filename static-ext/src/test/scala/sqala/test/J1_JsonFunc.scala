package sqala.test

import sqala.static.dsl.*
import sqala.static.ext.dsl.*
import sqala.test.ExtFixture.*

/**
 * JSON scalar functions: `JSON_SERIALIZE`, `JSON`, `JSON_VALUE`,
 * `JSON_QUERY` and `JSON_EXISTS`. Covers the generated sql, the inferred
 * result type, the operand forms, and the argument rules.
 */
class J1_JsonFunc extends munit.FunSuite:
    test("json scalar functions print the expected sql"):
        assertEquals(
            sql(from(Doc).map(d => jsonSerialize(d.meta))),
            lines(
                "SELECT",
                """    JSON_SERIALIZE("t1"."meta") AS "c1"""",
                "FROM",
                """    "doc" AS "t1""""
            )
        )
        assertEquals(
            sql(from(Doc).map(d => json(d.body))),
            lines(
                "SELECT",
                """    JSON("t1"."body") AS "c1"""",
                "FROM",
                """    "doc" AS "t1""""
            )
        )
        assertEquals(
            sql(from(Doc).map(d => jsonValue(d.meta, "$.name"))),
            lines(
                "SELECT",
                """    JSON_VALUE("t1"."meta", '$.name') AS "c1"""",
                "FROM",
                """    "doc" AS "t1""""
            )
        )
        assertEquals(
            sql(from(Doc).map(d => jsonQuery(d.meta, "$.items"))),
            lines(
                "SELECT",
                """    JSON_QUERY("t1"."meta", '$.items') AS "c1"""",
                "FROM",
                """    "doc" AS "t1""""
            )
        )

    test("jsonExists filters rows"):
        assertEquals(
            sql(from(Doc).filter(d => jsonExists(d.meta, "$.name")).map(d => d.id)),
            lines(
                "SELECT",
                """    "t1"."id" AS "c1"""",
                "FROM",
                """    "doc" AS "t1"""",
                "WHERE",
                """    JSON_EXISTS("t1"."meta", '$.name')"""
            )
        )

    test("an optional operand keeps the optional result"):
        assertEquals(
            sql(from(Doc).map(d => (a = jsonSerialize(d.ometa), b = json(d.obody), c = jsonValue(d.ometa, "$.a")))),
            lines(
                "SELECT",
                """    JSON_SERIALIZE("t1"."ometa") AS "c1",""",
                """    JSON("t1"."obody") AS "c2",""",
                """    JSON_VALUE("t1"."ometa", '$.a') AS "c3"""",
                "FROM",
                """    "doc" AS "t1""""
            )
        )

    test("json scalar functions yield optional results"):
        assertEquals(compileErrors("from(Doc).map(d => expect[Option[String]](jsonSerialize(d.meta)))"), "")
        assertEquals(compileErrors("from(Doc).map(d => expect[Option[sqala.metadata.Json]](json(d.body)))"), "")
        assertEquals(compileErrors("""from(Doc).map(d => expect[Option[String]](jsonValue(d.meta, "$.a")))"""), "")
        assertEquals(compileErrors("""from(Doc).map(d => expect[Option[sqala.metadata.Json]](jsonQuery(d.meta, "$.a")))"""), "")
        assertEquals(compileErrors("""from(Doc).map(d => expect[Option[Boolean]](jsonExists(d.meta, "$.a")))"""), "")
        assertEquals(compileErrors("""from(Doc).map(d => expect[Option[String]](jsonValue(d.ometa, "$.a")))"""), "")
        assertEquals(compileErrors("""from(Doc).map(d => expect[Option[sqala.metadata.Json]](jsonQuery(d.ometa, "$.a")))"""), "")

    test("json scalar functions accept optional columns and subqueries"):
        assertEquals(compileErrors("from(Doc).map(d => jsonSerialize(d.ometa))"), "")
        assertEquals(compileErrors("from(Doc).map(d => json(d.obody))"), "")
        assertEquals(compileErrors("from(Doc).map(d => jsonSerialize(from(Doc).map(x => max(x.meta))))"), "")
        assertEquals(compileErrors("from(Doc).map(d => json(from(Doc).map(x => max(x.body))))"), "")
        assertEquals(compileErrors("""from(Doc).map(d => jsonValue(from(Doc).map(x => max(x.meta)), "$.a"))"""), "")
        assertEquals(compileErrors("""from(Doc).map(d => jsonQuery(from(Doc).map(x => max(x.meta)), "$.a"))"""), "")
        assertEquals(compileErrors("""from(Doc).filter(d => jsonExists(from(Doc).map(x => max(x.meta)), "$.a"))"""), "")

    test("jsonSerialize and jsonQuery require a json operand"):
        assertEquals(
            messages(compileErrors("from(Doc).map(d => jsonSerialize(d.body))")),
            List("The arguments type should be json, but got java.lang.String.")
        )
        assertEquals(
            messages(compileErrors("from(Doc).map(d => jsonSerialize(d.id))")),
            List("The arguments type should be json, but got scala.Int.")
        )
        assertEquals(
            messages(compileErrors("""from(Doc).map(d => jsonQuery(d.body, "$.a"))""")),
            List("The arguments type should be json, but got java.lang.String.")
        )

    test("json requires a string operand"):
        assertEquals(
            messages(compileErrors("from(Doc).map(d => json(d.meta))")),
            List("The arguments type should be string, but got sqala.metadata.Json.")
        )

    test("jsonValue and jsonExists require a json operand"):
        assertEquals(
            messages(compileErrors("""from(Doc).map(d => jsonValue(d.body, "$.a"))""")),
            List("The arguments type should be json, but got java.lang.String.")
        )
        assertEquals(
            messages(compileErrors("""from(Doc).map(d => jsonExists(d.body, "$.a"))""")),
            List("The arguments type should be json, but got java.lang.String.")
        )

    test("the path argument must be a literal string"):
        assert(compileErrors("from(Doc).map(d => jsonValue(d.meta, d.body))").contains("Required: String"))