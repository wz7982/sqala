package sqala.test

import sqala.static.dsl.*
import sqala.static.ext.dsl.*
import sqala.test.ExtFixture.*

/**
 * The `IS JSON` predicate family: `IS JSON`, `IS JSON OBJECT`,
 * `IS JSON ARRAY` and `IS JSON SCALAR`. Covers the generated sql, the
 * inferred result type, the clause positions, and the operand rule.
 */
class J3_JsonPredicate extends munit.FunSuite:
    test("is json predicates print the expected sql"):
        assertEquals(
            sql(from(Doc).filter(d => d.body.isJson).map(d => d.id)),
            lines(
                "SELECT",
                """    "t1"."id" AS "c1"""",
                "FROM",
                """    "doc" AS "t1"""",
                "WHERE",
                """    "t1"."body" IS JSON"""
            )
        )
        assertEquals(
            sql(from(Doc).filter(d => d.body.isJsonObject).map(d => d.id)),
            lines(
                "SELECT",
                """    "t1"."id" AS "c1"""",
                "FROM",
                """    "doc" AS "t1"""",
                "WHERE",
                """    "t1"."body" IS JSON OBJECT"""
            )
        )
        assertEquals(
            sql(from(Doc).filter(d => d.body.isJsonArray).map(d => d.id)),
            lines(
                "SELECT",
                """    "t1"."id" AS "c1"""",
                "FROM",
                """    "doc" AS "t1"""",
                "WHERE",
                """    "t1"."body" IS JSON ARRAY"""
            )
        )
        assertEquals(
            sql(from(Doc).filter(d => d.body.isJsonScalar).map(d => d.id)),
            lines(
                "SELECT",
                """    "t1"."id" AS "c1"""",
                "FROM",
                """    "doc" AS "t1"""",
                "WHERE",
                """    "t1"."body" IS JSON SCALAR"""
            )
        )

    test("is json predicates combine with boolean operators"):
        assertEquals(
            sql(from(Doc).filter(d => d.body.isJson && d.body.isJsonArray).map(d => d.id)),
            lines(
                "SELECT",
                """    "t1"."id" AS "c1"""",
                "FROM",
                """    "doc" AS "t1"""",
                "WHERE",
                """    "t1"."body" IS JSON AND "t1"."body" IS JSON ARRAY"""
            )
        )

    test("is json predicates work in having and on"):
        assertEquals(
            sql(from(Doc).groupBy(d => d.id).having((_, d) => max(d.body).isJson).map((_, _) => count())),
            lines(
                "SELECT",
                """    COUNT(*) AS "c1"""",
                "FROM",
                """    "doc" AS "t1"""",
                "GROUP BY",
                """    "t1"."id"""",
                "HAVING",
                """    MAX("t1"."body") IS JSON"""
            )
        )
        assertEquals(compileErrors("from(Doc.join(Doc).on((a, b) => a.body.isJson)).map((a, b) => a.id)"), "")

    test("is json predicates yield a boolean, optional for an optional operand"):
        assertEquals(compileErrors("from(Doc).map(d => expect[Boolean](d.body.isJson))"), "")
        assertEquals(compileErrors("from(Doc).map(d => expect[Boolean](d.body.isJsonObject))"), "")
        assertEquals(compileErrors("from(Doc).map(d => expect[Boolean](d.body.isJsonArray))"), "")
        assertEquals(compileErrors("from(Doc).map(d => expect[Boolean](d.body.isJsonScalar))"), "")
        assertEquals(compileErrors("from(Doc).map(d => expect[Option[Boolean]](d.obody.isJson))"), "")
        assertEquals(compileErrors("from(Doc).map(d => expect[Option[Boolean]](d.obody.isJsonObject))"), "")
        assertEquals(compileErrors("from(Doc).map(d => expect[Option[Boolean]](d.obody.isJsonArray))"), "")
        assertEquals(compileErrors("from(Doc).map(d => expect[Option[Boolean]](d.obody.isJsonScalar))"), "")

    test("is json predicates accept string operand forms"):
        assertEquals(compileErrors("""from(Doc).filter(d => "x".isJson)"""), "")
        assertEquals(compileErrors("""from(Doc).filter(d => (d.body ++ "x").isJson)"""), "")
        assertEquals(compileErrors("from(Doc).filter(d => from(Doc).map(x => max(x.body)).isJson)"), "")

    test("is json predicates reject a non-string operand"):
        assertEquals(
            messages(compileErrors("from(Doc).map(d => d.id.isJson)")),
            List(
                "Cannot compare types scala.Int and java.lang.String.",
                "The arguments type should be string, but got scala.Int."
            )
        )
        assertEquals(
            messages(compileErrors("from(Doc).map(d => d.meta.isJson)")),
            List(
                "Cannot compare types sqala.metadata.Json and java.lang.String.",
                "The arguments type should be string, but got sqala.metadata.Json."
            )
        )