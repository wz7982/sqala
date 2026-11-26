package sqala.test

import sqala.static.dsl.*
import sqala.test.StaticFixture.*

/**
 * Padding and trimming functions, regular expression matching, byte
 * length, and the string operators `toLowerCase`, `toUpperCase`,
 * `trim` and `length`.
 */
class F1_StringFunc extends munit.FunSuite:
    test("padding and trimming print the expected sql"):
        assertEquals(
            sql(from(Wide).map(w => (
                a = rpad(w.name, 5),
                b = rpad(w.name, 5, "x"),
                c = ltrim(w.name),
                d = ltrim(w.name, "x"),
                e = rtrim(w.name),
                f = rtrim(w.name, "x")
            ))),
            lines(
                "SELECT",
                """    RPAD("t1"."name", 5) AS "c1",""",
                """    RPAD("t1"."name", 5, 'x') AS "c2",""",
                """    LTRIM("t1"."name") AS "c3",""",
                """    LTRIM("t1"."name", 'x') AS "c4",""",
                """    RTRIM("t1"."name") AS "c5",""",
                """    RTRIM("t1"."name", 'x') AS "c6"""",
                "FROM",
                """    "wide" AS "t1""""
            )
        )

    test("regular expression and byte length print the expected sql"):
        assertEquals(
            sql(from(Wide).map(w => (a = regexpLike(w.name, "x"), b = octetLength(w.name)))),
            lines(
                "SELECT",
                """    REGEXP_LIKE("t1"."name", 'x') AS "c1",""",
                """    OCTET_LENGTH("t1"."name") AS "c2"""",
                "FROM",
                """    "wide" AS "t1""""
            )
        )

    test("string operators print the expected sql"):
        assertEquals(
            sql(from(Wide).map(w => (a = w.name.toLowerCase, b = w.name.toUpperCase, c = w.name.trim, d = w.name.length))),
            lines(
                "SELECT",
                """    LOWER("t1"."name") AS "c1",""",
                """    UPPER("t1"."name") AS "c2",""",
                """    TRIM("t1"."name") AS "c3",""",
                """    CHAR_LENGTH("t1"."name") AS "c4"""",
                "FROM",
                """    "wide" AS "t1""""
            )
        )

    test("padding and trimming yield an optional string"):
        assertEquals(compileErrors("""from(Wide).map(w => expect[Option[String]](rpad(w.name, 5)))"""), "")
        assertEquals(compileErrors("""from(Wide).map(w => expect[Option[String]](rpad(w.name, 5, "x")))"""), "")
        assertEquals(compileErrors("from(Wide).map(w => expect[Option[String]](ltrim(w.name)))"), "")
        assertEquals(compileErrors("""from(Wide).map(w => expect[Option[String]](ltrim(w.name, "x")))"""), "")
        assertEquals(compileErrors("from(Wide).map(w => expect[Option[String]](rtrim(w.name)))"), "")
        assertEquals(compileErrors("""from(Wide).map(w => expect[Option[String]](rtrim(w.name, "x")))"""), "")

    test("regular expression matching yields an optional boolean"):
        assertEquals(compileErrors("""from(Wide).map(w => expect[Option[Boolean]](regexpLike(w.name, "x")))"""), "")

    test("byte length yields an optional int"):
        assertEquals(compileErrors("from(Wide).map(w => expect[Option[Int]](octetLength(w.name)))"), "")

    test("case conversion and trim preserve the operand type"):
        assertEquals(compileErrors("from(Wide).map(w => expect[String](w.name.toLowerCase))"), "")
        assertEquals(compileErrors("from(Wide).map(w => expect[String](w.name.toUpperCase))"), "")
        assertEquals(compileErrors("from(Wide).map(w => expect[String](w.name.trim))"), "")
        assertEquals(compileErrors("from(Texts).map(t => expect[Option[String]](t.os.toLowerCase))"), "")
        assertEquals(compileErrors("from(Texts).map(t => expect[Option[String]](t.os.trim))"), "")

    test("length yields an optional int like charLength"):
        assertEquals(compileErrors("from(Wide).map(w => expect[Option[Int]](w.name.length))"), "")
        assertEquals(compileErrors("from(Wide).map(w => expect[Option[Int]](charLength(w.name)))"), "")

    test("string functions accept string operand forms"):
        assertEquals(compileErrors("""from(Wide).map(w => ltrim("x"))"""), "")
        assertEquals(compileErrors("from(Wide).map(w => ltrim(w.name))"), "")
        assertEquals(compileErrors("from(Texts).map(t => ltrim(t.os))"), "")
        assertEquals(compileErrors("""from(Wide).map(w => ltrim(w.name ++ "x"))"""), "")
        assertEquals(compileErrors("from(Wide).map(w => ltrim(from(Wide).map(x => max(x.name))))"), "")

    test("string functions reject a non-string operand"):
        assertEquals(
            messages(compileErrors("from(Wide).map(w => ltrim(w.id))")),
            List("The arguments type should be string, but got scala.Int.")
        )
        assertEquals(
            messages(compileErrors("from(Wide).map(w => rtrim(w.flag))")),
            List("The arguments type should be string, but got scala.Boolean.")
        )
        assertEquals(
            messages(compileErrors("""from(Wide).map(w => regexpLike(w.id, "x"))""")),
            List("The arguments type should be string, but got scala.Int.")
        )
        assertEquals(
            messages(compileErrors("from(Wide).map(w => octetLength(w.id))")),
            List("The arguments type should be string, but got scala.Int.")
        )

    test("string operators reject a non-string operand"):
        assert(compileErrors("from(Wide).map(w => w.id.toLowerCase)").nonEmpty)
        assert(compileErrors("from(Wide).map(w => w.flag.trim)").nonEmpty)
        assert(compileErrors("from(Wide).map(w => w.id.length)").nonEmpty)

    test("padding rejects a non-numeric length and a non-string pad"):
        assertEquals(
            messages(compileErrors("""from(Wide).map(w => rpad(w.name, "x"))""")),
            List("The arguments type should be numeric, but got java.lang.String.")
        )
        assertEquals(
            messages(compileErrors("from(Wide).map(w => rpad(w.name, 5, 1))")),
            List("The arguments type should be string, but got scala.Int.")
        )

    test("trimming rejects a non-string character set"):
        assertEquals(
            messages(compileErrors("from(Wide).map(w => ltrim(w.name, 1))")),
            List("The arguments type should be string, but got scala.Int.")
        )
        assertEquals(
            messages(compileErrors("from(Wide).map(w => rtrim(w.name, w.id))")),
            List("The arguments type should be string, but got scala.Int.")
        )