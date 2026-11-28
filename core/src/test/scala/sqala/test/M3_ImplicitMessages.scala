package sqala.test

import sqala.metadata.*

/**
 * implicitNotFound messages of the metadata conversion type classes.
 * Every summon targets a type with no given instance, so the compiler
 * must surface the trait's own annotation message.
 */
class M3_ImplicitMessages extends munit.FunSuite:
    private def check(errors: String, msg: String): Unit =
        assert(errors.contains(msg), s"expected message: $msg")

    test("SQL data type classes report their conversion messages"):
        check(compileErrors("summon[SqlNumber[Thread]]"), "This value cannot be converted to SQL numeric.")
        check(compileErrors("summon[SqlDate[Thread]]"), "This value cannot be converted to SQL date.")
        check(compileErrors("summon[SqlDateTime[Thread]]"), "This value cannot be converted to SQL timestamp.")
        check(compileErrors("summon[SqlTime[Thread]]"), "This value cannot be converted to SQL time.")
        check(compileErrors("summon[SqlString[Thread]]"), "This value cannot be converted to SQL string.")
        check(compileErrors("summon[SqlBoolean[Thread]]"), "This value cannot be converted to SQL boolean.")
        check(compileErrors("summon[SqlJson[Thread]]"), "This value cannot be converted to SQL JSON.")
        check(compileErrors("summon[SqlGeometry[Thread]]"), "This value cannot be converted to SQL geometry.")
        check(compileErrors("summon[SqlInterval[Thread]]"), "This value cannot be converted to SQL interval.")

    test("AsSqlExpr and AsSqlExprs report their conversion messages"):
        check(compileErrors("summon[AsSqlExpr[Thread]]"), "This value cannot be converted to an SQL expression.")
        check(compileErrors("summon[AsSqlExprs[Thread]]"), "This value cannot be converted to an SQL expression.")

    test("CustomField reports its underlying-type message"):
        check(
            compileErrors("summon[CustomField[Thread, Thread]]"),
            "The underlying type of this CustomField cannot be converted to an SQL expression."
        )

    test("FetchPrimaryKey reports its resolution message"):
        check(compileErrors("summon[FetchPrimaryKey[Thread]]"), "Cannot resolve the primary key of this entity.")

    test("FetchCompanion reports its resolution message"):
        check(compileErrors("summon[FetchCompanion[Thread]]"), "Cannot resolve the companion class of this entity.")

    test("built-in instances resolve for supported types"):
        val _ = summon[SqlNumber[Int]]
        val _ = summon[SqlDate[java.time.LocalDate]]
        val _ = summon[SqlString[String]]
        val _ = summon[SqlBoolean[Boolean]]
        val _ = summon[AsSqlExpr[Int]]
        val _ = summon[AsSqlExprs[(Int, String)]]