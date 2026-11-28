package sqala.test

/**
 * implicitNotFound messages of the JSON conversion type classes.
 * Every summon targets a type with no given instance, so the compiler
 * must surface the trait's own annotation message.
 */
class J4_ImplicitMessages extends munit.FunSuite:
    private def check(errors: String, msg: String): Unit =
        assert(errors.contains(msg), s"expected message: $msg")

    test("AsJsonObject reports its conversion message"):
        check(compileErrors("summon[sqala.static.ext.dsl.AsJsonObject[Thread]]"), "This value cannot be converted to a JSON object member.")

    test("JsonColumnFlatten reports its conversion message"):
        check(compileErrors("summon[sqala.static.ext.dsl.table.JsonColumnFlatten[Thread, 0]]"), "This value cannot be converted to JSON table columns.")