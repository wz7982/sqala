package sqala.test

/**
 * implicitNotFound messages of the MATCH_RECOGNIZE conversion type
 * classes. Every summon targets a type with no given instance, so the
 * compiler must surface the trait's own annotation message.
 */
class R2_ImplicitMessages extends munit.FunSuite:
    private def check(errors: String, msg: String): Unit =
        assert(errors.contains(msg), s"expected message: $msg")

    test("recognize clause conversions report their messages"):
        check(compileErrors("summon[sqala.static.ext.dsl.table.AsRecognize[Thread, 0]]"), "This value cannot be converted to a matchRecognize clause.")
        check(compileErrors("summon[sqala.static.ext.dsl.table.AliasRecognize[Thread]]"), "This value cannot be converted to a matchRecognize clause.")
        check(compileErrors("summon[sqala.static.ext.dsl.table.ReplaceTarget[Thread]]"), "This value cannot be converted to a matchRecognize clause.")

    test("recognize partition conversions report their messages"):
        check(compileErrors("summon[sqala.static.ext.dsl.table.AsRecognizePartitionItem[Thread, 0]]"), "This value cannot be converted to a matchRecognize partition expression.")
        check(compileErrors("summon[sqala.static.ext.dsl.table.AsRecognizePartition[Thread, 0]]"), "This value cannot be converted to a matchRecognize partition expression.")

    test("recognize sort conversions report their messages"):
        check(compileErrors("summon[sqala.static.ext.dsl.table.AsRecognizeSortItem[Thread, 0]]"), "This value cannot be converted to a matchRecognize sort expression.")
        check(compileErrors("summon[sqala.static.ext.dsl.table.AsRecognizeSort[Thread, 0]]"), "This value cannot be converted to a matchRecognize sort expression.")