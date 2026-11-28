package sqala.test

/**
 * implicitNotFound messages of the JDBC codec type classes. Every
 * summon targets a type with no given instance, so the compiler must
 * surface the trait's own annotation message.
 */
class J9_ImplicitMessages extends munit.FunSuite:
    private def check(errors: String, msg: String): Unit =
        assert(errors.contains(msg), s"expected message: $msg")

    test("JdbcEncoder reports its conversion message"):
        check(compileErrors("summon[sqala.jdbc.JdbcEncoder[Thread]]"), "This value cannot be converted to a JDBC parameter.")

    test("JdbcDecoder reports its conversion message"):
        check(compileErrors("summon[sqala.jdbc.JdbcDecoder[Thread]]"), "A JDBC result cannot be converted to this type.")

    test("JdbcArrayItemEncoder reports its conversion message"):
        check(compileErrors("summon[sqala.jdbc.JdbcArrayItemEncoder[Thread]]"), "This value cannot be converted to a JDBC array element.")

    test("JdbcArrayItemDecoder reports its conversion message"):
        check(compileErrors("summon[sqala.jdbc.JdbcArrayItemDecoder[Thread]]"), "A JDBC array element cannot be converted to this type.")