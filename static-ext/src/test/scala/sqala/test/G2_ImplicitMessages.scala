package sqala.test

import sqala.static.ext.dsl.table.*

/**
 * implicitNotFound message of the graph schema conversion type class,
 * plus a positive check that its exhaustive tuple cases compile.
 */
class G2_ImplicitMessages extends munit.FunSuite:
    test("TransformGraphSchema reports its conversion message"):
        assert(
            compileErrors("summon[TransformGraphSchema[Thread, 0]]")
                .contains("This value cannot be converted to a graph schema.")
        )

    test("TransformGraphSchema compiles for empty tuples"):
        summon[TransformGraphSchema[EmptyTuple, 0]]