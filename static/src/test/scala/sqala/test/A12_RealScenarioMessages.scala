package sqala.test

import sqala.static.dsl.*
import sqala.test.StaticFixture.*

/**
 * Real-scenario checks that the implicitNotFound annotations surface
 * through the public DSL: projecting, filtering, sorting, grouping or
 * joining an unsupported value must report the conversion message of
 * the outermost conversion type class.
 */
class A12_RealScenarioMessages extends munit.FunSuite:
    private def check(errors: String, msg: String): Unit =
        assert(errors.contains(msg), s"expected message: $msg")

    test("map rejects unsupported projections with the projection message"):
        check(compileErrors("from(User).map(u => new Thread)"), "This value cannot be converted to a projection expression.")
        check(compileErrors("from(User).map(u => (u.id, new Thread))"), "This value cannot be converted to a projection expression.")
        check(compileErrors("from(User).mapDistinct(u => new Thread)"), "This value cannot be converted to a projection expression.")

    test("mapDistinct rejects bare values with the TransformKind message"):
        check(compileErrors("from(User).mapDistinct(u => 1)"), "This value cannot be converted to an expression.")

    test("filter, having and on reject unsupported predicates with the expression message"):
        check(compileErrors("from(User).filter(u => new Thread)"), "This value cannot be converted to a query expression.")
        check(compileErrors("from(User).groupBy(u => u.id).having((g, u) => new Thread)"), "This value cannot be converted to a query expression.")
        check(compileErrors("from(User.join(Post).on((u, p) => new Thread))"), "This value cannot be converted to a query expression.")

    test("comparison rejects unsupported operands with the comparison message"):
        check(compileErrors("from(User).filter(u => u.id === new Thread)"), "This value cannot be converted to a comparison operand.")

    test("sortBy rejects unsupported keys with the sort message"):
        check(compileErrors("from(User).sortBy(u => new Thread)"), "This value cannot be converted to a sort expression.")
        check(compileErrors("from(User).mapDistinct(u => u.id).sortBy(x => new Thread)"), "This value cannot be converted to a sort expression.")
        check(compileErrors("from(User).groupBy(u => u.id).map((g, u) => (g, count())).sortBy((g, u) => new Thread)"), "This value cannot be converted to a sort expression.")
        check(compileErrors("from(User).map(u => count().over(sortBy(new Thread)))"), "This value cannot be converted to a sort expression.")

    test("groupBy rejects unsupported keys with the grouping message"):
        check(compileErrors("from(User).groupBy(u => new Thread)"), "This value cannot be converted to a grouping expression.")

    test("partitionBy rejects unsupported keys with the partition message"):
        check(compileErrors("from(User).map(u => count().over(partitionBy(new Thread)))"), "This value cannot be converted to a partition expression.")

    test("from and joins reject unsupported sources with the table message"):
        check(compileErrors("from(new Thread)"), "This value cannot be converted to a table.")
        check(compileErrors("from(User.join(new Thread))"), "This value cannot be converted to a table.")
        check(compileErrors("from(User.leftJoin(new Thread))"), "This value cannot be converted to a table.")

    test("supported projections compile and print"):
        assert(sql(from(User).map(u => u.id)).contains("SELECT"))

    test("unsupported sources abort the metadata macro without a stack trace"):
        assert(!compileErrors("from(new Thread)").contains("NoSuchElementException"))
        assert(!compileErrors("from(User.join(new Thread))").contains("NoSuchElementException"))
        assert(!compileErrors("from(User.leftJoin(new Thread))").contains("NoSuchElementException"))

    test("cascade failures report the invalid-expression message instead of raw implicit noise"):
        val compare = compileErrors("from(User).filter(u => u.id === new Thread)")
        assert(compare.contains("This is not a valid expression."))
        assert(!compare.contains("No given instance"))
        val partition = compileErrors("from(User).map(u => count().over(partitionBy(new Thread)))")
        assert(partition.contains("This is not a valid expression."))
        assert(!partition.contains("No given instance"))
        val overSort = compileErrors("from(User).map(u => count().over(sortBy(new Thread)))")
        assert(overSort.contains("This is not a valid expression."))
        assert(!overSort.contains("No given instance"))