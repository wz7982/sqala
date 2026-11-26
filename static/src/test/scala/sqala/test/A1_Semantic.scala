package sqala.test

import sqala.static.dsl.*
import sqala.test.StaticFixture.{messages, sql}

/**
 * Compile-time semantic rules. Every rejection is a macro error, so the
 * assertion is on the compiler diagnostic rather than on a runtime throw.
 * The expected diagnostic must come first, so an unrelated given-search
 * failure cannot mask the rule under test.
 */
class A1_Semantic extends munit.FunSuite:
    test("an aggregate may appear in a projection"):
        assertEquals(compileErrors("from(User).map(u => count())"), "")
        assertEquals(compileErrors("from(User).map(u => count(u.id))"), "")
        assertEquals(compileErrors("from(User).map(u => sum(u.age))"), "")

    test("an aggregate may be promoted to a window function"):
        assertEquals(compileErrors("from(User).map(u => count().over())"), "")
        assertEquals(compileErrors("from(User).map(u => count(u.id).over())"), "")
        assertEquals(compileErrors("from(User).map(u => count().over(partitionBy(u.age)))"), "")

    test("a window specification may itself contain aggregates"):
        assertEquals(compileErrors("from(User).map(u => count().over(partitionBy(count())))"), "")
        assertEquals(compileErrors("from(User).map(u => count().over(partitionBy(sum(u.age))))"), "")
        assertEquals(compileErrors("from(User).map(u => count().over(partitionBy(count()).sortBy(count())))"), "")
        assert(sql(from(User).map(_ => count().over(partitionBy(count())))).contains("COUNT(*) OVER (PARTITION BY COUNT(*))"))

    test("aggregates may nest when the result is promoted to a window function"):
        assertEquals(compileErrors("from(User).map(u => count(count()).over())"), "")
        assertEquals(compileErrors("from(User).map(u => sum(count(u.id)).over())"), "")
        assertEquals(compileErrors("from(User).map(u => count(count()).over(partitionBy(count())))"), "")
        assert(sql(from(User).map(_ => count(count()).over())).contains("COUNT(COUNT(*)) OVER ()"))

    test("aggregates may not nest without a window promotion"):
        assert(compileErrors("from(User).map(u => count(count()))").startsWith("error: Aggregate function calls cannot be nested."))
        assert(compileErrors("from(User).map(u => sum(count(u.id)))").startsWith("error: Aggregate function calls cannot be nested."))
        assert(compileErrors("from(User).map(u => count(sum(u.age) + 1))").startsWith("error: Aggregate function calls cannot be nested."))

    test("an aggregate may not contain a window function"):
        assert(compileErrors("from(User).map(u => count(count().over()))").startsWith("error: Aggregate function calls cannot contain window function calls."))
        assert(compileErrors("from(User).map(u => sum(rowNumber().over()))").startsWith("error: Aggregate function calls cannot contain window function calls."))

    test("a window function may not nest in another window specification"):
        assert(compileErrors("from(User).map(u => count().over(partitionBy(count().over())))").startsWith("error: Window function calls cannot be nested."))

    test("over is only available on aggregate and window functions"):
        assert(compileErrors("from(User).map(u => u.id.over())").startsWith("error: Over specified, but this expression is not a window function nor an aggregate function."))
        assert(compileErrors("from(User).map(u => count().over().over())").startsWith("error: Over specified, but this expression is not a window function nor an aggregate function."))

    test("aggregates and windows are rejected in filter"):
        assert(compileErrors("from(User).filter(u => count() > 1)").startsWith("error: Aggregate functions are not allowed in filter."))
        assert(compileErrors("from(User).filter(u => count().over() > 1)").startsWith("error: Window functions are not allowed in filter."))

    test("aggregates and windows are rejected in groupBy"):
        assert(compileErrors("from(User).groupBy(u => count())").startsWith("error: Aggregate functions are not allowed in groupBy."))
        assert(compileErrors("from(User).groupBy(u => count().over())").startsWith("error: Window functions are not allowed in groupBy."))

    test("aggregates and windows are rejected in a join condition"):
        assert(compileErrors("from(User.join(Post).on((u, p) => count() > p.id))").startsWith("error: Aggregate functions are not allowed in on."))
        assert(compileErrors("from(User.join(Post).on((u, p) => count().over() > p.id))").startsWith("error: Window functions are not allowed in on."))

    test("aggregates and windows are rejected in a delete or update predicate"):
        assert(compileErrors("delete[User].where(u => count() > 1)").startsWith("error: Aggregate functions are not allowed in where."))
        assert(compileErrors("update[User].set(u => u.name := u.name).where(u => count().over() > 1)").startsWith("error: Window functions are not allowed in where."))

    test("an ungrouped column is rejected after groupBy"):
        assert(compileErrors("from(User).groupBy(u => u.age).map((g, u) => u.name)").startsWith("error: Ungrouped column references must appear in the groupBy clause or be used in an aggregate function."))
        assertEquals(compileErrors("from(User).groupBy(u => u.age).map((g, u) => count(u.name))"), "")
        assertEquals(compileErrors("from(User).groupBy(u => u.age).map((g, _) => (g, count()))"), "")

    test("a window specification may not reference an ungrouped column"):
        assertEquals(
            messages(compileErrors("from(User).groupBy(u => u.age).map((g, u) => count().over(partitionBy(u.name)))")),
            List(
                "Ungrouped column references must appear in the groupBy clause or be used in an aggregate function.",
                "Window function calls cannot contain ungrouped column references."
            )
        )
        assertEquals(
            messages(compileErrors("from(User).groupBy(u => u.age).map((g, _) => (g, count())).sortBy((g, u) => count().over(partitionBy(u.name)))")),
            List(
                "Ungrouped column references must appear in the groupBy clause or be used in an aggregate function.",
                "Window function calls cannot contain ungrouped column references."
            )
        )
        assertEquals(
            messages(compileErrors("from(User).groupBy(u => u.age).map((g, u) => count().over(partitionBy(u.name, u.name)))")),
            List(
                "Ungrouped column references must appear in the groupBy clause or be used in an aggregate function.",
                "Window function calls cannot contain ungrouped column references.",
                "Window function calls cannot contain ungrouped column references."
            )
        )
        assertEquals(compileErrors("from(User).groupBy(u => u.age).map((g, _) => count().over(partitionBy(g)))"), "")
        assertEquals(compileErrors("from(User).groupBy(u => u.age).map((g, _) => count().over(partitionBy(count())))"), "")

    test("a window function is rejected in having"):
        assert(compileErrors("from(User).groupBy(u => u.age).having((g, u) => count().over() > 1)").startsWith("error: Window functions are not allowed in having."))
        assertEquals(compileErrors("from(User).groupBy(u => u.age).having((g, u) => count(u.id) > 1)"), "")

    test("an aggregate may not consume an ungrouped column of an outer query"):
        assertEquals(
            messages(compileErrors("from(User).groupBy(u => u.age).map((g, u) => from(Post).map(p => count(u.name)))")),
            List(
                "More than one row returned by a subquery used as an expression.",
                "Do not aggregate ungrouped columns from the outer queries."
            )
        )
        assert(compileErrors("from(User).groupBy(u => u.age).map((g, u) => from(Post).map(p => count(u.name)).take(1))").startsWith("error: Do not aggregate ungrouped columns from the outer queries."))

    test("a grouped sort accepts aggregates and rejects ungrouped columns"):
        assertEquals(compileErrors("from(User).groupBy(u => u.age).map((g, _) => (g, count())).sortBy((g, u) => count(u.id))"), "")
        assertEquals(compileErrors("from(User).groupBy(u => u.age).map((g, _) => (g, count())).sortBy((g, _) => g.desc)"), "")
        assertEquals(compileErrors("from(User).groupBy(u => u.age).map((g, _) => (g, count())).sortBy((g, u) => count().over())"), "")
        assert(compileErrors("from(User).groupBy(u => u.age).map((g, _) => (g, count())).sortBy((g, u) => u.name)").startsWith("error: Ungrouped column references must appear in the groupBy clause or be used in an aggregate function."))
        assert(compileErrors("from(User).groupBy(u => u.age).map((g, _) => (g, count())).sortBy((g, u) => count(count()))").startsWith("error: Aggregate function calls cannot be nested."))

    test("a plain sort accepts windows but not bare aggregates"):
        assertEquals(compileErrors("from(User).sortBy(u => count().over())"), "")
        assert(compileErrors("from(User).sortBy(u => count())").startsWith("error: Ungrouped column references must appear in the groupBy clause or be used in an aggregate function."))

    test("a frame must start before it ends"):
        assert(compileErrors("from(User).map(u => count().over(partitionBy(u.id).sortBy(u.id).rowsBetween(currentRow, unboundedPreceding)))").startsWith("error: Frame starting from current row cannot end at unbounded preceding."))
        assertEquals(compileErrors("from(User).map(u => count().over(partitionBy(u.id).sortBy(u.id).rowsBetween(unboundedPreceding, currentRow)))"), "")