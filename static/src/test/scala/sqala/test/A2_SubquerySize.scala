package sqala.test

import sqala.static.dsl.*
import sqala.test.StaticFixture.{lines, sql}

/**
 * Row-count inference for subqueries used as scalar expressions. A projection
 * over aggregates is single-row only when it references no outer column.
 */
class A2_SubquerySize extends munit.FunSuite:
    test("a plain projection is a multi-row subquery"):
        assert(compileErrors("from(User).map(u => from(Post).map(p => p.id))").startsWith("error: More than one row returned by a subquery used as an expression."))
        assert(compileErrors("from(User).map(u => from(Post).filter(p => p.userId === u.id).map(p => p.title))").startsWith("error: More than one row returned by a subquery used as an expression."))

    test("a projection over aggregates is a single-row subquery"):
        assertEquals(compileErrors("from(User).map(u => from(Post).filter(p => p.userId === u.id).map(_ => count()))"), "")
        assertEquals(compileErrors("from(User).map(u => from(Post).map(p => sum(p.viewCount)))"), "")

    test("aggregating an outer column keeps the subquery multi-row"):
        assert(compileErrors("from(User).map(u => from(Post).map(p => count(u.id)))").startsWith("error: More than one row returned by a subquery used as an expression."))
        assert(compileErrors("from(User).map(u => from(Post).map(p => sum(u.age)))").startsWith("error: More than one row returned by a subquery used as an expression."))
        assert(compileErrors("from(User).map(u => from(Post).map(p => count(u.id) + count()))").startsWith("error: More than one row returned by a subquery used as an expression."))

    test("a window function over an outer column keeps the subquery multi-row"):
        assert(compileErrors("from(User).map(u => from(Post).map(p => count(u.id).over()))").startsWith("error: More than one row returned by a subquery used as an expression."))
        assert(compileErrors("from(User).map(u => from(Post).map(p => count().over(partitionBy(u.id))))").startsWith("error: More than one row returned by a subquery used as an expression."))

    test("a grouped subquery is multi-row"):
        assert(compileErrors("from(User).map(u => from(Post).groupBy(p => p.userId).map((g, _) => g))").startsWith("error: More than one row returned by a subquery used as an expression."))
        assert(compileErrors("from(User).map(u => from(Post).groupBy(p => p.userId).map((g, _) => count()))").startsWith("error: More than one row returned by a subquery used as an expression."))

    test("limiting the subquery to one row makes it usable as an expression"):
        assertEquals(compileErrors("from(User).map(u => from(Post).map(p => count(u.id)).take(1))"), "")
        assertEquals(compileErrors("from(User).map(u => from(Post).map(p => p.id).take(1))"), "")
        assert(compileErrors("from(User).map(u => from(Post).map(p => p.id).take(2))").startsWith("error: More than one row returned by a subquery used as an expression."))

    test("a multi-row subquery is still valid under exists and in"):
        assertEquals(compileErrors("from(User).filter(u => exists(from(Post).filter(p => p.userId === u.id).map(p => p.id)))"), "")
        assertEquals(compileErrors("from(User).filter(u => u.id.in(from(Post).map(p => p.userId)))"), "")
        assertEquals(compileErrors("from(User).filter(u => u.age > all(from(Post).map(p => p.viewCount)))"), "")

    test("a correlated aggregate subquery prints as a scalar select"):
        assertEquals(
            sql(from(User).map(u => from(Post).filter(p => p.userId === u.id).map(_ => count()))),
            lines(
                "SELECT",
                "    (",
                "        SELECT",
                "            COUNT(*) AS \"c1\"",
                "        FROM",
                "            \"post\" AS \"t2\"",
                "        WHERE",
                "            \"t2\".\"user_id\" = \"t1\".\"id\"",
                "    ) AS \"c1\"",
                "FROM",
                "    \"user\" AS \"t1\""
            )
        )

    test("an outer column inside a limited subquery keeps its outer alias"):
        assertEquals(
            sql(from(User).map(u => from(Post).map(_ => count(u.id)).take(1))),
            lines(
                "SELECT",
                "    (",
                "        SELECT",
                "            COUNT(\"t1\".\"id\") AS \"c1\"",
                "        FROM",
                "            \"post\" AS \"t2\"",
                "        LIMIT 1",
                "    ) AS \"c1\"",
                "FROM",
                "    \"user\" AS \"t1\""
            )
        )