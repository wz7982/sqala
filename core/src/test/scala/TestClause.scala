import sqala.ast.expr.SqlExpr
import sqala.ast.group.*
import sqala.ast.limit.*
import sqala.ast.order.*
import sqala.ast.quantifier.SqlQuantifier
import sqala.ast.token.SqlUnsafeCustomToken
import sqala.util.NonEmptyList

class TestClause extends munit.FunSuite:
    test("grouping item"):
        val col = (n: String) => SqlExpr.Column(None, n)
        val cases: List[(SqlGroupingItem, String)] = List(
            SqlGroupingItem.EmptyGroup -> "()",
            SqlGroupingItem.Expr(col("x")) -> """"x"""",
            SqlGroupingItem.Cube(NonEmptyList(col("x"), Nil)) -> """CUBE("x")""",
            SqlGroupingItem.Cube(NonEmptyList(col("x"), List(col("y")))) -> """CUBE("x", "y")""",
            SqlGroupingItem.Rollup(NonEmptyList(col("x"), Nil)) -> """ROLLUP("x")""",
            SqlGroupingItem.Rollup(NonEmptyList(col("x"), List(col("y")))) -> """ROLLUP("x", "y")""",
            SqlGroupingItem.GroupingSets(NonEmptyList(SqlGroupingItem.Expr(col("x")), Nil)) -> """GROUPING SETS("x")""",
            SqlGroupingItem.GroupingSets(NonEmptyList(SqlGroupingItem.Expr(col("x")), List(SqlGroupingItem.EmptyGroup))) -> """GROUPING SETS("x", ())""",
            SqlGroupingItem.GroupingSets(NonEmptyList(SqlGroupingItem.Cube(NonEmptyList(col("x"), List(col("y")))), List(SqlGroupingItem.Expr(col("z"))))) -> """GROUPING SETS(CUBE("x", "y"), "z")""",
        )
        for (item, sql) <- cases do
            assertEquals(createSql(_.printGroupingItem(item)), sql)

    test("group"):
        val col = (n: String) => SqlExpr.Column(None, n)
        val cases: List[(SqlGroup, String)] = List(
            SqlGroup(None, NonEmptyList(SqlGroupingItem.Expr(col("x")), Nil)) -> "GROUP BY\n    \"x\"",
            SqlGroup(Some(SqlQuantifier.Distinct), NonEmptyList(SqlGroupingItem.Expr(col("x")), Nil)) -> "GROUP BY DISTINCT\n    \"x\"",
            SqlGroup(None, NonEmptyList(SqlGroupingItem.Expr(col("x")), List(SqlGroupingItem.Expr(col("y"))))) -> "GROUP BY\n    \"x\",\n    \"y\"",
        )
        for (g, sql) <- cases do
            assertEquals(createSql(_.printGroup(g)), sql)

    test("fetch unit"):
        val cases: List[(SqlFetchUnit, String)] = List(
            SqlFetchUnit.RowCount -> "ROWS",
            SqlFetchUnit.Percentage -> "PERCENT ROWS",
        )
        for (u, sql) <- cases do
            assertEquals(createSql(_.printFetchUnit(u)), sql)

    test("fetch mode"):
        val cases: List[(SqlFetchMode, String)] = List(
            SqlFetchMode.Only -> "ONLY",
            SqlFetchMode.WithTies -> "WITH TIES",
        )
        for (m, sql) <- cases do
            assertEquals(createSql(_.printFetchMode(m)), sql)

    test("fetch"):
        val cases: List[(SqlFetch, String)] = List(
            SqlFetch(SqlExpr.NumberLiteral(5), SqlFetchUnit.RowCount, SqlFetchMode.Only) -> "FETCH NEXT 5 ROWS ONLY",
            SqlFetch(SqlExpr.NumberLiteral(5), SqlFetchUnit.Percentage, SqlFetchMode.WithTies) -> "FETCH NEXT 5 PERCENT ROWS WITH TIES",
        )
        for (f, sql) <- cases do
            assertEquals(createSql(_.printFetch(f)), sql)

    test("limit"):
        val offsetVariants: List[(Option[SqlExpr], String)] = List(
            None -> "",
            Some(SqlExpr.NumberLiteral(10)) -> "OFFSET 10 ROWS",
        )
        val fetchVariants: List[(Option[SqlFetch], String)] = List(
            None -> "",
            Some(SqlFetch(SqlExpr.NumberLiteral(5), SqlFetchUnit.RowCount, SqlFetchMode.Only)) -> "FETCH NEXT 5 ROWS ONLY",
            Some(SqlFetch(SqlExpr.NumberLiteral(5), SqlFetchUnit.Percentage, SqlFetchMode.WithTies)) -> "FETCH NEXT 5 PERCENT ROWS WITH TIES",
        )
        for
            (offset, oStr) <- offsetVariants
            (fetch, fStr) <- fetchVariants
        do
            val expr = SqlLimit(offset, fetch)
            val expected = oStr + (if oStr.nonEmpty && fStr.nonEmpty then " " else "") + fStr
            assertEquals(createSql(_.printLimit(expr)), expected)

    test("ordering"):
        val cases: List[(SqlOrdering, String)] = List(
            SqlOrdering.Asc -> "ASC",
            SqlOrdering.Desc -> "DESC",
        )
        for (o, sql) <- cases do
            assertEquals(createSql(_.printOrdering(o)), sql)

    test("nulls ordering"):
        val cases: List[(SqlNullsOrdering, String)] = List(
            SqlNullsOrdering.First -> "NULLS FIRST",
            SqlNullsOrdering.Last -> "NULLS LAST",
        )
        for (n, sql) <- cases do
            assertEquals(createSql(_.printNullsOrdering(n)), sql)

    test("ordering item"):
        val col = SqlExpr.Column(None, "x")
        val orderingVariants: List[(Option[SqlOrdering], String)] = List(
            None -> "ASC",
            Some(SqlOrdering.Asc) -> "ASC",
        )
        val nullsOrderingVariants: List[(Option[SqlNullsOrdering], String)] = List(
            None -> "",
            Some(SqlNullsOrdering.First) -> " NULLS FIRST",
            Some(SqlNullsOrdering.Last) -> " NULLS LAST",
        )
        for
            (ordering, oStr) <- orderingVariants
            (nullsOrdering, nStr) <- nullsOrderingVariants
        do
            val expr = SqlOrderingItem(col, ordering, nullsOrdering)
            val expected = s"""\"x\" $oStr$nStr"""
            assertEquals(createSql(_.printOrderingItem(expr)), expected)

    test("quantifier"):
        val cases: List[(SqlQuantifier, String)] = List(
            SqlQuantifier.All -> "ALL",
            SqlQuantifier.Distinct -> "DISTINCT",
        )
        for (q, sql) <- cases do
            assertEquals(createSql(_.printQuantifier(q)), sql)

    test("unsafe custom token"):
        val cases: List[(SqlUnsafeCustomToken, String)] = List(
            SqlUnsafeCustomToken.Keyword("SELECT") -> "SELECT",
            SqlUnsafeCustomToken.Expr(SqlExpr.Column(None, "x")) -> """"x"""",
        )
        for (t, sql) <- cases do
            assertEquals(createSql(_.printUnsafeCustomToken(t)), sql)