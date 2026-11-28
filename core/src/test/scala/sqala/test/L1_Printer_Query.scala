package sqala.test

import sqala.ast.expr.*
import sqala.ast.group.{SqlGroup, SqlGroupingItem}
import sqala.ast.limit.{SqlFetch, SqlFetchMode, SqlFetchUnit, SqlLimit}
import sqala.ast.quantifier.SqlQuantifier
import sqala.ast.statement.*
import sqala.ast.window.{SqlWindow, SqlWindowItem}

class L1_Printer_Query extends munit.FunSuite:
    import QueryFixture.*

    def lines(xs: String*): String = xs.mkString("\n")

    test("minimal select"):
        assertEquals(
            createSql(_.printQuery(simple)),
            lines("""SELECT""", """    "a"""", """FROM""", """    "t"""")
        )

    test("select without from"):
        assertEquals(
            createSql(_.printQuery(select(nel(item(num(1)))))),
            lines("SELECT", "    1")
        )

    test("select quantifier and item list"):
        val q = simple.copy(
            quantifier = Some(SqlQuantifier.Distinct),
            select = nel(item(col("a")), itemAs(col("b"), "x"), SqlSelectItem.Asterisk(Some("t")))
        )
        assertEquals(
            createSql(_.printQuery(q)),
            lines("SELECT DISTINCT", """    "a",""", """    "b" AS "x",""", """    "t".*""", "FROM", """    "t"""")
        )

    test("where group by and having"):
        val q = simple.copy(
            where = Some(gt(col("a"), 0)),
            groupBy = Some(SqlGroup(None, nel(SqlGroupingItem.Expr(col("a"))))),
            having = Some(gt(col("b"), 1))
        )
        assertEquals(
            createSql(_.printQuery(q)),
            lines(
                "SELECT", """    "a"""", "FROM", """    "t"""",
                "WHERE", """    "a" > 0""",
                "GROUP BY", """    "a"""",
                "HAVING", """    "b" > 1"""
            )
        )

    test("group by quantifier"):
        val q = simple.copy(
            groupBy = Some(SqlGroup(Some(SqlQuantifier.All), nel(SqlGroupingItem.Expr(col("a")))))
        )
        assertEquals(
            createSql(_.printQuery(q)),
            lines("SELECT", """    "a"""", "FROM", """    "t"""", "GROUP BY ALL", """    "a"""")
        )

    test("window and qualify clauses"):
        val q = simple.copy(
            window = List(SqlWindowItem("w", SqlWindow.Inlined(None, List(col("a")), Nil, None))),
            qualify = Some(gt(col("a"), 0))
        )
        assertEquals(
            createSql(_.printQuery(q)),
            lines(
                "SELECT", """    "a"""", "FROM", """    "t"""",
                "WINDOW", """    "w" AS (PARTITION BY "a")""",
                "QUALIFY", """    "a" > 0"""
            )
        )

    test("order by limit and lock"):
        val q = simple.copy(
            orderBy = List(ordDesc(col("a"))),
            limit = Some(SqlLimit(Some(num(5)), Some(SqlFetch(num(10), SqlFetchUnit.RowCount, SqlFetchMode.Only)))),
            lock = Some(SqlLock.Update(Some(SqlLockWaitMode.NoWait)))
        )
        assertEquals(
            createSql(_.printQuery(q)),
            lines(
                "SELECT", """    "a"""", "FROM", """    "t"""",
                "ORDER BY", """    "a" DESC""",
                "OFFSET 5 ROWS FETCH NEXT 10 ROWS ONLY",
                "FOR UPDATE NOWAIT"
            )
        )

    test("values query"):
        assertEquals(
            createSql(_.printQuery(SqlQuery.Values(nel(nel(num(1), num(2))), Nil, None))),
            "VALUES (1, 2)"
        )
        assertEquals(
            createSql(_.printQuery(SqlQuery.Values(
                nel(nel(num(1)), nel(num(2))),
                List(ord(col("a"))),
                Some(SqlLimit(Some(num(1)), None))
            ))),
            lines("VALUES (1), (2)", "ORDER BY", """    "a" ASC""", "OFFSET 1 ROWS")
        )

    test("set query"):
        val q = SqlQuery.Set(simple, SqlSetOperator.Union(None), None, simple, Nil, None)
        assertEquals(
            createSql(_.printQuery(q)),
            lines(
                "(",
                """    SELECT""",
                """        "a"""",
                """    FROM""",
                """        "t"""",
                ")",
                "UNION",
                "(",
                """    SELECT""",
                """        "a"""",
                """    FROM""",
                """        "t"""",
                ")"
            )
        )

    test("set operator and CORRESPONDING are separated by a space"):
        val q = SqlQuery.Set(
            simple,
            SqlSetOperator.Union(Some(SqlQuantifier.All)),
            Some(SqlSetCorresponding(List("a"))),
            simple,
            Nil,
            None
        )
        val printed = createSql(_.printQuery(q))
        assert(
            printed.contains("UNION ALL CORRESPONDING BY ("),
            s"set operator ran into CORRESPONDING:\n$printed"
        )

    test("CORRESPONDING without column list is separated by a space"):
        val q = SqlQuery.Set(simple, SqlSetOperator.Intersect(None), Some(SqlSetCorresponding(Nil)), simple, Nil, None)
        val printed = createSql(_.printQuery(q))
        assert(printed.contains("INTERSECT CORRESPONDING"), s"missing separating space:\n$printed")

    test("with query"):
        val q = SqlQuery.With(false, nel(SqlWithItem("c", Nil, simple)), simple)
        assertEquals(
            createSql(_.printQuery(q)),
            lines(
                "WITH",
                """"c" AS (""",
                """    SELECT""",
                """        "a"""",
                """    FROM""",
                """        "t"""",
                ")",
                "SELECT",
                """    "a"""",
                "FROM",
                """    "t""""
            )
        )

    test("recursive with query and column names"):
        val q = SqlQuery.With(true, nel(SqlWithItem("c", List("x", "y"), simple)), simple)
        val printed = createSql(_.printQuery(q))
        assert(printed.startsWith("WITH RECURSIVE\n"), printed)
        assert(printed.contains(""""c"("x", "y") AS ("""), printed)

    test("subquery expression is indented"):
        assertEquals(
            createSql(_.printExpr(SqlExpr.Subquery(simple))),
            lines("(", """    SELECT""", """        "a"""", """    FROM""", """        "t"""", ")")
        )

    test("exists predicate"):
        assertEquals(
            createSql(_.printExpr(SqlExpr.ExistsPredicate(simple))),
            lines("EXISTS(", """    SELECT""", """        "a"""", """    FROM""", """        "t"""", ")")
        )

    test("quantified comparison predicate"):
        assertEquals(
            createSql(_.printExpr(SqlExpr.QuantifiedComparisonPredicate(
                col("a"), SqlQuantifiedComparisonOperator.GreaterThan, SqlSubqueryQuantifier.All, simple
            ))),
            lines(""""a" > ALL(""", """    SELECT""", """        "a"""", """    FROM""", """        "t"""", ")")
        )

    test("in with subquery right operand"):
        assertEquals(
            createSql(_.printExpr(SqlExpr.In(col("a"), SqlInRightOperand.Subquery(simple), false))),
            lines(""""a" IN (""", "    SELECT", """        "a"""", "    FROM", """        "t"""", ")")
        )

    test("in with value list"):
        assertEquals(
            createSql(_.printExpr(SqlExpr.In(col("a"), SqlInRightOperand.Values(nel(num(1), num(2))), false))),
            """"a" IN (1, 2)"""
        )
        assertEquals(
            createSql(_.printExpr(SqlExpr.In(col("a"), SqlInRightOperand.Values(nel(num(1))), true))),
            """"a" NOT IN (1)"""
        )