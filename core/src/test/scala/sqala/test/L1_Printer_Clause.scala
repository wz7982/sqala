package sqala.test

import sqala.ast.expr.SqlExpr
import sqala.ast.group.SqlGroupingItem
import sqala.ast.limit.{SqlFetch, SqlFetchMode, SqlFetchUnit, SqlLimit}
import sqala.ast.order.{SqlNullsOrdering, SqlOrdering, SqlOrderingItem}
import sqala.ast.statement.{SqlLock, SqlLockWaitMode}
import sqala.ast.table.{SqlJoinCondition, SqlJoinType}
import sqala.ast.token.SqlUnsafeCustomToken
import sqala.test.Coverage.assertCoverage
import sqala.util.NonEmptyList

object ClauseFixture:
    def col(name: String): SqlExpr = SqlExpr.Column(None, name)

    def num(n: Int): SqlExpr = SqlExpr.NumberLiteral(n)

    def nel[A](head: A, tail: A*): NonEmptyList[A] = NonEmptyList(head, tail.toList)

    val orderings: Map[String, (SqlOrdering, String)] = Map(
        "Asc" -> (SqlOrdering.Asc, "ASC"),
        "Desc" -> (SqlOrdering.Desc, "DESC")
    )

    val nullsOrderings: Map[String, (SqlNullsOrdering, String)] = Map(
        "First" -> (SqlNullsOrdering.First, "NULLS FIRST"),
        "Last" -> (SqlNullsOrdering.Last, "NULLS LAST")
    )

    val fetchUnits: Map[String, (SqlFetchUnit, String)] = Map(
        "RowCount" -> (SqlFetchUnit.RowCount, "ROWS"),
        "Percentage" -> (SqlFetchUnit.Percentage, "PERCENT ROWS")
    )

    val fetchModes: Map[String, (SqlFetchMode, String)] = Map(
        "Only" -> (SqlFetchMode.Only, "ONLY"),
        "WithTies" -> (SqlFetchMode.WithTies, "WITH TIES")
    )

    val joinTypes: Map[String, (SqlJoinType, String)] = Map(
        "Inner" -> (SqlJoinType.Inner, "INNER JOIN"),
        "Left" -> (SqlJoinType.Left, "LEFT OUTER JOIN"),
        "Right" -> (SqlJoinType.Right, "RIGHT OUTER JOIN"),
        "Full" -> (SqlJoinType.Full, "FULL OUTER JOIN"),
        "Cross" -> (SqlJoinType.Cross, "CROSS JOIN")
    )

class L1_Printer_Clause extends munit.FunSuite:
    import ClauseFixture.*

    test("ordering tokens"):
        assertCoverage[SqlOrdering]("Asc", "Desc")
        assertEquals(orderings.keySet, Coverage.casesOf[SqlOrdering])

        for (name, (o, expected)) <- orderings.toList.sortBy(_._1) do
            assertEquals(createSql(_.printOrdering(o)), expected, name)

    test("nulls ordering tokens"):
        assertCoverage[SqlNullsOrdering]("First", "Last")
        assertEquals(nullsOrderings.keySet, Coverage.casesOf[SqlNullsOrdering])

        for (name, (o, expected)) <- nullsOrderings.toList.sortBy(_._1) do
            assertEquals(createSql(_.printNullsOrdering(o)), expected, name)

    test("ordering item defaults to ASC"):
        assertEquals(createSql(_.printOrderingItem(SqlOrderingItem(col("a"), None, None))), """"a" ASC""")
        assertEquals(
            createSql(_.printOrderingItem(SqlOrderingItem(col("a"), Some(SqlOrdering.Asc), None))),
            """"a" ASC"""
        )
        assertEquals(
            createSql(_.printOrderingItem(SqlOrderingItem(col("a"), Some(SqlOrdering.Desc), None))),
            """"a" DESC"""
        )

    test("ordering item nulls suffix"):
        assertEquals(
            createSql(_.printOrderingItem(
                SqlOrderingItem(col("a"), Some(SqlOrdering.Desc), Some(SqlNullsOrdering.Last))
            )),
            """"a" DESC NULLS LAST"""
        )
        assertEquals(
            createSql(_.printOrderingItem(SqlOrderingItem(col("a"), None, Some(SqlNullsOrdering.First)))),
            """"a" ASC NULLS FIRST"""
        )

    test("grouping item tokens"):
        assertCoverage[SqlGroupingItem]("EmptyGroup", "Expr", "Cube", "Rollup", "GroupingSets")

        assertEquals(createSql(_.printGroupingItem(SqlGroupingItem.EmptyGroup)), "()")
        assertEquals(createSql(_.printGroupingItem(SqlGroupingItem.Expr(col("a")))), """"a"""")
        assertEquals(createSql(_.printGroupingItem(SqlGroupingItem.Cube(nel(col("a"))))), """CUBE("a")""")
        assertEquals(
            createSql(_.printGroupingItem(SqlGroupingItem.Cube(nel(col("a"), col("b"))))),
            """CUBE("a", "b")"""
        )
        assertEquals(
            createSql(_.printGroupingItem(SqlGroupingItem.Rollup(nel(col("a"), col("b"))))),
            """ROLLUP("a", "b")"""
        )

    test("grouping sets nest grouping items"):
        val sets = SqlGroupingItem.GroupingSets(nel(
            SqlGroupingItem.EmptyGroup,
            SqlGroupingItem.Expr(col("a")),
            SqlGroupingItem.Cube(nel(col("b")))
        ))
        assertEquals(createSql(_.printGroupingItem(sets)), """GROUPING SETS((), "a", CUBE("b"))""")

    test("fetch unit tokens"):
        assertCoverage[SqlFetchUnit]("RowCount", "Percentage")
        assertEquals(fetchUnits.keySet, Coverage.casesOf[SqlFetchUnit])

        for (name, (u, expected)) <- fetchUnits.toList.sortBy(_._1) do
            assertEquals(createSql(_.printFetchUnit(u)), expected, name)

    test("fetch mode tokens"):
        assertCoverage[SqlFetchMode]("Only", "WithTies")
        assertEquals(fetchModes.keySet, Coverage.casesOf[SqlFetchMode])

        for (name, (m, expected)) <- fetchModes.toList.sortBy(_._1) do
            assertEquals(createSql(_.printFetchMode(m)), expected, name)

    test("fetch clause"):
        assertEquals(
            createSql(_.printFetch(SqlFetch(num(10), SqlFetchUnit.RowCount, SqlFetchMode.Only))),
            "FETCH NEXT 10 ROWS ONLY"
        )
        assertEquals(
            createSql(_.printFetch(SqlFetch(num(10), SqlFetchUnit.Percentage, SqlFetchMode.WithTies))),
            "FETCH NEXT 10 PERCENT ROWS WITH TIES"
        )

    test("limit clause combinations"):
        assertEquals(createSql(_.printLimit(SqlLimit(None, None))), "")
        assertEquals(createSql(_.printLimit(SqlLimit(Some(num(5)), None))), "OFFSET 5 ROWS")
        assertEquals(
            createSql(_.printLimit(SqlLimit(None, Some(SqlFetch(num(10), SqlFetchUnit.RowCount, SqlFetchMode.Only))))),
            "FETCH NEXT 10 ROWS ONLY"
        )
        assertEquals(
            createSql(_.printLimit(SqlLimit(
                Some(num(5)),
                Some(SqlFetch(num(10), SqlFetchUnit.RowCount, SqlFetchMode.Only))
            ))),
            "OFFSET 5 ROWS FETCH NEXT 10 ROWS ONLY"
        )

    test("lock wait mode tokens"):
        assertCoverage[SqlLockWaitMode]("NoWait", "SkipLocked", "Wait")

        assertEquals(createSql(_.printLockWaitMode(SqlLockWaitMode.NoWait)), "NOWAIT")
        assertEquals(createSql(_.printLockWaitMode(SqlLockWaitMode.SkipLocked)), "SKIP LOCKED")
        assertEquals(createSql(_.printLockWaitMode(SqlLockWaitMode.Wait(num(5)))), "WAIT 5")

    test("lock clause"):
        assertCoverage[SqlLock]("Update", "Share")

        assertEquals(createSql(_.printLock(SqlLock.Update(None))), "FOR UPDATE")
        assertEquals(createSql(_.printLock(SqlLock.Share(None))), "FOR SHARE")
        assertEquals(createSql(_.printLock(SqlLock.Update(Some(SqlLockWaitMode.NoWait)))), "FOR UPDATE NOWAIT")
        assertEquals(createSql(_.printLock(SqlLock.Share(Some(SqlLockWaitMode.SkipLocked)))), "FOR SHARE SKIP LOCKED")
        assertEquals(createSql(_.printLock(SqlLock.Update(Some(SqlLockWaitMode.Wait(num(3)))))), "FOR UPDATE WAIT 3")

    test("join type tokens"):
        assertCoverage[SqlJoinType]("Inner", "Left", "Right", "Full", "Cross", "UnsafeCustom")
        assertEquals(joinTypes.keySet + "UnsafeCustom", Coverage.casesOf[SqlJoinType])

        for (name, (t, expected)) <- joinTypes.toList.sortBy(_._1) do
            assertEquals(createSql(_.printJoinType(t)), expected, name)

    test("join type UnsafeCustom is space-joined"):
        assertEquals(
            createSql(_.printJoinType(SqlJoinType.UnsafeCustom(List(SqlUnsafeCustomToken.Keyword("STRAIGHT_JOIN"))))),
            "STRAIGHT_JOIN"
        )

    test("join condition"):
        assertCoverage[SqlJoinCondition]("On", "Using")

        assertEquals(createSql(_.printJoinCondition(SqlJoinCondition.On(col("a")))), """ON "a"""")
        assertEquals(createSql(_.printJoinCondition(SqlJoinCondition.Using(nel("a")))), """USING ("a")""")
        assertEquals(createSql(_.printJoinCondition(SqlJoinCondition.Using(nel("a", "b")))), """USING ("a", "b")""")