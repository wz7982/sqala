package sqala.test

import sqala.ast.expr.SqlBinaryOperator
import sqala.ast.limit.{SqlFetchMode, SqlFetchUnit}
import sqala.ast.order.{SqlNullsOrdering, SqlOrdering}
import sqala.ast.quantifier.SqlQuantifier
import sqala.ast.statement.SqlSetOperator
import sqala.ast.table.SqlJoinType
import sqala.ir.*
import sqala.test.IrFixture.*
import sqala.util.NonEmptyList

/**
 * Query lowering: select clauses, grouping forms, locks, builder
 * extensions, set operations, VALUES, and CTE naming and resolution.
 */
class I2_Query extends munit.FunSuite:
    test("select renders qualified columns from its from table"):
        val user = IrTable.Ident("user", Nil)
        assertEquals(
            pg(sel(List(IrSelectItem(col(user, "id"), "id")), Some(user))),
            lines(
                "SELECT",
                """    "t1"."id" AS "id"""",
                "FROM",
                """    "user" AS "t1""""
            )
        )

    test("distinct quantifier and where clause"):
        val user = IrTable.Ident("user", Nil)
        assertEquals(
            pg(IrQuery.Select(Some(SqlQuantifier.Distinct), NonEmptyList(IrSelectItem(col("a"), "a"), Nil), Some(user), Some(IrExpr.Binary(col("a"), SqlBinaryOperator.Equal, n(1))), None, None, Nil, None, None)),
            lines(
                "SELECT DISTINCT",
                """    "a" AS "a"""",
                "FROM",
                """    "user" AS "t1"""",
                "WHERE",
                """    "a" = 1"""
            )
        )

    test("group by and having"):
        val user = IrTable.Ident("user", Nil)
        assertEquals(
            pg(IrQuery.Select(None, NonEmptyList(IrSelectItem(col("a"), "a"), List(IrSelectItem(IrExpr.CountAsteriskFunc(None), "c"))), Some(user), None, Some(IrGroup(None, NonEmptyList(IrGroupingItem.Expr(col("a")), Nil))), Some(IrExpr.Binary(IrExpr.CountAsteriskFunc(None), SqlBinaryOperator.GreaterThan, n(2))), Nil, None, None)),
            lines(
                "SELECT",
                """    "a" AS "a",""",
                """    COUNT(*) AS "c"""",
                "FROM",
                """    "user" AS "t1"""",
                "GROUP BY",
                """    "a"""",
                "HAVING",
                "    COUNT(*) > 2"
            )
        )

    test("group by distinct with cube and rollup items"):
        val user = IrTable.Ident("user", Nil)
        assertEquals(
            pg(IrQuery.Select(None, NonEmptyList(IrSelectItem(col("a"), "a"), Nil), Some(user), None, Some(IrGroup(Some(SqlQuantifier.Distinct), NonEmptyList(IrGroupingItem.Cube(NonEmptyList(col("a"), List(col("b")))), List(IrGroupingItem.Rollup(NonEmptyList(col("c"), Nil)))))), None, Nil, None, None)),
            lines(
                "SELECT",
                """    "a" AS "a"""",
                "FROM",
                """    "user" AS "t1"""",
                "GROUP BY DISTINCT",
                """    CUBE("a", "b"),""",
                """    ROLLUP("c")"""
            )
        )

    test("grouping sets include the empty group"):
        val user = IrTable.Ident("user", Nil)
        assertEquals(
            pg(IrQuery.Select(None, NonEmptyList(IrSelectItem(col("a"), "a"), Nil), Some(user), None, Some(IrGroup(None, NonEmptyList(IrGroupingItem.GroupingSets(NonEmptyList(IrGroupingItem.Expr(col("a")), List(IrGroupingItem.EmptyGroup))), Nil))), None, Nil, None, None)),
            lines(
                "SELECT",
                """    "a" AS "a"""",
                "FROM",
                """    "user" AS "t1"""",
                "GROUP BY",
                """    GROUPING SETS("a", ())"""
            )
        )

    test("order by with nulls ordering and offset fetch with ties"):
        val user = IrTable.Ident("user", Nil)
        assertEquals(
            pg(IrQuery.Select(None, NonEmptyList(IrSelectItem(col("a"), "a"), Nil), Some(user), None, None, None, List(IrOrderingItem(col("a"), SqlOrdering.Asc, None), IrOrderingItem(col("b"), SqlOrdering.Desc, Some(SqlNullsOrdering.Last))), Some(IrLimit(Some(n(10)), Some(IrFetch(n(5), SqlFetchUnit.RowCount, SqlFetchMode.WithTies)))), None)),
            lines(
                "SELECT",
                """    "a" AS "a"""",
                "FROM",
                """    "user" AS "t1"""",
                "ORDER BY",
                """    "a" ASC,""",
                """    "b" DESC NULLS LAST""",
                "OFFSET 10 ROWS FETCH NEXT 5 ROWS WITH TIES"
            )
        )

    test("row locks render wait modes"):
        val user = IrTable.Ident("user", Nil)
        def base(lock: IrLock) =
            pg(IrQuery.Select(None, NonEmptyList(IrSelectItem(col("a"), "a"), Nil), Some(user), None, None, None, Nil, None, Some(lock)))
        val header = lines(
            "SELECT",
            """    "a" AS "a"""",
            "FROM",
            """    "user" AS "t1""""
        )
        assertEquals(base(IrLock.Update(Some(IrLockWaitMode.NoWait))), header + "\nFOR UPDATE NOWAIT")
        assertEquals(base(IrLock.Share(Some(IrLockWaitMode.SkipLocked))), header + "\nFOR SHARE SKIP LOCKED")
        assertEquals(base(IrLock.Update(None)), header + "\nFOR UPDATE")

    test("select builder extensions accumulate items and conditions"):
        val user = IrTable.Ident("user", Nil)
        assertEquals(
            pg(sel(List(IrSelectItem(col("a"), "a")), Some(user))
                .addSelectItem(IrSelectItem(col("b"), "b"))
                .addWhere(col("c"))
                .addWhere(col("d"))
                .addHaving(col("e"))
                .addHaving(col("f"))
            ),
            lines(
                "SELECT",
                """    "a" AS "a",""",
                """    "b" AS "b"""",
                "FROM",
                """    "user" AS "t1"""",
                "WHERE",
                """    "c" AND "d"""",
                "HAVING",
                """    "e" AND "f""""
            )
        )

    test("set operations render parenthesized operands with trailing clauses"):
        assertEquals(
            pg(IrQuery.Set(
                sel(List(IrSelectItem(n(1), "v"))),
                SqlSetOperator.Union(None),
                sel(List(IrSelectItem(n(2), "v"))),
                List(IrOrderingItem(col("v"), SqlOrdering.Asc, None)),
                Some(IrLimit(None, Some(IrFetch(n(1), SqlFetchUnit.RowCount, SqlFetchMode.Only))))
            )),
            lines(
                "(",
                "    SELECT",
                "        1 AS \"v\"",
                ")",
                "UNION",
                "(",
                "    SELECT",
                "        2 AS \"v\"",
                ")",
                "ORDER BY",
                """    "v" ASC""",
                "LIMIT 1"
            )
        )
        assertEquals(
            pg(IrQuery.Set(
                sel(List(IrSelectItem(n(1), "v"))),
                SqlSetOperator.Except(Some(SqlQuantifier.All)),
                sel(List(IrSelectItem(n(2), "v"))),
                Nil,
                None
            )),
            lines(
                "(",
                "    SELECT",
                "        1 AS \"v\"",
                ")",
                "EXCEPT ALL",
                "(",
                "    SELECT",
                "        2 AS \"v\"",
                ")"
            )
        )

    test("values query with order by"):
        assertEquals(
            pg(IrQuery.Values(
                NonEmptyList(NonEmptyList(n(1), List(n(2))), List(NonEmptyList(n(3), List(n(4))))),
                List(IrOrderingItem(col("c1"), SqlOrdering.Asc, None)),
                None
            )),
            lines("VALUES (1, 2), (3, 4)", "ORDER BY", """    "c1" ASC""")
        )

    test("with items get sequential cte names and are referenced through cte tables"):
        val cteB = IrTable.Cte(1, Nil)
        assertEquals(
            pg(IrQuery.With(false, NonEmptyList(IrWithItem(List("c1", "c2"), sel(List(IrSelectItem(n(1), "v")))), List(IrWithItem(Nil, sel(List(IrSelectItem(n(2), "v")))))), sel(List(IrSelectItem(col(cteB, "v"), "x")), Some(cteB)))),
            lines(
                "WITH",
                "\"cte1\"(\"c1\", \"c2\") AS (",
                "    SELECT",
                "        1 AS \"v\"",
                "),",
                "\"cte2\" AS (",
                "    SELECT",
                "        2 AS \"v\"",
                ")",
                "SELECT",
                """    "t1"."v" AS "x"""",
                "FROM",
                """    "cte2" AS "t1""""
            )
        )

    test("recursive with carries column aliases of the cte reference"):
        val cteZ = IrTable.Cte(0, List("z"))
        assertEquals(
            pg(IrQuery.With(true, NonEmptyList(IrWithItem(Nil, sel(List(IrSelectItem(n(1), "v")))), Nil), sel(List(IrSelectItem(col(cteZ, "v"), "x")), Some(cteZ)))),
            lines(
                "WITH RECURSIVE",
                "\"cte1\" AS (",
                "    SELECT",
                "        1 AS \"v\"",
                ")",
                "SELECT",
                """    "t1"."v" AS "x"""",
                "FROM",
                """    "cte1" AS "t1"("z")"""
            )
        )

    test("nested with pops the cte name stack after the inner query"):
        val innerCte = IrTable.Cte(0, Nil)
        val cteA = IrTable.Cte(0, Nil)
        assertEquals(
            pg(IrQuery.With(false, NonEmptyList(IrWithItem(Nil, IrQuery.With(false, NonEmptyList(IrWithItem(Nil, sel(List(IrSelectItem(n(1), "v")))), Nil), sel(List(IrSelectItem(col(innerCte, "v"), "w")), Some(innerCte)))), Nil), sel(List(IrSelectItem(col(cteA, "v"), "x")), Some(cteA)))),
            lines(
                "WITH",
                "\"cte1\" AS (",
                "    WITH",
                "    \"cte2\" AS (",
                "        SELECT",
                "            1 AS \"v\"",
                "    )",
                "    SELECT",
                "        \"t1\".\"v\" AS \"w\"",
                "    FROM",
                "        \"cte2\" AS \"t1\"",
                ")",
                "SELECT",
                """    "t2"."v" AS "x"""",
                "FROM",
                """    "cte1" AS "t2""""
            )
        )

    test("multiple cte references inside one join keep distinct aliases"):
        val cteA = IrTable.Cte(0, Nil)
        val cteB = IrTable.Cte(1, Nil)
        val joinQuery = IrQuery.With(
            false,
            NonEmptyList(
                IrWithItem(Nil, sel(List(IrSelectItem(n(1), "v")))),
                List(IrWithItem(Nil, sel(List(IrSelectItem(n(2), "v")))))
            ),
            sel(
                List(IrSelectItem(col(cteA, "v"), "a"), IrSelectItem(col(cteB, "v"), "b")),
                Some(IrTable.Join(cteA, SqlJoinType.Inner, cteB, Some(IrExpr.Binary(col(cteA, "v"), SqlBinaryOperator.Equal, col(cteB, "v")))))
            )
        )
        assertEquals(
            pg(joinQuery),
            lines(
                "WITH",
                "\"cte1\" AS (",
                "    SELECT",
                "        1 AS \"v\"",
                "),",
                "\"cte2\" AS (",
                "    SELECT",
                "        2 AS \"v\"",
                ")",
                "SELECT",
                """    "t1"."v" AS "a",""",
                """    "t2"."v" AS "b"""",
                "FROM",
                """    "cte1" AS "t1"""",
                """    INNER JOIN "cte2" AS "t2" ON "t1"."v" = "t2"."v""""
            )
        )

    test("join column references share aliases with the joined tables"):
        val user = IrTable.Ident("user", Nil)
        val post = IrTable.Ident("post", Nil)
        val t = IrTable.Join(user, SqlJoinType.Inner, post, Some(IrExpr.Binary(col(user, "id"), SqlBinaryOperator.Equal, col(post, "user_id"))))
        assertEquals(
            pg(sel(List(IrSelectItem(col(user, "id"), "id"), IrSelectItem(col(post, "id"), "pid")), Some(t))),
            lines(
                "SELECT",
                """    "t1"."id" AS "id",""",
                """    "t2"."id" AS "pid"""",
                "FROM",
                """    "user" AS "t1"""",
                """    INNER JOIN "post" AS "t2" ON "t1"."id" = "t2"."user_id""""
            )
        )

    test("distinct table instances with the same name get distinct aliases"):
        assertEquals(
            pg(sel(List(IrSelectItem(col(IrTable.Ident("user", Nil), "id"), "a"), IrSelectItem(col(IrTable.Ident("user", Nil), "id"), "b")), None)),
            lines(
                "SELECT",
                """    "t1"."id" AS "a",""",
                """    "t2"."id" AS "b""""
            )
        )

    test("derived tables render with lateral and column aliases"):
        assertEquals(
            pg(sel(List(IrSelectItem(n(1), "v")), Some(IrTable.Subquery(false, sel(List(IrSelectItem(n(2), "w"))), Nil)))),
            lines(
                "SELECT",
                "    1 AS \"v\"",
                "FROM",
                "    (",
                "        SELECT",
                "            2 AS \"w\"",
                """    ) AS "t1""""
            )
        )
        assertEquals(
            pg(sel(List(IrSelectItem(n(1), "v")), Some(IrTable.Subquery(true, sel(List(IrSelectItem(n(2), "w"))), List("c"))))),
            lines(
                "SELECT",
                "    1 AS \"v\"",
                "FROM",
                "    LATERAL (",
                "        SELECT",
                "            2 AS \"w\"",
                """    ) AS "t1"("c")""",
            )
        )
