package sqala.test

import sqala.ast.expr.*
import sqala.ast.table.*
import sqala.test.Coverage.assertCoverage

class L1_Printer_Table extends munit.FunSuite:
    import QueryFixture.*

    def lines(xs: String*): String = xs.mkString("\n")

    test("table coverage"):
        assertCoverage[SqlTable]("Ident", "Func", "Subquery", "Json", "Graph", "Join")
        assertCoverage[SqlTablePeriodForMode]("SystemTimeAsOf", "SystemTimeBetween", "SystemTimeFrom")
        assertCoverage[SqlTablePeriodBetweenMode]("Asymmetric", "Symmetric")
        assertCoverage[SqlTableSampleMode]("Bernoulli", "System")

    test("ident table"):
        assertEquals(createSql(_.printTable(table("t"))), """"t"""")
        assertEquals(
            createSql(_.printTable(table("t").copy(alias = Some(SqlTableAlias("x", Nil))))),
            """"t" AS "x""""
        )
        assertEquals(
            createSql(_.printTable(table("t").copy(alias = Some(SqlTableAlias("x", List("a", "b")))))),
            """"t" AS "x"("a", "b")"""
        )

    test("temporal period modes"):
        assertEquals(
            createSql(_.printTablePeriodForMode(SqlTablePeriodForMode.SystemTimeAsOf(num(1)))),
            "FOR SYSTEM_TIME AS OF 1"
        )
        assertEquals(
            createSql(_.printTablePeriodForMode(SqlTablePeriodForMode.SystemTimeFrom(num(1), num(2)))),
            "FOR SYSTEM_TIME FROM 1 TO 2"
        )
        assertEquals(
            createSql(_.printTablePeriodForMode(SqlTablePeriodForMode.SystemTimeBetween(None, num(1), num(2)))),
            "FOR SYSTEM_TIME BETWEEN 1 AND 2"
        )
        assertEquals(
            createSql(_.printTablePeriodForMode(
                SqlTablePeriodForMode.SystemTimeBetween(Some(SqlTablePeriodBetweenMode.Symmetric), num(1), num(2))
            )),
            "FOR SYSTEM_TIME BETWEEN SYMMETRIC 1 AND 2"
        )

    test("temporal period parenthesizes boolean operands"):
        val and = SqlExpr.Binary(col("a"), SqlBinaryOperator.And, col("b"))
        assertEquals(
            createSql(_.printTablePeriodForMode(SqlTablePeriodForMode.SystemTimeBetween(None, and, num(2)))),
            """FOR SYSTEM_TIME BETWEEN ("a" AND "b") AND 2"""
        )

    test("tablesample"):
        assertEquals(
            createSql(_.printTableSample(SqlTableSample(SqlTableSampleMode.Bernoulli, num(10), None))),
            "TABLESAMPLE BERNOULLI (10)"
        )
        assertEquals(
            createSql(_.printTableSample(SqlTableSample(SqlTableSampleMode.System, num(10), Some(num(1))))),
            "TABLESAMPLE SYSTEM (10) REPEATABLE (1)"
        )

    test("ident table clause order"):
        val t = table("t").copy(
            periodForMode = Some(SqlTablePeriodForMode.SystemTimeAsOf(num(1))),
            alias = Some(SqlTableAlias("x", Nil)),
            sample = Some(SqlTableSample(SqlTableSampleMode.System, num(10), None))
        )
        assertEquals(
            createSql(_.printTable(t)),
            """"t" FOR SYSTEM_TIME AS OF 1 AS "x" TABLESAMPLE SYSTEM (10)"""
        )

    test("func table"):
        val base: SqlTable.Func = SqlTable.Func(false, "generate_series", List(num(1), num(3)), false, None, None)
        assertEquals(createSql(_.printTable(base)), "generate_series(1, 3)")
        assertEquals(createSql(_.printTable(base.copy(withLateral = true))), "LATERAL generate_series(1, 3)")
        assertEquals(createSql(_.printTable(base.copy(withOrdinality = true))), "generate_series(1, 3) WITH ORDINALITY")
        assertEquals(
            createSql(_.printTable(base.copy(
                withLateral = true,
                withOrdinality = true,
                alias = Some(SqlTableAlias("x", List("a")))
            ))),
            """LATERAL generate_series(1, 3) WITH ORDINALITY AS "x"("a")"""
        )

    test("subquery table"):
        val base: SqlTable.Subquery = SqlTable.Subquery(false, simple, None, None)
        assertEquals(
            createSql(_.printTable(base)),
            lines("(", """    SELECT""", """        "a"""", """    FROM""", """        "t"""", ")")
        )
        assertEquals(
            createSql(_.printTable(base.copy(withLateral = true, alias = Some(SqlTableAlias("x", Nil))))),
            lines("LATERAL (", """    SELECT""", """        "a"""", """    FROM""", """        "t"""", """) AS "x"""")
        )

    test("join table"):
        val cond = SqlJoinCondition.On(eqTo(qcol("a", "x"), qcol("b", "x")))
        assertEquals(
            createSql(_.printTable(SqlTable.Join(table("a"), SqlJoinType.Inner, table("b"), Some(cond)))),
            lines(""""a"""", """INNER JOIN "b" ON "a"."x" = "b"."x"""")
        )
        assertEquals(
            createSql(_.printTable(SqlTable.Join(table("a"), SqlJoinType.Cross, table("b"), None))),
            lines(""""a"""", """CROSS JOIN "b"""")
        )
        assertEquals(
            createSql(_.printTable(SqlTable.Join(
                table("a"), SqlJoinType.Left, table("b"), Some(SqlJoinCondition.Using(nel("x")))
            ))),
            lines(""""a"""", """LEFT OUTER JOIN "b" USING ("x")""")
        )

    test("nested join wraps the right side"):
        val inner = SqlTable.Join(
            table("b"),
            SqlJoinType.Inner,
            table("c"),
            Some(SqlJoinCondition.On(eqTo(qcol("b", "x"), qcol("c", "x"))))
        )
        assertEquals(
            createSql(_.printTable(SqlTable.Join(table("a"), SqlJoinType.Inner, inner, None))),
            lines(
                """"a"""",
                "INNER JOIN (",
                """    "b"""",
                """    INNER JOIN "c" ON "b"."x" = "c"."x"""",
                ")"
            )
        )