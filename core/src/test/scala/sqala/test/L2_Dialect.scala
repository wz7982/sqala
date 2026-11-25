package sqala.test

import sqala.ast.expr.*
import sqala.ast.limit.{SqlFetch, SqlFetchMode, SqlFetchUnit, SqlLimit}
import sqala.ast.order.{SqlNullsOrdering, SqlOrdering, SqlOrderingItem}
import sqala.ast.statement.*
import sqala.printer.*

object DialectFixture:
    def render(p: StandardSqlPrinter)(f: StandardSqlPrinter => Unit): String =
        f(p)
        p.sql

    def mysql(f: StandardSqlPrinter => Unit): String = render(new MysqlPrinter(true))(f)
    def postgres(f: StandardSqlPrinter => Unit): String = render(new PostgresqlPrinter(true))(f)
    def sqlite(f: StandardSqlPrinter => Unit): String = render(new SqlitePrinter(true))(f)
    def oracle(f: StandardSqlPrinter => Unit): String = render(new OraclePrinter(true))(f)
    def sqlserver(f: StandardSqlPrinter => Unit): String = render(new SqlserverPrinter(true))(f)
    def h2(f: StandardSqlPrinter => Unit): String = render(new H2Printer(true))(f)

    val rowOnly: SqlLimit = SqlLimit(Some(QueryFixture.num(5)), Some(SqlFetch(QueryFixture.num(10), SqlFetchUnit.RowCount, SqlFetchMode.Only)))
    val offsetOnly: SqlLimit = SqlLimit(Some(QueryFixture.num(5)), None)
    val withTies: SqlLimit = SqlLimit(Some(QueryFixture.num(5)), Some(SqlFetch(QueryFixture.num(10), SqlFetchUnit.RowCount, SqlFetchMode.WithTies)))
    val percent: SqlLimit = SqlLimit(None, Some(SqlFetch(QueryFixture.num(10), SqlFetchUnit.Percentage, SqlFetchMode.Only)))

    def upsert: SqlStatement.Upsert =
        SqlStatement.Upsert(
            QueryFixture.table("t"),
            QueryFixture.nel("a", "b"),
            QueryFixture.nel(QueryFixture.num(1), QueryFixture.num(2)),
            QueryFixture.nel("a"),
            QueryFixture.nel("b")
        )

    def listAgg: SqlExpr.ListAggFunc =
        SqlExpr.ListAggFunc(None, QueryFixture.col("a"), QueryFixture.str(","), None, Nil, None)

class L2_Dialect extends munit.FunSuite:
    import DialectFixture.*
    import QueryFixture.{col, num, nel, table}

    test("identifier quoting per dialect"):
        assertEquals(mysql(_.printIdent("a")), "`a`")
        assertEquals(sqlserver(_.printIdent("a")), "[a]")
        assertEquals(postgres(_.printIdent("a")), """"a"""")
        assertEquals(oracle(_.printIdent("a")), """"a"""")
        assertEquals(sqlite(_.printIdent("a")), """"a"""")
        assertEquals(h2(_.printIdent("a")), """"a"""")

    test("identifier quote escaping per dialect"):
        assertEquals(mysql(_.printIdent("a`b")), "`a``b`")
        assertEquals(sqlserver(_.printIdent("a]b")), "[a]]b]")
        assertEquals(postgres(_.printIdent("a\"b")), """"a""b"""")

    test("mysql limit"):
        assertEquals(mysql(_.printLimit(rowOnly)), "LIMIT 5, 10")
        assertEquals(mysql(_.printLimit(offsetOnly)), "LIMIT 5, 9223372036854775807")
        assertEquals(mysql(_.printLimit(withTies)), "OFFSET 5 ROWS FETCH NEXT 10 ROWS WITH TIES")
        assertEquals(mysql(_.printLimit(percent)), "FETCH NEXT 10 PERCENT ROWS ONLY")

    test("postgresql limit"):
        assertEquals(postgres(_.printLimit(rowOnly)), "LIMIT 10 OFFSET 5")
        assertEquals(postgres(_.printLimit(offsetOnly)), "OFFSET 5")
        assertEquals(postgres(_.printLimit(SqlLimit(None, Some(SqlFetch(num(10), SqlFetchUnit.RowCount, SqlFetchMode.Only))))), "LIMIT 10")
        assertEquals(postgres(_.printLimit(withTies)), "OFFSET 5 ROWS FETCH NEXT 10 ROWS WITH TIES")

    test("sqlite limit"):
        assertEquals(sqlite(_.printLimit(rowOnly)), "LIMIT 10 OFFSET 5")
        assertEquals(sqlite(_.printLimit(offsetOnly)), "LIMIT 9223372036854775807 OFFSET 5")
        assertEquals(sqlite(_.printLimit(withTies)), "OFFSET 5 ROWS FETCH NEXT 10 ROWS WITH TIES")

    test("upsert per dialect"):
        assertEquals(
            postgres(_.printUpsertStatement(upsert)),
            """INSERT INTO "t" ("a", "b") VALUES (1, 2) ON CONFLICT ("a") DO UPDATE SET "b" = EXCLUDED."b""""
        )
        assertEquals(
            mysql(_.printUpsertStatement(upsert)),
            "INSERT INTO `t` (`a`, `b`) VALUES (1, 2) ON DUPLICATE KEY UPDATE `b` = VALUES (`b`)"
        )
        assertEquals(
            sqlite(_.printUpsertStatement(upsert)),
            """INSERT OR REPLACE INTO "t" ("a", "b") VALUES (1, 2)"""
        )

    test("mysql rewrites concat and null-safe equality"):
        assertEquals(
            mysql(_.printExpr(SqlExpr.Binary(col("a"), SqlBinaryOperator.Concat, col("b")))),
            """CONCAT(`a`, `b`)"""
        )
        assertEquals(
            mysql(_.printExpr(SqlExpr.Binary(col("a"), SqlBinaryOperator.IsDistinctFrom(true), col("b")))),
            "`a` <=> `b`"
        )
        assertEquals(
            mysql(_.printExpr(SqlExpr.Binary(col("a"), SqlBinaryOperator.IsDistinctFrom(false), col("b")))),
            "NOT(`a` <=> `b`)"
        )

    test("mysql values query uses ROW"):
        assertEquals(
            mysql(_.printValuesQuery(SqlQuery.Values(nel(nel(num(1), num(2))), Nil, None))),
            "VALUES ROW(1, 2)"
        )

    test("mysql values query keeps ORDER BY and limit"):
        val q: SqlQuery.Values = SqlQuery.Values(
            nel(nel(num(1))),
            List(SqlOrderingItem(col("a"), Some(SqlOrdering.Desc), None)),
            Some(rowOnly)
        )
        val printed = mysql(_.printValuesQuery(q))
        assert(printed.contains("ORDER BY"), s"ORDER BY was dropped: $printed")
        assert(printed.contains("LIMIT"), s"limit was dropped: $printed")

    test("type mapping per dialect"):
        assertEquals(mysql(_.printType(SqlType.Varchar(Some(10)))), "CHAR(10)")
        assertEquals(mysql(_.printType(SqlType.Int)), "SIGNED")
        assertEquals(mysql(_.printType(SqlType.Long)), "SIGNED")
        assertEquals(mysql(_.printType(SqlType.Timestamp(None))), "DATETIME")
        assertEquals(mysql(_.printType(SqlType.Timestamp(Some(SqlTimeZoneMode.Without)))), "DATETIME")

        assertEquals(sqlserver(_.printType(SqlType.Varchar(None))), "NVARCHAR(MAX)")
        assertEquals(sqlserver(_.printType(SqlType.Varchar(Some(10)))), "NVARCHAR(10)")
        assertEquals(sqlserver(_.printType(SqlType.Timestamp(None))), "DATETIME2")
        assertEquals(sqlserver(_.printType(SqlType.Timestamp(Some(SqlTimeZoneMode.With)))), "DATETIMEOFFSET")

        assertEquals(oracle(_.printType(SqlType.Varchar(None))), "VARCHAR(4000)")
        assertEquals(oracle(_.printType(SqlType.Long)), "INT")

        assertEquals(sqlite(_.printType(SqlType.Decimal(Some((10, 2))))), "REAL")
        assertEquals(sqlite(_.printType(SqlType.Decimal(None))), "REAL")

    test("listagg mapping per dialect"):
        assertEquals(postgres(_.printExpr(listAgg)), """STRING_AGG("a", ',')""")
        assertEquals(sqlite(_.printExpr(listAgg)), """GROUP_CONCAT("a", ',')""")
        assertEquals(sqlserver(_.printExpr(listAgg)), """STRING_AGG([a], N',')""")
        assertEquals(mysql(_.printExpr(listAgg)), "GROUP_CONCAT(`a` SEPARATOR ',')")

    test("listagg with within group per dialect"):
        val q = listAgg.copy(withinGroup = List(SqlOrderingItem(col("b"), None, None)))
        assertEquals(postgres(_.printExpr(q)), """STRING_AGG("a", ',' ORDER BY "b" ASC)""")
        assertEquals(sqlserver(_.printExpr(q)), """STRING_AGG([a], N',') WITHIN GROUP (ORDER BY [b] ASC)""")
        assertEquals(mysql(_.printExpr(q)), "GROUP_CONCAT(`a` ORDER BY `b` ASC SEPARATOR ',')")

    test("listagg falls back to standard when on overflow is present"):
        val q = listAgg.copy(onOverflow = Some(SqlListAggOnOverflow.Error))
        assertEquals(postgres(_.printExpr(q)), """LISTAGG("a", ',' ON OVERFLOW ERROR)""")
        assertEquals(sqlite(_.printExpr(q)), """LISTAGG("a", ',' ON OVERFLOW ERROR)""")
        assertEquals(sqlserver(_.printExpr(q)), """LISTAGG([a], N',' ON OVERFLOW ERROR)""")
        assertEquals(mysql(_.printExpr(q)), """LISTAGG(`a`, ',' ON OVERFLOW ERROR)""")

    test("sqlite listagg keeps the FILTER clause"):
        val q = listAgg.copy(filter = Some(SqlExpr.Binary(col("b"), SqlBinaryOperator.GreaterThan, num(0))))
        assertEquals(postgres(_.printExpr(q)), """STRING_AGG("a", ',') FILTER (WHERE "b" > 0)""")
        assert(
            sqlite(_.printExpr(q)).contains("FILTER"),
            s"FILTER was dropped: ${sqlite(_.printExpr(q))}"
        )

    test("nulls ordering emulation"):
        val ascLast = SqlOrderingItem(col("a"), Some(SqlOrdering.Asc), Some(SqlNullsOrdering.Last))
        assertEquals(
            mysql(_.printOrderingItem(ascLast)),
            """CASE WHEN `a` IS NULL THEN 1 ELSE 0 END ASC, `a` ASC"""
        )
        assertEquals(
            sqlserver(_.printOrderingItem(ascLast)),
            """CASE WHEN [a] IS NULL THEN 1 ELSE 0 END ASC, [a] ASC"""
        )
        assertEquals(postgres(_.printOrderingItem(ascLast)), """"a" ASC NULLS LAST""")

    test("nulls ordering that matches the native default is not emulated"):
        val ascFirst = SqlOrderingItem(col("a"), Some(SqlOrdering.Asc), Some(SqlNullsOrdering.First))
        assertEquals(mysql(_.printOrderingItem(ascFirst)), "`a` ASC")
        val descLast = SqlOrderingItem(col("a"), Some(SqlOrdering.Desc), Some(SqlNullsOrdering.Last))
        assertEquals(mysql(_.printOrderingItem(descLast)), "`a` DESC")

    test("oracle and sqlserver omit RECURSIVE"):
        val q = SqlQuery.With(true, nel(SqlWithItem("c", Nil, QueryFixture.simple)), QueryFixture.simple)
        assert(oracle(_.printQuery(q)).startsWith("WITH \n"), oracle(_.printQuery(q)))
        assert(sqlserver(_.printQuery(q)).startsWith("WITH \n"), sqlserver(_.printQuery(q)))
        assert(postgres(_.printQuery(q)).startsWith("WITH RECURSIVE\n"))

    test("oracle table alias omits AS"):
        assertEquals(
            oracle(_.printTable(table("t").copy(alias = Some(sqala.ast.table.SqlTableAlias("x", List("p")))))),
            """"t" "x"("p")"""
        )
        assertEquals(
            postgres(_.printTable(table("t").copy(alias = Some(sqala.ast.table.SqlTableAlias("x", Nil))))),
            """"t" AS "x""""
        )

    test("oracle temporal literals use conversion functions"):
        assertEquals(
            oracle(_.printExpr(SqlExpr.TimeLiteral(SqlTimeType.Date, "2020-01-01"))),
            "TO_DATE('2020-01-01', 'YYYY-MM-DD')"
        )
        assertEquals(
            oracle(_.printExpr(SqlExpr.TimeLiteral(SqlTimeType.Timestamp(None), "2020-01-01 00:00:00"))),
            "TO_TIMESTAMP('2020-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS.FF9')"
        )
        assertEquals(
            oracle(_.printExpr(SqlExpr.TimeLiteral(SqlTimeType.Timestamp(Some(SqlTimeZoneMode.With)), "2020-01-01 00:00:00"))),
            "TO_TIMESTAMP_TZ('2020-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS.FF9 TZH:TZM')"
        )
        assertEquals(
            oracle(_.printExpr(SqlExpr.TimeLiteral(SqlTimeType.Time(None), "12:00:00"))),
            "TIME '12:00:00'"
        )

    test("sqlserver temporal literals use CAST"):
        assertEquals(
            sqlserver(_.printExpr(SqlExpr.TimeLiteral(SqlTimeType.Date, "2020-01-01"))),
            "CAST(N'2020-01-01' AS DATE)"
        )
        assertEquals(
            sqlserver(_.printExpr(SqlExpr.TimeLiteral(SqlTimeType.Timestamp(None), "2020-01-01 00:00:00"))),
            "CAST(N'2020-01-01 00:00:00' AS DATETIME2)"
        )
        assertEquals(
            sqlserver(_.printExpr(SqlExpr.TimeLiteral(SqlTimeType.Timestamp(Some(SqlTimeZoneMode.With)), "2020-01-01 00:00:00"))),
            "CAST(N'2020-01-01 00:00:00' AS DATETIMEOFFSET)"
        )

    test("sqlserver N prefix also applies to inlined string literals"):
        assertEquals(sqlserver(_.printExpr(SqlExpr.StringLiteral("x"))), "N'x'")
        assertEquals(
            sqlserver(_.printExpr(SqlExpr.InlinedLiteral(SqlExpr.StringLiteral("x")))),
            "N'x'"
        )

    test("mysql backslash escaping depends on standardEscapeStrings"):
        val standard = render(new MysqlPrinter(true))(_.printExpr(SqlExpr.StringLiteral("a\\b")))
        val escaped = render(new MysqlPrinter(false))(_.printExpr(SqlExpr.StringLiteral("a\\b")))
        assertEquals(standard, "'a\\b'")
        assertEquals(escaped, "'a\\\\b'")

    test("h2 inherits standard behavior"):
        assertEquals(h2(_.printLimit(rowOnly)), "OFFSET 5 ROWS FETCH NEXT 10 ROWS ONLY")
        assertEquals(h2(_.printExpr(listAgg)), """LISTAGG("a", ',')""")
        assertEquals(h2(_.printType(SqlType.Int)), "INTEGER")