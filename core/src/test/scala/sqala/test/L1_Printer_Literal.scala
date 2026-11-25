package sqala.test

import sqala.ast.expr.*
import sqala.ast.token.SqlUnsafeCustomToken
import sqala.test.Coverage.assertCoverage
import sqala.util.NonEmptyList

object LiteralFixture:
    def col(n: String): SqlExpr = SqlExpr.Column(None, n)
    def nel[A](h: A, t: A*): NonEmptyList[A] = NonEmptyList(h, t.toList)

    val timeUnits: Map[String, (SqlTimeUnit, String)] = Map(
        "Year" -> (SqlTimeUnit.Year, "YEAR"),
        "Month" -> (SqlTimeUnit.Month, "MONTH"),
        "Day" -> (SqlTimeUnit.Day, "DAY"),
        "Hour" -> (SqlTimeUnit.Hour, "HOUR"),
        "Minute" -> (SqlTimeUnit.Minute, "MINUTE"),
        "Second" -> (SqlTimeUnit.Second, "SECOND")
    )

class L1_Printer_Literal extends munit.FunSuite:
    import LiteralFixture.*

    test("column reference"):
        assertEquals(createSql(_.printExpr(col("a"))), """"a"""")
        assertEquals(createSql(_.printExpr(SqlExpr.Column(Some("t"), "a"))), """"t"."a"""")

    test("null and boolean literals"):
        assertEquals(createSql(_.printExpr(SqlExpr.NullLiteral)), "NULL")
        assertEquals(createSql(_.printExpr(SqlExpr.BooleanLiteral(true))), "TRUE")
        assertEquals(createSql(_.printExpr(SqlExpr.BooleanLiteral(false))), "FALSE")

    test("string literal escapes single quotes"):
        assertEquals(createSql(_.printExpr(SqlExpr.StringLiteral("abc"))), "'abc'")
        assertEquals(createSql(_.printExpr(SqlExpr.StringLiteral("a'b"))), "'a''b'")
        assertEquals(createSql(_.printExpr(SqlExpr.StringLiteral(""))), "''")

    test("string literal keeps backslash under standard escape strings"):
        assertEquals(createSql(_.printExpr(SqlExpr.StringLiteral("a\\b"))), "'a\\b'")

    test("numeric literals across Numeric types"):
        assertEquals(createSql(_.printExpr(SqlExpr.NumberLiteral(42))), "42")
        assertEquals(createSql(_.printExpr(SqlExpr.NumberLiteral(42L))), "42")
        assertEquals(createSql(_.printExpr(SqlExpr.NumberLiteral(3.14))), "3.14")
        assertEquals(createSql(_.printExpr(SqlExpr.NumberLiteral(1.5f))), "1.5")
        assertEquals(createSql(_.printExpr(SqlExpr.NumberLiteral(BigDecimal("123.456")))), "123.456")
        assertEquals(createSql(_.printExpr(SqlExpr.NumberLiteral(7.toShort))), "7")
        assertEquals(createSql(_.printExpr(SqlExpr.NumberLiteral(7.toByte))), "7")
        assertEquals(createSql(_.printExpr(SqlExpr.NumberLiteral(BigInt("12345678901234567890")))), "12345678901234567890")
        assertEquals(createSql(_.printExpr(SqlExpr.NumberLiteral(-5))), "-5")

    test("time unit tokens"):
        assertCoverage[SqlTimeUnit]("Year", "Month", "Day", "Hour", "Minute", "Second", "UnsafeCustom")
        assertEquals(timeUnits.keySet + "UnsafeCustom", Coverage.casesOf[SqlTimeUnit])

        for (name, (u, expected)) <- timeUnits.toList.sortBy(_._1) do
            assertEquals(createSql(_.printTimeUnit(u)), expected, name)

        assertEquals(
            createSql(_.printTimeUnit(SqlTimeUnit.UnsafeCustom(List(SqlUnsafeCustomToken.Keyword("WEEK"))))),
            "WEEK"
        )

    test("time zone mode tokens"):
        assertCoverage[SqlTimeZoneMode]("With", "Without")
        assertEquals(createSql(_.printTimeZoneMode(SqlTimeZoneMode.With)), "WITH TIME ZONE")
        assertEquals(createSql(_.printTimeZoneMode(SqlTimeZoneMode.Without)), "WITHOUT TIME ZONE")

    test("time type tokens"):
        assertCoverage[SqlTimeType]("Timestamp", "Date", "Time")

        assertEquals(createSql(_.printTimeType(SqlTimeType.Date)), "DATE")
        assertEquals(createSql(_.printTimeType(SqlTimeType.Timestamp(None))), "TIMESTAMP")
        assertEquals(createSql(_.printTimeType(SqlTimeType.Time(None))), "TIME")
        assertEquals(
            createSql(_.printTimeType(SqlTimeType.Timestamp(Some(SqlTimeZoneMode.With)))),
            "TIMESTAMP WITH TIME ZONE"
        )
        assertEquals(
            createSql(_.printTimeType(SqlTimeType.Time(Some(SqlTimeZoneMode.Without)))),
            "TIME WITHOUT TIME ZONE"
        )

    test("interval field tokens"):
        assertCoverage[SqlIntervalField]("To", "Single")

        assertEquals(createSql(_.printIntervalField(SqlIntervalField.Single(SqlTimeUnit.Day))), "DAY")
        assertEquals(
            createSql(_.printIntervalField(SqlIntervalField.To(SqlTimeUnit.Year, SqlTimeUnit.Month))),
            "YEAR TO MONTH"
        )

    test("time literal"):
        assertEquals(
            createSql(_.printExpr(SqlExpr.TimeLiteral(SqlTimeType.Date, "2020-01-01"))),
            "DATE '2020-01-01'"
        )
        assertEquals(
            createSql(_.printExpr(SqlExpr.TimeLiteral(SqlTimeType.Timestamp(None), "2020-01-01 00:00:00"))),
            "TIMESTAMP '2020-01-01 00:00:00'"
        )
        assertEquals(
            createSql(_.printExpr(SqlExpr.TimeLiteral(SqlTimeType.Time(None), "12:00:00"))),
            "TIME '12:00:00'"
        )

    test("interval literal"):
        assertEquals(
            createSql(_.printExpr(SqlExpr.IntervalLiteral("1", SqlIntervalField.Single(SqlTimeUnit.Day)))),
            "INTERVAL '1' DAY"
        )
        assertEquals(
            createSql(_.printExpr(SqlExpr.IntervalLiteral("1-2", SqlIntervalField.To(SqlTimeUnit.Year, SqlTimeUnit.Month)))),
            "INTERVAL '1-2' YEAR TO MONTH"
        )

    test("inlined literal delegates to the wrapped literal"):
        assertEquals(createSql(_.printExpr(SqlExpr.InlinedLiteral(SqlExpr.StringLiteral("x")))), "'x'")
        assertEquals(createSql(_.printExpr(SqlExpr.InlinedLiteral(SqlExpr.NumberLiteral(1)))), "1")
        assertEquals(createSql(_.printExpr(SqlExpr.InlinedLiteral(SqlExpr.BooleanLiteral(true)))), "TRUE")
        assertEquals(
            createSql(_.printExpr(SqlExpr.InlinedLiteral(SqlExpr.TimeLiteral(SqlTimeType.Date, "2020-01-01")))),
            "DATE '2020-01-01'"
        )

    test("tuple and array"):
        assertEquals(createSql(_.printExpr(SqlExpr.Tuple(nel(col("a"))))), """("a")""")
        assertEquals(createSql(_.printExpr(SqlExpr.Tuple(nel(col("a"), col("b"))))), """("a", "b")""")
        assertEquals(createSql(_.printExpr(SqlExpr.Array(Nil))), "ARRAY[]")
        assertEquals(createSql(_.printExpr(SqlExpr.Array(List(col("a"), col("b"))))), """ARRAY["a", "b"]""")

    test("unsafe custom expression is parenthesized"):
        assertEquals(
            createSql(_.printExpr(SqlExpr.UnsafeCustom(List(SqlUnsafeCustomToken.Keyword("FOO"))))),
            "(FOO)"
        )
        assertEquals(
            createSql(_.printExpr(SqlExpr.UnsafeCustom(
                List(SqlUnsafeCustomToken.Keyword("a"), SqlUnsafeCustomToken.Expr(col("b")))
            ))),
            """(a "b")"""
        )