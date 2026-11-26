package sqala.test

import sqala.ast.expr.*
import sqala.metadata.*
import sqala.util.NonEmptyList

import java.time.*

enum IntState:
    @enumValue(1) case Active
    @enumValue(0) case Deleted

object IntState:
    given CustomField[IntState, Int] = CustomField.derived

enum StrState:
    @enumName("ACTIVE") case Active
    case Deleted

object StrState:
    given CustomField[StrState, String] = CustomField.derived

object MetadataFixture:
    val date: LocalDate = LocalDate.of(2020, 1, 2)
    val dateTime: LocalDateTime = LocalDateTime.of(2020, 1, 2, 3, 4, 5, 123000000)
    val time: LocalTime = LocalTime.of(3, 4, 5, 123000000)
    val offsetDateTime: OffsetDateTime = OffsetDateTime.of(dateTime, ZoneOffset.ofHours(8))
    val offsetTime: OffsetTime = OffsetTime.of(time, ZoneOffset.ofHours(8))

    def expr[T](x: T, inlined: Boolean)(using a: AsSqlExpr[T]): SqlExpr = a.asSqlExpr(x, inlined)

    def sql[T](x: T, inlined: Boolean)(using a: AsSqlExpr[T]): String =
        createSql(_.printExpr(a.asSqlExpr(x, inlined)))

class M1_Metadata extends munit.FunSuite:
    import MetadataFixture.*

    test("numeric sqlType mapping"):
        assertEquals(summon[AsSqlExpr[Int]].sqlType, SqlType.Int)
        assertEquals(summon[AsSqlExpr[Long]].sqlType, SqlType.Long)
        assertEquals(summon[AsSqlExpr[Float]].sqlType, SqlType.Float)
        assertEquals(summon[AsSqlExpr[Double]].sqlType, SqlType.Double)
        assertEquals(summon[AsSqlExpr[BigDecimal]].sqlType, SqlType.Decimal(Some(38, 10)))

    test("numeric literals in both positions"):
        assertEquals(sql(1, false), "1")
        assertEquals(sql(1, true), "1")
        assertEquals(sql(1L, false), "1")
        assertEquals(sql(1.5f, false), "1.5")
        assertEquals(sql(1.5d, false), "1.5")
        assertEquals(sql(BigDecimal("1.50"), false), "1.50")
        assertEquals(expr(1, false), SqlExpr.NumberLiteral(1))
        assertEquals(expr(1, true), SqlExpr.InlinedLiteral(SqlExpr.NumberLiteral(1)))

    test("string and boolean literals"):
        assertEquals(summon[AsSqlExpr[String]].sqlType, SqlType.Varchar(None))
        assertEquals(summon[AsSqlExpr[Boolean]].sqlType, SqlType.Boolean)
        assertEquals(sql("a", false), "'a'")
        assertEquals(sql(true, false), "TRUE")
        assertEquals(sql(false, false), "FALSE")

    test("temporal literals"):
        assertEquals(summon[AsSqlExpr[LocalDate]].sqlType, SqlType.Date)
        assertEquals(summon[AsSqlExpr[LocalDateTime]].sqlType, SqlType.Timestamp(None))
        assertEquals(summon[AsSqlExpr[LocalTime]].sqlType, SqlType.Time(None))
        assertEquals(summon[AsSqlExpr[OffsetDateTime]].sqlType, SqlType.Timestamp(Some(SqlTimeZoneMode.With)))
        assertEquals(summon[AsSqlExpr[OffsetTime]].sqlType, SqlType.Time(Some(SqlTimeZoneMode.With)))

        assertEquals(sql(date, false), "DATE '2020-01-02'")
        assertEquals(sql(dateTime, false), "TIMESTAMP '2020-01-02 03:04:05.123000000'")
        assertEquals(sql(time, false), "TIME '03:04:05.123000000'")
        assertEquals(sql(offsetDateTime, false), "TIMESTAMP WITH TIME ZONE '2020-01-02 03:04:05.123000000 +08:00'")
        assertEquals(sql(offsetTime, false), "TIME WITH TIME ZONE '03:04:05.123000000 +08:00'")

    test("temporal literals keep nanosecond precision"):
        val precise = LocalDateTime.of(2020, 1, 2, 3, 4, 5, 1)
        assertEquals(sql(precise, false), "TIMESTAMP '2020-01-02 03:04:05.000000001'")
        val zero = LocalDateTime.of(2020, 1, 2, 3, 4, 5, 0)
        assertEquals(sql(zero, false), "TIMESTAMP '2020-01-02 03:04:05.000000000'")

    test("json values are cast to JSON"):
        assertEquals(summon[AsSqlExpr[Json]].sqlType, SqlType.Json)
        assertEquals(sql(Json("{}"), false), """CAST('{}' AS JSON)""")
        assertEquals(sql(Json("{}"), true), """CAST('{}' AS JSON)""")

    test("interval values carry their unit"):
        assertEquals(summon[AsSqlExpr[Interval]].sqlType, SqlType.Interval)
        assertEquals(sql(Interval("1", SqlTimeUnit.Day), false), "INTERVAL '1' DAY")
        // Interval.unit is a single SqlTimeUnit, so range units such as
        // YEAR TO MONTH cannot be expressed through AsSqlExpr[Interval].
        assertEquals(sql(Interval("2", SqlTimeUnit.Month), false), "INTERVAL '2' MONTH")

    test("geometry values use ST_GeomFromText"):
        assertEquals(summon[AsSqlExpr[Geometry]].sqlType, SqlType.Geometry)
        assertEquals(summon[AsSqlExpr[Point]].sqlType, SqlType.Point)
        assertEquals(summon[AsSqlExpr[LineString]].sqlType, SqlType.LineString)
        assertEquals(summon[AsSqlExpr[Polygon]].sqlType, SqlType.Polygon)
        assertEquals(summon[AsSqlExpr[MultiPoint]].sqlType, SqlType.MultiPoint)
        assertEquals(summon[AsSqlExpr[MultiLineString]].sqlType, SqlType.MultiLineString)
        assertEquals(summon[AsSqlExpr[MultiPolygon]].sqlType, SqlType.MultiPolygon)
        assertEquals(summon[AsSqlExpr[GeometryCollection]].sqlType, SqlType.GeometryCollection)

        assertEquals(sql(Point("POINT(1 2)", 4326), false), "ST_GeomFromText('POINT(1 2)', 4326)")
        assertEquals(sql(Geometry("POINT(1 2)", 4326), true), "ST_GeomFromText('POINT(1 2)', 4326)")

    test("every geometry type renders through the same function"):
        val wkt = "POINT(1 2)"
        val rendered = List(
            sql(Geometry(wkt, 4326), false),
            sql(Point(wkt, 4326), false),
            sql(LineString(wkt, 4326), false),
            sql(Polygon(wkt, 4326), false),
            sql(MultiPoint(wkt, 4326), false),
            sql(MultiLineString(wkt, 4326), false),
            sql(MultiPolygon(wkt, 4326), false),
            sql(GeometryCollection(wkt, 4326), false)
        )
        assertEquals(rendered.distinct, List("ST_GeomFromText('POINT(1 2)', 4326)"))

    test("option Some delegates to the underlying instance"):
        assertEquals(summon[AsSqlExpr[Option[Int]]].sqlType, SqlType.Int)
        assertEquals(sql(Option(1), false), "1")
        assertEquals(sql(Option("a"), true), "'a'")

    test("option None becomes a typed NULL"):
        assertEquals(sql(Option.empty[Int], false), "CAST(NULL AS INTEGER)")
        assertEquals(sql(Option.empty[String], false), "CAST(NULL AS VARCHAR)")
        assertEquals(sql(Option.empty[LocalDate], false), "CAST(NULL AS DATE)")

    test("arrays map to array types"):
        assertEquals(summon[AsSqlExpr[Array[Int]]].sqlType, SqlType.Array(SqlType.Int))
        assertEquals(sql(Array(1, 2), false), "ARRAY[1, 2]")
        assertEquals(sql(Array("a"), true), "ARRAY['a']")

    test("empty arrays become a typed empty array"):
        assertEquals(sql(Array.empty[Int], false), "CAST(ARRAY[] AS INTEGER[])")

    test("AsSqlExprs for single values and tuples"):
        assertEquals(summon[AsSqlExprs[Int]].asSqlExprs(1, false), List(SqlExpr.NumberLiteral(1)))
        assertEquals(
            summon[AsSqlExprs[(Int, String)]].asSqlExprs((1, "a"), false),
            List(SqlExpr.NumberLiteral(1), SqlExpr.StringLiteral("a"))
        )
        assertEquals(summon[AsSqlExprs[EmptyTuple]].asSqlExprs(EmptyTuple, false), Nil)

    test("AsSqlExprs propagates the inlined flag"):
        assertEquals(
            summon[AsSqlExprs[(Int, String)]].asSqlExprs((1, "a"), true),
            List(
                SqlExpr.InlinedLiteral(SqlExpr.NumberLiteral(1)),
                SqlExpr.InlinedLiteral(SqlExpr.StringLiteral("a"))
            )
        )

    test("marker type classes are summonable for their base and option types"):
        val numbers = List(summon[SqlNumber[Int]], summon[SqlNumber[Long]], summon[SqlNumber[Float]],
            summon[SqlNumber[Double]], summon[SqlNumber[BigDecimal]], summon[SqlNumber[Option[Int]]])
        val dates = List(summon[SqlDate[LocalDate]], summon[SqlDate[Option[LocalDate]]])
        val dateTimes = List(summon[SqlDateTime[LocalDateTime]], summon[SqlDateTime[OffsetDateTime]],
            summon[SqlDateTime[Option[LocalDateTime]]])
        val times = List(summon[SqlTime[LocalTime]], summon[SqlTime[OffsetTime]], summon[SqlTime[Option[LocalTime]]])
        val strings = List(summon[SqlString[String]], summon[SqlString[Option[String]]])
        val booleans = List(summon[SqlBoolean[Boolean]], summon[SqlBoolean[Option[Boolean]]])
        val jsons = List(summon[SqlJson[Json]], summon[SqlJson[Option[Json]]])
        val intervals = List(summon[SqlInterval[Interval]], summon[SqlInterval[Option[Interval]]])
        val geometries = List(summon[SqlGeometry[Geometry]], summon[SqlGeometry[Option[Point]]])
        assertEquals(
            List(numbers.size, dates.size, dateTimes.size, times.size, strings.size,
                booleans.size, jsons.size, intervals.size, geometries.size),
            List(6, 2, 3, 3, 2, 2, 2, 2, 2)
        )

    test("a hand-written CustomField maps through the underlying instance"):
        given CustomField[Wrapper, Int] with
            def toValue(x: Wrapper): Int = x.n
            def fromValue(x: Int): Wrapper = Wrapper(x)
        assertEquals(summon[CustomField[Wrapper, Int]].sqlType, SqlType.Int)
        assertEquals(sql(Wrapper(7), false), "7")
        assertEquals(summon[CustomField[Wrapper, Int]].fromValue(7), Wrapper(7))

    test("derived integer enum mapping reads enumValue"):
        assertEquals(sql(IntState.Active, false), "1")
        assertEquals(sql(IntState.Deleted, false), "0")
        assertEquals(summon[CustomField[IntState, Int]].fromValue(1), IntState.Active)
        assertEquals(summon[CustomField[IntState, Int]].fromValue(0), IntState.Deleted)

    test("derived string enum mapping falls back to the case name"):
        assertEquals(sql(StrState.Active, false), "'ACTIVE'")
        assertEquals(sql(StrState.Deleted, false), "'Deleted'")
        assertEquals(summon[CustomField[StrState, String]].fromValue("ACTIVE"), StrState.Active)
        assertEquals(summon[CustomField[StrState, String]].fromValue("Deleted"), StrState.Deleted)

    test("decoding an unknown enum value throws EnumDecodeException"):
        val ex = intercept[EnumDecodeException](summon[CustomField[IntState, Int]].fromValue(9))
        assertEquals(ex.value, 9)
        assert(ex.enumName.contains("IntState"), ex.enumName)
        assert(ex.getMessage.contains("9"), ex.getMessage)

    test("Json exposes its raw text"):
        assertEquals(Json("{}").value, "{}")

    test("TableMetaData keeps its fields"):
        val m = TableMetaData("t", List("id"), Some("id"), NonEmptyList("id", List("name")), NonEmptyList("id", List("name")))
        assertEquals(m.tableName, "t")
        assertEquals(m.primaryKeyFields, List("id"))
        assertEquals(m.incrementField, Some("id"))
        assertEquals(m.columns.toList, List("id", "name"))
        assertEquals(m.fields.toList, List("id", "name"))

case class Wrapper(n: Int)