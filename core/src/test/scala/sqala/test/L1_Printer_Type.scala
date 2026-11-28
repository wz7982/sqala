package sqala.test

import sqala.ast.expr.*
import sqala.ast.token.SqlUnsafeCustomToken
import sqala.test.Coverage.assertCoverage
import sqala.util.NonEmptyList

object TypeFixture:
    def col(n: String): SqlExpr = SqlExpr.Column(None, n)
    def num(n: Int): SqlExpr = SqlExpr.NumberLiteral(n)
    def nel[A](h: A, t: A*): NonEmptyList[A] = NonEmptyList(h, t.toList)

    val plainTypes: Map[String, (SqlType, String)] = Map(
        "Int" -> (SqlType.Int, "INTEGER"),
        "Long" -> (SqlType.Long, "BIGINT"),
        "Float" -> (SqlType.Float, "REAL"),
        "Double" -> (SqlType.Double, "DOUBLE PRECISION"),
        "Date" -> (SqlType.Date, "DATE"),
        "Json" -> (SqlType.Json, "JSON"),
        "Boolean" -> (SqlType.Boolean, "BOOLEAN"),
        "Interval" -> (SqlType.Interval, "INTERVAL"),
        "Geometry" -> (SqlType.Geometry, "GEOMETRY"),
        "Point" -> (SqlType.Point, "POINT"),
        "LineString" -> (SqlType.LineString, "LINESTRING"),
        "Polygon" -> (SqlType.Polygon, "POLYGON"),
        "MultiPoint" -> (SqlType.MultiPoint, "MULTIPOINT"),
        "MultiLineString" -> (SqlType.MultiLineString, "MULTILINESTRING"),
        "MultiPolygon" -> (SqlType.MultiPolygon, "MULTIPOLYGON"),
        "GeometryCollection" -> (SqlType.GeometryCollection, "GEOMETRYCOLLECTION")
    )

class L1_Printer_Type extends munit.FunSuite:
    import TypeFixture.*

    test("type coverage"):
        assertCoverage[SqlType](
            "Varchar", "Int", "Long", "Float", "Double", "Decimal", "Date", "Timestamp", "Time",
            "Json", "Boolean", "Interval", "Geometry", "Point", "LineString", "Polygon",
            "MultiPoint", "MultiLineString", "MultiPolygon", "GeometryCollection", "Array", "UnsafeCustom"
        )
        assertEquals(
            plainTypes.keySet ++ Set("Varchar", "Decimal", "Timestamp", "Time", "Array", "UnsafeCustom"),
            Coverage.casesOf[SqlType]
        )

    test("plain type tokens"):
        for (name, (t, expected)) <- plainTypes.toList.sortBy(_._1) do
            assertEquals(createSql(_.printType(t)), expected, name)

    test("varchar with and without length"):
        assertEquals(createSql(_.printType(SqlType.Varchar(None))), "VARCHAR")
        assertEquals(createSql(_.printType(SqlType.Varchar(Some(50)))), "VARCHAR(50)")

    test("decimal with and without precision"):
        assertEquals(createSql(_.printType(SqlType.Decimal(None))), "DECIMAL")
        assertEquals(createSql(_.printType(SqlType.Decimal(Some((10, 2))))), "DECIMAL(10, 2)")

    test("timestamp and time types carry time zone mode"):
        assertEquals(createSql(_.printType(SqlType.Timestamp(None))), "TIMESTAMP")
        assertEquals(createSql(_.printType(SqlType.Time(None))), "TIME")
        assertEquals(
            createSql(_.printType(SqlType.Timestamp(Some(SqlTimeZoneMode.With)))),
            "TIMESTAMP WITH TIME ZONE"
        )
        assertEquals(
            createSql(_.printType(SqlType.Time(Some(SqlTimeZoneMode.Without)))),
            "TIME WITHOUT TIME ZONE"
        )

    test("array type nests"):
        assertEquals(createSql(_.printType(SqlType.Array(SqlType.Int))), "INTEGER[]")
        assertEquals(
            createSql(_.printType(SqlType.Array(SqlType.Array(SqlType.Varchar(Some(10)))))),
            "VARCHAR(10)[][]"
        )

    test("unsafe custom type emits bare tokens"):
        assertEquals(
            createSql(_.printType(SqlType.UnsafeCustom(List(SqlUnsafeCustomToken.Keyword("UUID"))))),
            "UUID"
        )

    test("cast expression"):
        assertEquals(createSql(_.printExpr(SqlExpr.Cast(col("a"), SqlType.Int))), """CAST("a" AS INTEGER)""")
        assertEquals(
            createSql(_.printExpr(SqlExpr.Cast(SqlExpr.StringLiteral("1"), SqlType.Decimal(Some((5, 2)))))),
            "CAST('1' AS DECIMAL(5, 2))"
        )

    test("searched case expression"):
        val branch = SqlCaseBranch(SqlExpr.Binary(col("a"), SqlBinaryOperator.Equal, num(1)), num(10))

        assertEquals(
            createSql(_.printExpr(SqlExpr.Case(nel(branch), None))),
            """CASE WHEN "a" = 1 THEN 10 END"""
        )
        assertEquals(
            createSql(_.printExpr(SqlExpr.Case(nel(branch), Some(num(0))))),
            """CASE WHEN "a" = 1 THEN 10 ELSE 0 END"""
        )
        assertEquals(
            createSql(_.printExpr(SqlExpr.Case(
                nel(branch, SqlCaseBranch(SqlExpr.BooleanLiteral(true), num(20))),
                Some(num(0))
            ))),
            """CASE WHEN "a" = 1 THEN 10 WHEN TRUE THEN 20 ELSE 0 END"""
        )

    test("simple case expression"):
        val branch = SqlCaseBranch(num(1), num(10))

        assertEquals(
            createSql(_.printExpr(SqlExpr.SimpleCase(col("a"), nel(branch), None))),
            """CASE "a" WHEN 1 THEN 10 END"""
        )
        assertEquals(
            createSql(_.printExpr(SqlExpr.SimpleCase(col("a"), nel(branch), Some(num(0))))),
            """CASE "a" WHEN 1 THEN 10 ELSE 0 END"""
        )

    test("coalesce and nullif"):
        assertEquals(createSql(_.printExpr(SqlExpr.Coalesce(nel(col("a"))))), """COALESCE("a")""")
        assertEquals(
            createSql(_.printExpr(SqlExpr.Coalesce(nel(col("a"), col("b"), num(0))))),
            """COALESCE("a", "b", 0)"""
        )
        assertEquals(
            createSql(_.printExpr(SqlExpr.NullIf(col("a"), num(0)))),
            """NULLIF("a", 0)"""
        )