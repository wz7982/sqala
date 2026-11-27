package sqala.test

import sqala.dynamic.dsl.{column as dcolumn, select as dselect, table as dtable}
import sqala.jdbc.*
import sqala.metadata.Json
import sqala.static.dsl.*
import sqala.test.JdbcFixture.*

import java.time.{LocalDate, LocalDateTime, LocalTime, OffsetDateTime, OffsetTime}

/**
 * PostgreSQL column type roundtrips for every supported scalar, including
 * optional columns, arrays, JSON, custom fields and java.time types.
 */
class J5_Types_Postgresql extends munit.FunSuite:
    private def db: JdbcContext =
        pgContext

    private def unwrap(value: Option[Any]): Any =
        value.getOrElse(None) match
            case Some(inner) => inner
            case other => other

    override def beforeAll(): Unit =
        runPgDdl((pgDdl ++ pgScalarDdl)*)

    private val row = JScalarPg(
        1, "hi", None, true, 7, 1234567890123L, 1.5f, 2.25d, BigDecimal("123.4500"),
        Json("""{"a":1}"""), None, Array(1, 2, 3), None,
        LocalDate.of(2026, 2, 1), LocalDateTime.of(2026, 2, 1, 9, 30, 15, 123456000), LocalTime.of(9, 30, 45, 987654000),
        OffsetDateTime.parse("2026-02-01T09:30:00+08:00"), OffsetTime.parse("09:30:00+08:00")
    )

    test("scalar columns roundtrip through the entity api"):
        db.execute(sql"delete from j_scalar_pg")
        assertEquals(db.insert(row), 1)
        val rows = db.fetch(from(JScalarPg))
        assertEquals(rows.length, 1)
        val got = rows.head
        assertEquals((got.s, got.os, got.b, got.i, got.l, got.f, got.d, got.bd), (row.s, row.os, row.b, row.i, row.l, row.f, row.d, row.bd))
        assertEquals((got.dt, got.lts, got.ltm, got.otz), (row.dt, row.lts, row.ltm, row.otz))
        assertEquals(got.odt.toInstant, row.odt.toInstant)
        assert(sameRows(List(got.arr), List(List(1, 2, 3))))

    test("optional columns store and read nulls"):
        db.execute(sql"delete from j_scalar_pg")
        db.insert(row.copy(os = Some("opt"), ojs = Some(Json("[1,2]")), oarr = Some(Array(4, 5))))
        db.insert(row.copy(id = 2))
        val two = db.fetchByPrimaryKeys[JScalarPg](List(1, 2))
        assertEquals(two.map(r => (r.os, r.ojs, r.oarr.map(_.toList))), List((Some("opt"), Some(Json("[1,2]")), Some(List(4, 5))), (None, None, None)))

    test("json columns roundtrip"):
        db.execute(sql"delete from j_scalar_pg")
        db.insert(row.copy(ojs = Some(Json("[1,2]"))))
        val got = db.findByPrimaryKey[JScalarPg](1).get
        assertEquals(got.js, row.js)
        assertEquals(got.ojs, Some(Json("[1,2]")))

    test("custom field enum roundtrips"):
        db.execute(sql"delete from j_custom")
        assertEquals(db.insert(JCustom(1, JStatus.Active, Some(JStatus.Inactive))), 1)
        db.insert(JCustom(2, JStatus.Inactive, None))
        assertEquals(
            db.fetch(from(JCustom).sortBy(c => c.id)),
            List(JCustom(1, JStatus.Active, Some(JStatus.Inactive)), JCustom(2, JStatus.Inactive, None))
        )

    test("time literal filters bind as typed parameters"):
        db.execute(sql"delete from j_time")
        db.insert(JTime(
            LocalDate.of(2026, 2, 1), OffsetDateTime.parse("2026-02-01T09:30:00+08:00"),
            LocalDateTime.of(2026, 2, 1, 9, 30), OffsetTime.parse("09:30:00+08:00"), LocalTime.of(9, 30)
        ))
        val (text, args) = db.showSql(from(JTime).filter(t => t.dt === LocalDate.of(2026, 2, 1)).map(t => t.ts))
        assert(text.contains("?"))
        assertEquals(args.toList, List(LocalDate.of(2026, 2, 1)))
        assertEquals(db.fetch(from(JTime).filter(t => t.dt === LocalDate.of(2026, 2, 1)).map(t => t.lts)), List(LocalDateTime.of(2026, 2, 1, 9, 30)))
        assertEquals(db.fetch(from(JTime).filter(t => t.ltm === LocalTime.of(9, 30)).map(t => t.dt)), List(LocalDate.of(2026, 2, 1)))

    test("fetchToMap maps jdbc types"):
        db.execute(sql"delete from j_scalar_pg")
        db.insert(row)
        val q = dselect(
            dcolumn("id").as("id"), dcolumn("bd").as("bd"), dcolumn("dt").as("dt"), dcolumn("lts").as("lts")
        ).from(dtable("j_scalar_pg"))
        val m = db.fetchToMap(q).head
        assertEquals(m("id"), 1)
        assertEquals(m("bd"), Some(BigDecimal("123.4500")))
        assert(unwrap(m.get("dt")).isInstanceOf[java.sql.Timestamp])
        assert(unwrap(m.get("lts")).isInstanceOf[java.time.Instant])

    test("unicode and backslash text roundtrips"):
        db.execute(sql"delete from j_scalar_pg")
        db.insert(row.copy(s = "中文\\text", os = Some("😀")))
        val got = db.findByPrimaryKey[JScalarPg](1).get
        assertEquals((got.s, got.os), ("中文\\text", Some("😀")))


/**
 * MySQL column type roundtrips for every supported scalar.
 */
class J5_Types_Mysql extends munit.FunSuite:
    private def db: JdbcContext =
        mysqlContext

    private def unwrap(value: Option[Any]): Any =
        value.getOrElse(None) match
            case Some(inner) => inner
            case other => other

    override def beforeAll(): Unit =
        runMysqlDdl((mysqlDdl ++ mysqlScalarDdl)*)

    private val row = JScalarMysql(
        1, "hi", None, true, 7, 1234567890123L, 1.5f, 2.25d, BigDecimal("123.4500"),
        Json("""{"a":1}"""),
        LocalDate.of(2026, 2, 1), LocalDateTime.of(2026, 2, 1, 9, 30, 15, 123456000), LocalTime.of(9, 30, 45, 987654000),
        OffsetDateTime.parse("2026-02-01T09:30:00+08:00")
    )

    test("scalar columns roundtrip through the entity api"):
        db.execute(sql"delete from j_scalar_mysql")
        assertEquals(db.insert(row), 1)
        val rows = db.fetch(from(JScalarMysql))
        assertEquals(rows.length, 1)
        val got = rows.head
        assertEquals((got.s, got.os, got.b, got.i, got.l, got.f, got.d, got.bd), (row.s, row.os, row.b, row.i, row.l, row.f, row.d, row.bd))
        assertEquals((got.dt, got.lts, got.ltm), (row.dt, row.lts, row.ltm))
        assertEquals(got.odt.toInstant, row.odt.toInstant)

    test("optional columns store and read nulls"):
        db.execute(sql"delete from j_scalar_mysql")
        db.insert(row.copy(os = Some("opt")))
        db.insert(row.copy(id = 2))
        val two = db.fetchByPrimaryKeys[JScalarMysql](List(1, 2))
        assertEquals(two.map(_.os), List(Some("opt"), None))

    test("json column roundtrips"):
        db.execute(sql"delete from j_scalar_mysql")
        db.insert(row)
        // the mysql server reformats stored json text, so compare the semantic text
        assertEquals(db.findByPrimaryKey[JScalarMysql](1).get.js.value.replaceAll("\\s", ""), row.js.value)

    test("custom field enum roundtrips"):
        db.execute(sql"delete from j_custom")
        db.insert(JCustom(1, JStatus.Active, Some(JStatus.Inactive)))
        db.insert(JCustom(2, JStatus.Inactive, None))
        assertEquals(
            db.fetch(from(JCustom).sortBy(c => c.id)),
            List(JCustom(1, JStatus.Active, Some(JStatus.Inactive)), JCustom(2, JStatus.Inactive, None))
        )

    test("datetime(6) keeps microsecond precision"):
        db.execute(sql"delete from j_scalar_mysql")
        db.insert(row)
        val got = db.findByPrimaryKey[JScalarMysql](1).get
        assertEquals((got.lts.getNano, got.ltm.getNano), (123456000, 987654000))

    test("fetchToMap maps jdbc types"):
        db.execute(sql"delete from j_scalar_mysql")
        db.insert(row)
        val q = dselect(
            dcolumn("id").as("id"), dcolumn("bd").as("bd"), dcolumn("dt").as("dt"), dcolumn("lts").as("lts")
        ).from(dtable("j_scalar_mysql"))
        val m = db.fetchToMap(q).head
        assertEquals(m("id"), 1)
        assertEquals(m("bd"), Some(BigDecimal("123.4500")))
        assert(unwrap(m.get("dt")).isInstanceOf[java.sql.Timestamp])
        assert(unwrap(m.get("lts")).isInstanceOf[java.time.Instant])

    test("utf8 text roundtrips"):
        db.execute(sql"delete from j_scalar_mysql")
        db.insert(row.copy(s = "中文\\text", os = Some("😀")))
        val got = db.findByPrimaryKey[JScalarMysql](1).get
        assertEquals((got.s, got.os), ("中文\\text", Some("😀")))
