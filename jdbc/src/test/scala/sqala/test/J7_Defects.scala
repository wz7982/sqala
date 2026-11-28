package sqala.test

import sqala.dynamic.dsl.{inlinedValue as dinlinedValue, select as dselect}
import sqala.jdbc.*
import sqala.static.dsl.*
import sqala.test.JdbcFixture.*

import java.time.{LocalDate, LocalDateTime, LocalTime, OffsetDateTime, OffsetTime}

/**
 * Regression tests for the PostgreSQL jdbc layer defects reported during
 * testing, plus goldens documenting the `standardEscapeStrings` option
 * semantics on a standard-mode server.
 */
class J7_Defects_Postgresql extends munit.FunSuite:
    private def db: JdbcContext =
        pgContext

    override def beforeAll(): Unit =
        runPgDdl(pgDdl*)
        db.execute(sql"delete from j_time")
        db.insert(JTime(
            LocalDate.of(2026, 2, 1), OffsetDateTime.parse("2026-02-01T09:30:00+08:00"),
            LocalDateTime.of(2026, 2, 1, 9, 30), OffsetTime.parse("09:30:00+08:00"), LocalTime.of(9, 30)
        ))

    test("issue A: date plus day interval should evaluate"):
        val rows =
            db.fetch(from(JTime).map(t => (d = t.dt + 3.day))).asInstanceOf[List[(d: LocalDate)]]
        assertEquals(rows.map(_.d), List(LocalDate.of(2026, 2, 4)))

    test("issue B: time plus interval should decode as OffsetTime"):
        val rows =
            db.fetch(from(JTime).map(t => (x = t.tm + 2.hour))).asInstanceOf[List[(x: OffsetTime)]]
        assertEquals(rows.map(_.x), List(OffsetTime.parse("11:30:00+08:00")))

    test("issue C: date difference interval projection reports a clean compile error"):
        val errors = compileErrors("db.fetch(from(JTime).map(t => (iv = t.dt - t.dt)))")
        assert(errors.nonEmpty)
        assert(!errors.contains("NoSuchElementException"))

    test("golden: the non-standard escape context doubles backslashes, which a standard-mode server reads literally"):
        val rows = db.fetchTo[String](dselect(dinlinedValue("a\\b").as("v")))
        assertEquals(rows, List("a\\\\b"))

    test("golden: the standard escape context round-trips backslashes"):
        val rows = pgContextEscape.fetchTo[String](dselect(dinlinedValue("a\\b").as("v")))
        assertEquals(rows, List("a\\b"))


/**
 * Interval arithmetic golden behavior on MySQL, where the same static
 * expressions evaluate correctly.
 */
class J7_MysqlIntervalGolden extends munit.FunSuite:
    private def db: JdbcContext =
        mysqlContext

    override def beforeAll(): Unit =
        runMysqlDdl(mysqlDdl*)
        db.execute(sql"delete from j_time")
        db.insert(JTime(
            LocalDate.of(2026, 2, 1), OffsetDateTime.parse("2026-02-01T09:30:00+08:00"),
            LocalDateTime.of(2026, 2, 1, 9, 30), OffsetTime.parse("09:30:00+08:00"), LocalTime.of(9, 30)
        ))

    test("date plus day interval evaluates on mysql"):
        val rows =
            db.fetch(from(JTime).map(t => (d = t.dt + 3.day))).asInstanceOf[List[(d: LocalDate)]]
        assertEquals(rows.map(_.d), List(LocalDate.of(2026, 2, 4)))
