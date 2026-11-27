package sqala.test

import sqala.jdbc.*
import sqala.test.JdbcFixture.*

import java.sql.SQLException

/**
 * I6 binding-path hardening for NativeSql: concatenation must keep the
 * argument order aligned with the placeholder order, spliced raw text that
 * contains a bare `?` must fail fast at bind time instead of shifting
 * parameters silently, and interpolated values must never leak into the SQL
 * text.
 */
abstract class J8_NativeSql_BindingBase extends munit.FunSuite:
    def db: JdbcContext

    def reset(): Unit

    override def beforeAll(): Unit =
        reset()
        db.insertBatch(List(JUser(1, "Alice", 30), JUser(2, "Bob%li", 25)))

    test("interleaved seq and single arguments stay aligned across concatenation"):
        val ids = List("Alice", "Bob%li")
        val a = sql"select name from j_user where id = ${1}"
        val b = sql" and name in $ids"
        val c = sql" and age >= ${20}"
        val joined = (a + b) + c

        assertEquals(joined.sql, "select name from j_user where id = ? and name in (?, ?) and age >= ?")
        assertEquals(joined.args.map(_.value).toList, List(1, "Alice", "Bob%li", 20))
        assertEquals(db.fetchTo[String](joined), List("Alice"))

        assertEquals((a + (b + c)).sql, joined.sql)
        assertEquals((a + (b + c)).args.map(_.value).toList, joined.args.map(_.value).toList)

    test("a bare question mark spliced by string concatenation fails fast"):
        val shifted = sql"select name from j_user where name = ${"Alice"}" + " or ? = ?"
        assertEquals(shifted.sql.count(_ == '?'), 3)
        assertEquals(shifted.args.length, 1)
        intercept[SQLException](db.fetchTo[String](shifted))

    test("interpolated values never appear in the sql text"):
        val payloads = List("Alice' OR '1'='1", "'; DROP TABLE j_user; --", "\\' OR 1=1 --", "x\"\"y", "%_", "\\N")
        for payload <- payloads do
            val q = sql"select name from j_user where name = $payload"
            assertEquals(q.sql, "select name from j_user where name = ?")
            assert(!q.sql.contains(payload), s"payload leaked into sql text: $payload")
            assertEquals(q.args.map(_.value).toList, List(payload))
            assertEquals(db.fetchTo[String](q), Nil)

        assertEquals(db.fetchTo[String](sql"select name from j_user where name = ${""}"), Nil)

    test("NativeSql.build expands Left and Right in order"):
        val n = NativeSql.build(
            StringContext("a ", " b ", " c "),
            List(Right(Seq(JdbcArg(1), JdbcArg(2))), Left(JdbcArg(3)))
        )
        assertEquals(n.sql, "a (?, ?) b ? c ")
        assertEquals(n.args.map(_.value).toList, List(1, 2, 3))

    test("an expanded empty seq is rejected before any sql is produced"):
        val ids: List[Int] = List.empty
        intercept[IllegalArgumentException](sql"select name from j_user where id in $ids")

class J8_NativeSql_Binding_Postgresql extends J8_NativeSql_BindingBase:
    override def db: JdbcContext = pgContext
    override def reset(): Unit = runPgDdl(userDdl*)

class J8_NativeSql_Binding_Mysql extends J8_NativeSql_BindingBase:
    override def db: JdbcContext = mysqlContext
    override def reset(): Unit = runMysqlDdl(userDdl*)
