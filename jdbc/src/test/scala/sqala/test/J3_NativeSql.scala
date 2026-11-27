package sqala.test

import sqala.jdbc.*
import sqala.test.JdbcFixture.*

import java.time.LocalDate

/**
 * Native SQL interpolator: placeholder rendering, argument collection,
 * sequence expansion, concatenation, and the native execution entry points.
 */
abstract class J3_NativeSqlBase extends munit.FunSuite:
    def db: JdbcContext

    def reset(): Unit

    override def beforeAll(): Unit =
        reset()
        db.insertBatch(List(JUser(1, "Alice", 30), JUser(2, "Bob%li", 25)))

    test("interpolator renders placeholders and collects args"):
        val q = sql"select name from j_user where age > ${20} and name = ${"Alice"}"
        assertEquals(q.sql, "select name from j_user where age > ? and name = ?")
        assertEquals(q.args.map(_.value).toList, List(20, "Alice"))

    test("seq argument expands to a placeholder list"):
        val ids = List(1, 2)
        val q = sql"select name from j_user where id in $ids"
        assertEquals(q.sql, "select name from j_user where id in (?, ?)")
        assertEquals(q.args.map(_.value).toList, List(1, 2))
        assertEquals(db.fetchTo[String](q).sorted, List("Alice", "Bob%li"))

    test("empty seq is rejected"):
        val ids: List[Int] = List.empty
        intercept[IllegalArgumentException](sql"select name from j_user where id in $ids")

    test("concatenation appends raw sql and native fragments"):
        val base = sql"select name from j_user where age > ${20}"
        assertEquals((base + " order by name").sql, "select name from j_user where age > ? order by name")
        val joined = base + sql" and name = ${"Bob%li"}"
        assertEquals(joined.args.map(_.value).toList, List(20, "Bob%li"))
        assertEquals(db.fetchTo[String](joined), List("Bob%li"))

    test("percent characters stay inside the bound parameter"):
        val like = "%Bob%"
        val q = sql"select name from j_user where name like $like"
        assertEquals(q.sql, "select name from j_user where name like ?")
        assertEquals(db.fetchTo[String](q), List("Bob%li"))

    test("java.time values bind as parameters"):
        val when = LocalDate.of(2026, 2, 1)
        val q = sql"select name from j_user where name = ${"Alice"} and $when = $when"
        assertEquals(q.args.map(_.value).toList, List("Alice", when, when))
        assertEquals(db.fetchTo[String](q), List("Alice"))

    test("execute returns the affected row count"):
        assertEquals(db.execute(sql"update j_user set age = ${44} where id = ${2}"), 1)
        assertEquals(db.fetchTo[Int](sql"select age from j_user where id = ${2}"), List(44))

    test("fetchToMap returns aliased native columns"):
        val maps = db.fetchToMap(sql"select id as uid, name as uname from j_user where age > ${20} order by uid")
        assertEquals(maps, List(Map("uid" -> 1, "uname" -> "Alice"), Map("uid" -> 2, "uname" -> "Bob%li")))

    test("native sql inside a transaction shares the connection"):
        db.deleteByPrimaryKey[JUser](3)
        db.executeTransaction:
            db.execute(sql"insert into j_user values (${3}, ${"Cy"}, ${33})")
            assertEquals(db.fetchTo[String](sql"select name from j_user where id = ${3}"), List("Cy"))
        assertEquals(db.findByPrimaryKey[JUser](3), Some(JUser(3, "Cy", 33)))


class J3_NativeSql_Postgresql extends J3_NativeSqlBase:
    override def db: JdbcContext =
        pgContext

    override def reset(): Unit =
        runPgDdl(userDdl*)


class J3_NativeSql_Mysql extends J3_NativeSqlBase:
    override def db: JdbcContext =
        mysqlContext

    override def reset(): Unit =
        runMysqlDdl(userDdl*)
