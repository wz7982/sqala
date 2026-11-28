package sqala.test

import sqala.jdbc.*
import sqala.static.dsl.*
import sqala.test.JdbcFixture.*

/**
 * JDBC DML execution: `insert`, `insertBatch`, `insertAndReturn`,
 * `update` with `skipNone`, `save`, primary-key deletes and finds, and
 * the static `insert`/`update`/`delete` statements through `execute`.
 */
abstract class J1_DmlBase extends munit.FunSuite:
    def db: JdbcContext

    def reset(): Unit

    /**
     * Rows affected by an upsert that hits an existing key. MySQL reports
     * two affected rows for the update branch of `ON DUPLICATE KEY UPDATE`.
     */
    def saveUpdateCount: Int

    override def beforeAll(): Unit =
        reset()

    test("insert writes the entity row"):
        db.execute(sql"delete from j_user")
        assertEquals(db.insert(JUser(1, "Alice", 30)), 1)
        assertEquals(db.fetch(from(JUser)), List(JUser(1, "Alice", 30)))

    test("insertBatch writes every row"):
        db.execute(sql"delete from j_user")
        assertEquals(db.insertBatch(List(JUser(2, "Bob", 25), JUser(3, "Carol", 35))), 2)
        assertEquals(db.fetch(from(JUser).sortBy(u => u.id)), List(JUser(2, "Bob", 25), JUser(3, "Carol", 35)))

    test("insert excludes the auto-increment column"):
        db.execute(sql"delete from j_auto")
        assertEquals(db.insert(JAuto(0, "a", Some("m"))), 1)
        assertEquals(db.fetch(from(JAuto)).map(_.name), List("a"))

    test("insertAndReturn populates the generated key"):
        db.execute(sql"delete from j_auto")
        val first = db.insertAndReturn(JAuto(0, "x", None))
        val second = db.insertAndReturn(JAuto(0, "y", None))
        assert(first.id > 0, s"generated id was ${first.id}")
        assertEquals(second.id, first.id + 1)
        assertEquals(second.memo, None)

    test("update writes every field by primary key"):
        db.execute(sql"delete from j_user")
        db.insert(JUser(1, "Alice", 30))
        assertEquals(db.update(JUser(1, "Alicia", 31)), 1)
        assertEquals(db.findByPrimaryKey[JUser](1), Some(JUser(1, "Alicia", 31)))

    test("update by primary key reports zero when the row is missing"):
        db.execute(sql"delete from j_user")
        db.insert(JUser(1, "Alice", 30))
        assertEquals(db.update(JUser(99, "Nobody", 1)), 0)

    test("update skipNone omits None fields"):
        db.execute(sql"delete from j_auto")
        db.insert(JAuto(0, "keep", Some("old memo")))
        val id = db.fetch(from(JAuto)).head.id
        assertEquals(db.update(JAuto(id, "renamed", None), skipNone = true), 1)
        assertEquals(db.findByPrimaryKey[JAuto](id), Some(JAuto(id, "renamed", Some("old memo"))))

    test("update writes nulls when skipNone is false"):
        db.execute(sql"delete from j_auto")
        db.insert(JAuto(0, "keep", Some("old memo")))
        val id = db.fetch(from(JAuto)).head.id
        assertEquals(db.update(JAuto(id, "renamed", None)), 1)
        assertEquals(db.findByPrimaryKey[JAuto](id), Some(JAuto(id, "renamed", None)))

    test("save inserts a missing key and updates an existing one"):
        db.execute(sql"delete from j_key")
        assertEquals(db.save(JKey(1, 2, "first")), 1)
        assertEquals(db.save(JKey(1, 2, "second")), saveUpdateCount)
        assertEquals(db.fetch(from(JKey)), List(JKey(1, 2, "second")))

    test("deleteByPrimaryKey removes the row"):
        db.execute(sql"delete from j_user")
        db.insertBatch(List(JUser(1, "A", 1), JUser(2, "B", 2)))
        assertEquals(db.deleteByPrimaryKey[JUser](1), 1)
        assertEquals(db.fetch(from(JUser)), List(JUser(2, "B", 2)))

    test("deleteByPrimaryKeys removes composite keys"):
        db.execute(sql"delete from j_key")
        db.insertBatch(List(JKey(1, 10, "a"), JKey(2, 20, "b"), JKey(3, 30, "c")))
        assertEquals(db.deleteByPrimaryKeys[JKey](List((1, 10), (3, 30))), 2)
        assertEquals(db.fetch(from(JKey)), List(JKey(2, 20, "b")))

    test("findByPrimaryKey and fetchByPrimaryKeys read rows"):
        db.execute(sql"delete from j_user")
        db.insertBatch(List(JUser(1, "A", 1), JUser(2, "B", 2), JUser(3, "C", 3)))
        assertEquals(db.findByPrimaryKey[JUser](2), Some(JUser(2, "B", 2)))
        assertEquals(db.findByPrimaryKey[JUser](9), None)
        assertEquals(db.fetchByPrimaryKeys[JUser](List(1, 3, 9)).sortBy(_.id), List(JUser(1, "A", 1), JUser(3, "C", 3)))

    test("execute runs a static insert statement"):
        db.execute(sql"delete from j_user")
        assertEquals(db.execute(insert[JUser](u => (u.id, u.name, u.age)).values(List((1, "Alice", 30), (2, "Bob", 25)))), 2)
        assertEquals(db.fetch(from(JUser).sortBy(u => u.id)).map(_.name), List("Alice", "Bob"))

    test("execute runs a static update statement"):
        db.execute(sql"delete from j_user")
        db.insert(JUser(1, "Alice", 30))
        assertEquals(db.execute(update[JUser].set(u => u.age := 31).set(u => u.name := "Alicia").where(u => u.id === 1)), 1)
        assertEquals(db.findByPrimaryKey[JUser](1), Some(JUser(1, "Alicia", 31)))

    test("execute runs a static delete statement"):
        db.execute(sql"delete from j_user")
        db.insertBatch(List(JUser(1, "A", 10), JUser(2, "B", 20)))
        assertEquals(db.execute(delete[JUser].where(u => u.age >= 20)), 1)
        assertEquals(db.fetch(from(JUser)), List(JUser(1, "A", 10)))

    test("execute runs an insert backed by a query"):
        db.execute(sql"delete from j_user")
        db.execute(sql"delete from j_auto")
        db.insertBatch(List(JUser(1, "Alice", 30), JUser(2, "Bob", 25)))
        assertEquals(db.execute(insert[JAuto](a => (a.name, a.memo)).query(from(JUser).map(u => (u.name, Some("copied"): Option[String])))), 2)
        assertEquals(db.fetch(from(JAuto).mapDistinct(a => a.name)).sorted, List("Alice", "Bob"))

    test("inserting a duplicate primary key fails"):
        db.execute(sql"delete from j_user")
        db.insert(JUser(1, "Alice", 30))
        intercept[java.sql.SQLException] {
            db.insert(JUser(1, "Again", 30))
        }
        assertEquals(db.fetch(from(JUser)).map(_.name), List("Alice"))

    test("insertBatch rejects an empty sequence"):
        intercept[IllegalArgumentException] {
            db.insertBatch(List.empty[JUser])
        }


class J1_Dml_Postgresql extends J1_DmlBase:
    override def db: JdbcContext =
        pgContext

    override def reset(): Unit =
        runPgDdl(pgDdl*)

    override def saveUpdateCount: Int =
        1


class J1_Dml_Mysql extends J1_DmlBase:
    override def db: JdbcContext =
        mysqlContext

    override def reset(): Unit =
        runMysqlDdl(mysqlDdl*)

    override def saveUpdateCount: Int =
        2
