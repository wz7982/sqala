package sqala.test

import sqala.jdbc.*
import sqala.static.dsl.*
import sqala.test.JdbcFixture.*

/**
 * Query execution: `fetch`, `fetchTo`, `find`, `findTo`, `fetchCount`,
 * `fetchExists`, result shapes for projections, joins, groupings and set
 * operations, plus `page` and `cursorFetch`.
 */
abstract class J2_QueryBase extends munit.FunSuite:
    def db: JdbcContext

    def reset(): Unit

    override def beforeAll(): Unit =
        reset()

    private def seedUsers(): Unit =
        db.execute(sql"delete from j_user")
        db.insertBatch(List(JUser(1, "Alice", 30), JUser(2, "Bob", 25), JUser(3, "Carol", 35), JUser(4, "Dan", 41), JUser(5, "Eve", 28)))

    test("fetch maps a named projection"):
        seedUsers()
        val rows: List[(name: String, grown: Boolean)] =
            db.fetch(from(JUser).filter(u => u.age > 20).map(u => (name = u.name, grown = u.age > 30)))
        assertEquals(rows.sortBy(_.name), List((name = "Alice", grown = false), (name = "Bob", grown = false), (name = "Carol", grown = true), (name = "Dan", grown = true), (name = "Eve", grown = false)))

    test("fetch returns whole entities"):
        seedUsers()
        assertEquals(db.fetch(from(JUser).filter(u => u.id === 2)), List(JUser(2, "Bob", 25)))

    test("fetch unwraps a single unnamed column"):
        seedUsers()
        assertEquals(db.fetch(from(JUser).filter(u => u.age >= 35).map(u => u.name).sortBy(u => u.name)), List("Carol", "Dan"))

    test("fetchTo decodes tuples with an explicit decoder"):
        seedUsers()
        assertEquals(db.fetchTo[(String, Int)](from(JUser).filter(u => u.id <= 2).map(u => (u.name, u.id)).sortBy(u => u.id)), List(("Alice", 1), ("Bob", 2)))

    test("fetchTo decodes into a product type"):
        seedUsers()
        case class Light(name: String, age: Int)
        assertEquals(
            db.fetchTo[Light](from(JUser).filter(u => u.id === 1).map(u => (u.name, u.age))),
            List(Light("Alice", 30))
        )

    test("fetch keeps optional columns in Option"):
        db.execute(sql"delete from j_auto")
        db.insert(JAuto(0, "with memo", Some("keep me")))
        db.insert(JAuto(0, "without memo", None))
        val rows = db.fetch(from(JAuto).sortBy(a => a.name))
        assertEquals(rows.map(_.memo), List(Some("keep me"), None))
        assertEquals(db.fetchTo[(String, Option[String])](from(JAuto).filter(a => a.name === "without memo").map(a => (a.name, a.memo))), List(("without memo", None)))

    test("find returns the first row and empty results"):
        seedUsers()
        assertEquals(db.find(from(JUser).filter(u => u.age > 20).map(u => (n = u.name)).sortBy(u => u.name)), Some((n = "Alice")))
        assertEquals(db.find(from(JUser).filter(u => u.age > 100)), None)
        assertEquals(db.findTo[JUser](from(JUser).filter(u => u.id === 3)), Some(JUser(3, "Carol", 35)))
        assertEquals(db.findTo[JUser](from(JUser).filter(u => u.id === 99)), None)

    test("fetchCount and fetchExists read query size and emptiness"):
        seedUsers()
        assertEquals(db.fetchCount(from(JUser)), 5L)
        assertEquals(db.fetchCount(from(JUser).filter(u => u.age > 30)), 2L)
        assertEquals(db.fetchExists(from(JUser).filter(u => u.age > 40)), true)
        assertEquals(db.fetchExists(from(JUser).filter(u => u.age > 100)), false)

    test("limit and offset slice the rows"):
        seedUsers()
        assertEquals(db.fetch(from(JUser).sortBy(u => u.id).drop(1).take(2)).map(_.id), List(2, 3))

    test("group by counts rows per key"):
        seedUsers()
        db.execute(sql"delete from j_key")
        db.execute(sql"insert into j_key values (1, 10, 'a'), (1, 11, 'b'), (2, 20, 'c')")
        assertEquals(
            db.fetchTo[(Int, Int)](from(JKey).groupBy(k => k.src).map((g, _) => (g, count()))).sortBy(_._1),
            List((1, 2), (2, 1))
        )

    test("mapDistinct removes duplicate projections"):
        seedUsers()
        db.execute(sql"delete from j_key")
        db.execute(sql"insert into j_key values (1, 10, 'x'), (1, 11, 'x'), (2, 20, 'y')")
        assertEquals(db.fetch(from(JKey).mapDistinct(k => k.label)).sorted, List("x", "y"))

    test("set operations combine row lists"):
        seedUsers()
        assertEquals(
            db.fetch(from(JUser).filter(u => u.id <= 2).map(u => u.id).union(from(JUser).filter(u => u.id >= 4).map(u => u.id))).sorted,
            List(1, 2, 4, 5)
        )

    test("subquery existence filters rows"):
        seedUsers()
        db.execute(sql"delete from j_key")
        db.execute(sql"insert into j_key values (1, 10, 'a'), (3, 30, 'b')")
        assertEquals(
            db.fetch(from(JUser).filter(u => exists(from(JKey).filter(k => k.src === u.id).map(k => k.src))).map(u => u.name)).sorted,
            List("Alice", "Carol")
        )

    test("joins combine both tables"):
        seedUsers()
        db.execute(sql"delete from j_key")
        db.execute(sql"insert into j_key values (1, 10, 'a'), (2, 20, 'b')")
        val rows: List[(name: String, label: String)] =
            db.fetch(from(JUser.join(JKey).on((u, k) => u.id === k.src)).map((u, k) => (name = u.name, label = k.label)))
        assertEquals(rows.sortBy(_.name), List((name = "Alice", label = "a"), (name = "Bob", label = "b")))

    test("size returns the row count as a query"):
        seedUsers()
        assertEquals(db.fetch(from(JUser).size), List(5L))

    test("page slices data and reports counts"):
        seedUsers()
        val page = db.page(from(JUser).sortBy(u => u.id), 2, 2)
        assertEquals(page.pageSize, 2)
        assertEquals(page.pageNo, 2)
        assertEquals(page.querySize, 5L)
        assertEquals(page.pageTotal, 3)
        assertEquals(page.data.map(_.id), List(3, 4))

    test("page skips the count query when disabled"):
        seedUsers()
        val page = db.pageTo[JUser](from(JUser).sortBy(u => u.id), 2, 3, returnCount = false)
        assertEquals(page.querySize, 0L)
        assertEquals(page.pageTotal, 0)
        assertEquals(page.data.map(_.id), List(5))

    test("page with zero size returns no data"):
        seedUsers()
        val page = db.pageTo[JUser](from(JUser), 0, 1)
        assertEquals(page.data, Nil)
        assertEquals(page.querySize, 5L)
        assertEquals(page.pageTotal, 0)

    test("page maps its data"):
        seedUsers()
        assertEquals(db.page(from(JUser).sortBy(u => u.id), 2, 1).map(u => u.name).data, List("Alice", "Bob"))

    test("cursorFetch streams batches of the fetch size"):
        seedUsers()
        var batches = List.empty[(Int, List[Int])]
        db.cursorFetch(from(JUser).sortBy(u => u.id), 2)(c =>
            batches = batches :+ (c.batchNo, c.data.map(_.id))
        )
        assertEquals(batches, List((1, List(1, 2)), (2, List(3, 4)), (3, List(5))))

    test("dynamic queries fetch typed rows"):
        import sqala.dynamic.dsl.{column as dcolumn, select as dselect, table as dtable, value as dvalue}
        seedUsers()
        val q = dselect(dcolumn("id").as("id"), dcolumn("name").as("name")).from(dtable("j_user")).where(dcolumn("age") > dvalue(30)).orderBy(dcolumn("id").asc)
        assertEquals(db.fetchTo[(Int, String)](q), List((3, "Carol"), (4, "Dan")))

    test("dynamic queries fetch maps"):
        import sqala.dynamic.dsl.{column as dcolumn, select as dselect, table as dtable, value as dvalue}
        seedUsers()
        val q = dselect(dcolumn("name").as("name")).from(dtable("j_user")).where(dcolumn("id") === dvalue(2))
        assertEquals(db.fetchToMap(q), List(Map("name" -> "Bob")))


class J2_Query_Postgresql extends J2_QueryBase:
    override def db: JdbcContext =
        pgContext

    override def reset(): Unit =
        runPgDdl(pgDdl*)


class J2_Query_Mysql extends J2_QueryBase:
    override def db: JdbcContext =
        mysqlContext

    override def reset(): Unit =
        runMysqlDdl(mysqlDdl*)
