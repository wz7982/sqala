package sqala.test

import sqala.dialect.PostgresqlDialect
import sqala.jdbc.*
import sqala.metadata.primaryKey
import sqala.test.JdbcFixture.*

case class JRepoUser(@primaryKey id: Int, name: String, age: Int)

case class JRepoNode(@primaryKey id: Int, parentId: Option[Int], name: String)

case class JRepoTag(label: String)

case class JRepoMemo(@primaryKey id: Int, memo: Option[String])

case class JRepoOverlap(
    @primaryKey id: Int,
    name: String,
    nameLike: String,
    age: Int,
    ageIn: Int,
    ageDesc: Int,
    ageInList: Int
)

/**
 * Repository method-name parsing: SQL goldens for the contracted name
 * grammar, argument and result type refinement, compile-time rejection
 * of unknown, ambiguous and unsupported names, and the `Dynamic` call
 * shape on `JdbcContext`.
 */
class J10_Repository_Parse extends munit.FunSuite:
    private def sql(q: sqala.static.dsl.statement.query.Query[?, ?, ?, ?]): String =
        q.sql(PostgresqlDialect)

    test("equality conditions and an order tail print as a filtered select"):
        assertEquals(
            sql(Repository.derived[JRepoUser, "fetchByAgeAndNameOrderByAgeDesc"].query((30, "Alice"))),
            """SELECT
              |    "t1"."id" AS "c1",
              |    "t1"."name" AS "c2",
              |    "t1"."age" AS "c3"
              |FROM
              |    "j_repo_user" AS "t1"
              |WHERE
              |    "t1"."age" = 30 AND "t1"."name" = 'Alice'
              |ORDER BY
              |    "t1"."age" DESC""".stripMargin
        )

    test("And binds tighter than Or"):
        assertEquals(
            sql(Repository.derived[JRepoUser, "findByAgeGreaterThanAndNameLikeOrAgeLessThan"].query((30, "%a%", 10))),
            """SELECT
              |    "t1"."id" AS "c1",
              |    "t1"."name" AS "c2",
              |    "t1"."age" AS "c3"
              |FROM
              |    "j_repo_user" AS "t1"
              |WHERE
              |    "t1"."age" > 30 AND "t1"."name" LIKE '%a%' OR "t1"."age" < 10""".stripMargin
        )

    test("Or chains stay left associative"):
        assertEquals(
            sql(Repository.derived[JRepoUser, "fetchByAgeOrNameOrId"].query((1, "x", 2))).endsWith(
                """("t1"."age" = 1 OR "t1"."name" = 'x') OR "t1"."id" = 2"""
            ),
            true
        )

    test("Option parameters skip their condition when None"):
        assertEquals(
            sql(Repository.derived[JRepoNode, "fetchByParentIdAndNameLike"].query((None, "%a%"))),
            """SELECT
              |    "t1"."id" AS "c1",
              |    "t1"."parent_id" AS "c2",
              |    "t1"."name" AS "c3"
              |FROM
              |    "j_repo_node" AS "t1"
              |WHERE
              |    "t1"."name" LIKE '%a%'""".stripMargin
        )
        assertEquals(
            sql(Repository.derived[JRepoNode, "fetchByParentIdIn"].query(None)).contains("WHERE"),
            false
        )

    test("empty In prints FALSE and empty NotIn prints TRUE"):
        assert(sql(Repository.derived[JRepoUser, "countByAgeIn"].query(Seq.empty)).endsWith("WHERE\n    FALSE"))
        assert(sql(Repository.derived[JRepoUser, "countByAgeNotIn"].query(Seq.empty)).endsWith("WHERE\n    TRUE"))

    test("null tests take no arguments"):
        assertEquals(
            sql(Repository.derived[JRepoNode, "fetchByParentIdIsNullOrderByName"].query(())),
            """SELECT
              |    "t1"."id" AS "c1",
              |    "t1"."parent_id" AS "c2",
              |    "t1"."name" AS "c3"
              |FROM
              |    "j_repo_node" AS "t1"
              |WHERE
              |    "t1"."parent_id" IS NULL
              |ORDER BY
              |    "t1"."name" ASC""".stripMargin
        )
        assert(sql(Repository.derived[JRepoNode, "fetchByParentIdIsNotNull"].query(())).endsWith("IS NOT NULL"))

    test("Between and NotBetween take a pair"):
        assert(sql(Repository.derived[JRepoUser, "fetchByAgeBetween"].query((1, 2))).endsWith("BETWEEN 1 AND 2"))
        assert(sql(Repository.derived[JRepoUser, "fetchByAgeNotBetween"].query((1, 2))).endsWith("NOT BETWEEN 1 AND 2"))
        assert(sql(Repository.derived[JRepoNode, "fetchByParentIdBetween"].query(Some((1, 2)))).endsWith("BETWEEN 1 AND 2"))
        assertEquals(sql(Repository.derived[JRepoNode, "fetchByParentIdBetween"].query(None)).contains("WHERE"), false)

    test("Like, NotLike, StartsWith and EndsWith print pattern predicates"):
        assert(sql(Repository.derived[JRepoUser, "fetchByNameLike"].query("%a%")).endsWith("LIKE '%a%'"))
        assert(sql(Repository.derived[JRepoUser, "fetchByNameNotLike"].query("%a%")).endsWith("NOT LIKE '%a%'"))
        assert(sql(Repository.derived[JRepoUser, "fetchByNameStartsWith"].query("A")).endsWith("LIKE 'A%'"))
        assert(sql(Repository.derived[JRepoUser, "fetchByNameEndsWith"].query("e")).endsWith("LIKE '%e'"))
        assert(sql(Repository.derived[JRepoMemo, "fetchByMemoStartsWith"].query(Some("a"))).endsWith("LIKE 'a%'"))
        assertEquals(sql(Repository.derived[JRepoMemo, "fetchByMemoEndsWith"].query(None)).contains("WHERE"), false)

    test("fetchDistinctBy prints SELECT DISTINCT"):
        assert(sql(Repository.derived[JRepoUser, "fetchDistinctByAge"].query(30)).startsWith("SELECT DISTINCT"))

    test("longest column match wins in OrderBy"):
        assertEquals(
            sql(Repository.derived[JRepoOverlap, "fetchByAgeOrderByAgeDesc"].query(1)).endsWith(
                """ORDER BY
                  |    "t1"."age_desc" ASC""".stripMargin
            ),
            true
        )

    test("keywords inside column names do not split when the remainder cannot parse"):
        assert(sql(Repository.derived[JRepoOverlap, "fetchByAgeInList"].query(5)).endsWith(""""t1"."age_in_list" = 5"""))

    test("argument types mirror condition order and Option columns"):
        val single = summon[Repository[JRepoUser, "fetchByAge"]]
        summon[single.Args =:= Int]
        val pair = summon[Repository[JRepoUser, "fetchByAgeAndName"]]
        summon[pair.Args =:= (Int, String)]
        val optional = summon[Repository[JRepoNode, "fetchByParentIdAndNameLike"]]
        summon[optional.Args =:= (Option[Int], String)]
        val membership = summon[Repository[JRepoUser, "fetchByAgeIn"]]
        summon[membership.Args =:= Seq[Int]]
        val optionalMembership = summon[Repository[JRepoNode, "fetchByParentIdIn"]]
        summon[optionalMembership.Args =:= Option[Seq[Int]]]
        val range = summon[Repository[JRepoUser, "fetchByAgeBetween"]]
        summon[range.Args =:= (Int, Int)]
        val optionalRange = summon[Repository[JRepoNode, "fetchByParentIdBetween"]]
        summon[optionalRange.Args =:= Option[(Int, Int)]]
        val nullary = summon[Repository[JRepoNode, "fetchByParentIdIsNull"]]
        summon[nullary.Args =:= Unit]
        val starts = summon[Repository[JRepoUser, "fetchByNameStartsWith"]]
        summon[starts.Args =:= String]
        val optionalStarts = summon[Repository[JRepoMemo, "fetchByMemoStartsWith"]]
        summon[optionalStarts.Args =:= Option[String]]

    test("result types follow the mode prefix"):
        val fetch = summon[Repository[JRepoUser, "fetchByAge"]]
        summon[fetch.R =:= List[JRepoUser]]
        val find = summon[Repository[JRepoUser, "findByAge"]]
        summon[find.R =:= Option[JRepoUser]]
        val count = summon[Repository[JRepoUser, "countByAge"]]
        summon[count.R =:= Long]
        val exists = summon[Repository[JRepoUser, "existsByAge"]]
        summon[exists.R =:= Boolean]

    test("unknown columns are rejected"):
        assert(compileErrors("""Repository.derived[JRepoUser, "fetchByNoSuchColumn"]""").contains("Unknown column in repository method name."))
        assert(compileErrors("""Repository.derived[JRepoUser, "fetchByAgeOrderByNoSuch"]""").contains("Unknown column in repository method name."))

    test("ambiguous names are rejected"):
        assert(compileErrors("""Repository.derived[JRepoOverlap, "fetchByNameLike"]""").contains("Ambiguous repository method name."))
        assert(compileErrors("""Repository.derived[JRepoOverlap, "fetchByAgeIn"]""").contains("Ambiguous repository method name."))

    test("unsupported operations are rejected"):
        assert(compileErrors("""Repository.derived[JRepoUser, "deleteByAge"]""").contains("Unsupported repository operation."))
        assert(compileErrors("""Repository.derived[JRepoUser, "pageByAge"]""").contains("Unsupported repository operation."))
        assert(compileErrors("""Repository.derived[JRepoUser, "fetchBy"]""").contains("Unsupported repository operation."))
        assert(compileErrors("""Repository.derived[JRepoUser, "fetchByAgeLike"]""").contains("Unsupported repository operation."))
        assert(compileErrors("""Repository.derived[JRepoUser, "countDistinctByAge"]""").contains("Unsupported repository operation."))
        assert(compileErrors("""Repository.derived[JRepoUser, "findDistinctByAge"]""").contains("Unsupported repository operation."))
        assert(compileErrors("""Repository.derived[JRepoUser, "existsDistinctByAge"]""").contains("Unsupported repository operation."))
        assert(compileErrors("""Repository.derived[JRepoUser, "fetchByAgeStartsWith"]""").contains("Unsupported repository operation."))
        assert(compileErrors("""Repository.derived[JRepoUser, "fetchByAgeEndsWith"]""").contains("Unsupported repository operation."))
        assert(compileErrors("""Repository.derived[JRepoUser, "fetchByAgeAnd"]""").contains("Unknown column in repository method name."))

    test("keyword boundaries require camel case"):
        assert(compileErrors("""Repository.derived[JRepoOverlap, "fetchByAgeInlist"]""").contains("Unknown column in repository method name."))

    test("Dynamic calls compile with tupled, single and unit arguments"):
        assertEquals(compileErrors("JdbcFixture.pgContext.fetchByAgeAndName[JRepoUser](30, \"Alice\")"), "")
        assertEquals(compileErrors("JdbcFixture.pgContext.fetchByAge[JRepoUser](30)"), "")
        assertEquals(compileErrors("JdbcFixture.pgContext.fetchByParentIdIsNull[JRepoNode](())"), "")
        assertEquals(compileErrors("JdbcFixture.pgContext.fetchByNameStartsWith[JRepoUser](\"A\")"), "")
        assertEquals(compileErrors("JdbcFixture.pgContext.fetchByNameEndsWith[JRepoUser](\"e\")"), "")

    test("Dynamic calls reject missing or mistyped arguments"):
        assert(compileErrors("JdbcFixture.pgContext.fetchByAge[JRepoUser]()").nonEmpty)
        assert(compileErrors("JdbcFixture.pgContext.fetchByAge[JRepoUser](\"x\")").nonEmpty)
        assert(compileErrors("JdbcFixture.pgContext.fetchByAgeAndName[JRepoUser](30)").nonEmpty)
        assert(compileErrors("JdbcFixture.pgContext.fetchByNameStartsWith[JRepoUser](1)").nonEmpty)

/**
 * Repository execution against a live database: every mode prefix,
 * operator suffix, Option skipping, empty membership lists, ordering,
 * distinct rows and transaction participation, with values bound as
 * parameters rather than inlined.
 */
abstract class J10_RepositoryBase extends munit.FunSuite:
    def db: JdbcContext

    def reset(): Unit

    override def beforeAll(): Unit =
        reset()

    private def seed(): Unit =
        db.execute(sql"delete from j_repo_user")
        db.execute(sql"delete from j_repo_node")
        db.execute(sql"delete from j_repo_tag")
        db.execute(sql"delete from j_repo_memo")
        db.insertBatch(List(
            JRepoUser(1, "Alice", 30),
            JRepoUser(2, "Bob", 25),
            JRepoUser(3, "Carol", 35),
            JRepoUser(4, "Dan", 41),
            JRepoUser(5, "Eve", 28),
            JRepoUser(6, "O'Brien", 30)
        ))
        db.insertBatch(List(
            JRepoNode(1, None, "root"),
            JRepoNode(2, Some(1), "child"),
            JRepoNode(3, Some(1), "leaf"),
            JRepoNode(4, None, "other")
        ))
        db.insertBatch(List(JRepoTag("a"), JRepoTag("a"), JRepoTag("b")))
        db.insertBatch(List(JRepoMemo(1, Some("apple")), JRepoMemo(2, None), JRepoMemo(3, Some("banana"))))

    test("fetchBy returns matching rows in order"):
        seed()
        assertEquals(db.fetchByAgeAndNameOrderByAgeDesc[JRepoUser](30, "Alice"), List(JRepoUser(1, "Alice", 30)))
        assertEquals(db.fetchByAgeAndName[JRepoUser](30, "Nobody"), Nil)

    test("findBy returns the first row or None"):
        seed()
        assertEquals(db.findByAgeOrderByAgeAscIdAsc[JRepoUser](30), Some(JRepoUser(1, "Alice", 30)))
        assertEquals(db.findByAge[JRepoUser](99), None)

    test("countBy aggregates"):
        seed()
        assertEquals(db.countByAgeGreaterThan[JRepoUser](28), 4L)

    test("existsBy reports emptiness"):
        seed()
        assertEquals(db.existsByAgeGreaterThan[JRepoUser](40), true)
        assertEquals(db.existsByAgeGreaterThan[JRepoUser](100), false)

    test("In and NotIn filter by membership"):
        seed()
        assertEquals(db.fetchByAgeIn[JRepoUser](Seq(25, 35)).map(_.id).sorted, List(2, 3))
        assertEquals(db.fetchByAgeNotIn[JRepoUser](Seq(25, 35, 41)).map(_.id).sorted, List(1, 5, 6))

    test("empty In returns nothing and empty NotIn returns everything"):
        seed()
        assertEquals(db.fetchByAgeIn[JRepoUser](Seq.empty), Nil)
        assertEquals(db.fetchByAgeNotIn[JRepoUser](Seq.empty).map(_.id).sorted, List(1, 2, 3, 4, 5, 6))

    test("Between and NotBetween filter ranges"):
        seed()
        assertEquals(db.fetchByAgeBetween[JRepoUser](28, 35).map(_.id).sorted, List(1, 3, 5, 6))
        assertEquals(db.fetchByAgeNotBetween[JRepoUser](28, 35).map(_.id).sorted, List(2, 4))

    test("Like and NotLike match patterns with bound quotes"):
        seed()
        assertEquals(db.fetchByNameLike[JRepoUser]("%O'B%"), List(JRepoUser(6, "O'Brien", 30)))
        assertEquals(db.fetchByNameNotLike[JRepoUser]("%O'B%").map(_.id).sorted, List(1, 2, 3, 4, 5))

    test("StartsWith and EndsWith anchor the pattern"):
        seed()
        assertEquals(db.fetchByNameStartsWith[JRepoUser]("A").map(_.id), List(1))
        assertEquals(db.fetchByNameEndsWith[JRepoUser]("e").map(_.id).sorted, List(1, 5))
        assertEquals(db.fetchByNameStartsWith[JRepoUser]("O'B").map(_.id), List(6))

    test("Option string columns support the Like family and skip when None"):
        seed()
        assertEquals(db.fetchByMemoStartsWith[JRepoMemo](Some("a")).map(_.id), List(1))
        assertEquals(db.fetchByMemoEndsWith[JRepoMemo](Some("a")).map(_.id), List(3))
        assertEquals(db.fetchByMemoLike[JRepoMemo](Some("%an%")).map(_.id), List(3))
        assertEquals(db.fetchByMemoStartsWith[JRepoMemo](None).map(_.id).sorted, List(1, 2, 3))

    test("Option arguments skip conditions when None"):
        seed()
        assertEquals(db.fetchByParentIdAndNameLike[JRepoNode](Some(1), "%o%").map(_.id).sorted, Nil)
        assertEquals(db.fetchByParentIdAndNameLike[JRepoNode](None, "%o%").map(_.id).sorted, List(1, 4))
        assertEquals(db.fetchByParentIdIn[JRepoNode](None).map(_.id).sorted, List(1, 2, 3, 4))

    test("IsNull and IsNotNull partition rows"):
        seed()
        assertEquals(db.fetchByParentIdIsNullOrderByName[JRepoNode](()).map(_.id), List(4, 1))
        assertEquals(db.fetchByParentIdIsNotNull[JRepoNode](()).map(_.id).sorted, List(2, 3))

    test("OrderBy sorts by multiple columns and directions"):
        seed()
        assertEquals(db.fetchByAgeGreaterThanOrderByAgeDescNameAsc[JRepoUser](25).map(_.id), List(4, 3, 1, 6, 5))

    test("comparison suffixes map to operators"):
        seed()
        assertEquals(db.fetchByAgeNot[JRepoUser](30).map(_.id).sorted, List(2, 3, 4, 5))
        assertEquals(db.fetchByAgeGreaterThanEqual[JRepoUser](35).map(_.id).sorted, List(3, 4))
        assertEquals(db.fetchByAgeLessThanEqual[JRepoUser](28).map(_.id).sorted, List(2, 5))

    test("fetchDistinctBy deduplicates whole rows"):
        seed()
        assertEquals(db.fetchDistinctByLabel[JRepoTag]("a"), List(JRepoTag("a")))

    test("Dynamic queries join the surrounding transaction"):
        seed()
        val inside = db.executeTransaction:
            db.insert(JRepoUser(7, "Zed", 50))
            db.fetchByAge[JRepoUser](50)
        assertEquals(inside, List(JRepoUser(7, "Zed", 50)))
        assertEquals(db.fetchByAge[JRepoUser](50), List(JRepoUser(7, "Zed", 50)))


class J10_Repository_Postgresql extends J10_RepositoryBase:
    override def db: JdbcContext =
        pgContext

    override def reset(): Unit =
        runPgDdl(
            "drop table if exists j_repo_user",
            "create table j_repo_user(id int primary key, name varchar(64) not null, age int not null)",
            "drop table if exists j_repo_node",
            "create table j_repo_node(id int primary key, parent_id int, name varchar(64) not null)",
            "drop table if exists j_repo_tag",
            "create table j_repo_tag(label varchar(64) not null)",
            "drop table if exists j_repo_memo",
            "create table j_repo_memo(id int primary key, memo varchar(64))"
        )


class J10_Repository_Mysql extends J10_RepositoryBase:
    override def db: JdbcContext =
        mysqlContext

    override def reset(): Unit =
        runMysqlDdl(
            "drop table if exists j_repo_user",
            "create table j_repo_user(id int primary key, name varchar(64) not null, age int not null)",
            "drop table if exists j_repo_node",
            "create table j_repo_node(id int primary key, parent_id int, name varchar(64) not null)",
            "drop table if exists j_repo_tag",
            "create table j_repo_tag(label varchar(64) not null)",
            "drop table if exists j_repo_memo",
            "create table j_repo_memo(id int primary key, memo varchar(64))"
        )