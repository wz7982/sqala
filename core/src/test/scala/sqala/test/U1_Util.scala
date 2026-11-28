package sqala.test

import sqala.ast.expr.SqlExpr
import sqala.ast.statement.SqlStatement
import sqala.dialect.{MysqlDialect, PostgresqlDialect, SqlserverDialect}
import sqala.util.*

class U1_Util extends munit.FunSuite:
    test("NonEmptyList basics"):
        val xs = NonEmptyList(1, List(2, 3))
        assertEquals(xs.toList, List(1, 2, 3))
        assertEquals(xs.size, 3)
        assertEquals(xs.map(_ * 2).toList, List(2, 4, 6))
        assertEquals((0 :: xs).toList, List(0, 1, 2, 3))
        assertEquals((xs :+ 4).toList, List(1, 2, 3, 4))
        assertEquals((xs ++ NonEmptyList(4, List(5))).toList, List(1, 2, 3, 4, 5))
        assertEquals((xs ++ Seq(4, 5)).toList, List(1, 2, 3, 4, 5))
        assertEquals(xs.indices.toList, List(0, 1, 2))
        assertEquals(xs.zipWithIndex.toList, List((1, 0), (2, 1), (3, 2)))

    test("NonEmptyList apply from another non-empty list"):
        assertEquals(NonEmptyList(1, NonEmptyList(2, List(3))).toList, List(1, 2, 3))

    test("NonEmptyList single element"):
        val xs = NonEmptyList(1, Nil)
        assertEquals(xs.size, 1)
        assertEquals(xs.indices.toList, List(0))
        assertEquals(xs.zipWithIndex.toList, List((1, 0)))
        assertEquals(xs.zip(NonEmptyList("a", Nil)).toList, List((1, "a")))

    test("NonEmptyList zip truncates to the shorter tail"):
        val xs = NonEmptyList(1, List(2, 3))
        val ys = NonEmptyList("a", Nil)
        assertEquals(xs.zip(ys).toList, List((1, "a")))
        assertEquals(ys.zip(xs).toList, List(("a", 1)))

    test("NonEmptyList covariance"):
        val xs: NonEmptyList[Any] = NonEmptyList(1, List("a"))
        assertEquals(xs.toList, List(1, "a"))

    test("camelToSnake"):
        assertEquals(camelToSnake("userId"), "user_id")
        assertEquals(camelToSnake("id"), "id")
        assertEquals(camelToSnake(""), "")
        assertEquals(camelToSnake("userNameId"), "user_name_id")

    test("camelToSnake splits every uppercase letter in an acronym"):
        assertEquals(camelToSnake("userID"), "user_i_d")
        assertEquals(camelToSnake("UserId"), "user_id")

    test("camelToSnake lowercases without splitting digits"):
        assertEquals(camelToSnake("A1B2"), "a1_b2")
        assertEquals(camelToSnake("already_snake"), "already_snake")

    test("pipe operator"):
        assertEquals(1 |> (_ + 1), 2)
        assertEquals("a" |> (_.toUpperCase), "A")

    test("queryToString dispatches to the dialect printer"):
        val sql = queryToString(QueryFixture.simple, PostgresqlDialect, true)
        assertEquals(sql, "SELECT\n    \"a\"\nFROM\n    \"t\"")
        assertEquals(
            queryToString(QueryFixture.simple, MysqlDialect, true),
            "SELECT\n    `a`\nFROM\n    `t`"
        )

    test("statementToString dispatches to the dialect printer"):
        val delete = SqlStatement.Delete(QueryFixture.table("t"), None)
        assertEquals(statementToString(delete, SqlserverDialect, true), "DELETE FROM [t]")

    test("queryToString honors standardEscapeStrings"):
        val q = QueryFixture.select(
          QueryFixture.nel(QueryFixture.item(SqlExpr.StringLiteral("a\\b")))
        )
        assert(queryToString(q, MysqlDialect, true).contains("'a\\b'"))
        assert(queryToString(q, MysqlDialect, false).contains("'a\\\\b'"))