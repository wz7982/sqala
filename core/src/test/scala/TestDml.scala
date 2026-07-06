import sqala.ast.expr.SqlExpr
import sqala.ast.statement.*
import sqala.ast.table.SqlTable
import sqala.util.NonEmptyList

class TestDml extends munit.FunSuite:
    test("insert mode"):
        val cases: List[(SqlInsertMode, String)] = List(
            SqlInsertMode.Values(NonEmptyList(NonEmptyList(SqlExpr.NumberLiteral(1), Nil), Nil)) -> "VALUES (1)",
            SqlInsertMode.Values(NonEmptyList(NonEmptyList(SqlExpr.NumberLiteral(1), Nil), List(NonEmptyList(SqlExpr.NumberLiteral(2), Nil)))) -> "VALUES (1), (2)",
            SqlInsertMode.Subquery(SqlQuery.Values(NonEmptyList(NonEmptyList(SqlExpr.NumberLiteral(1), Nil), Nil), None)) -> "VALUES (1)",
        )
        for (m, sql) <- cases do
            assertEquals(createSql(_.printInsertMode(m)), sql)

    test("insert statement"):
        val cases: List[(SqlStatement.Insert, String)] = List(
            SqlStatement.Insert(SqlTable.Ident("t", None, None, None, None), Nil, SqlInsertMode.Values(NonEmptyList(NonEmptyList(SqlExpr.NumberLiteral(1), Nil), Nil))) -> """INSERT INTO "t" VALUES (1)""",
            SqlStatement.Insert(SqlTable.Ident("t", None, None, None, None), List("a", "b"), SqlInsertMode.Values(NonEmptyList(NonEmptyList(SqlExpr.NumberLiteral(1), List(SqlExpr.NumberLiteral(2))), Nil))) -> """INSERT INTO "t" ("a", "b") VALUES (1, 2)""",
        )
        for (s, sql) <- cases do
            assertEquals(createSql(_.printStatement(s)), sql)

    test("update set pair"):
        assertEquals(createSql(_.printUpdateSetPair(SqlUpdateSetPair("c", SqlExpr.Column(None, "x")))), """"c" = "x"""")

    test("update statement"):
        val cases: List[(SqlStatement.Update, String)] = List(
            SqlStatement.Update(SqlTable.Ident("t", None, None, None, None), NonEmptyList(SqlUpdateSetPair("c", SqlExpr.Column(None, "x")), Nil), None) -> """UPDATE "t" SET "c" = "x"""",
            SqlStatement.Update(SqlTable.Ident("t", None, None, None, None), NonEmptyList(SqlUpdateSetPair("a", SqlExpr.NumberLiteral(1)), List(SqlUpdateSetPair("b", SqlExpr.NumberLiteral(2)))), Some(SqlExpr.Column(None, "cond"))) -> """UPDATE "t" SET "a" = 1, "b" = 2 WHERE "cond"""",
        )
        for (s, sql) <- cases do
            assertEquals(createSql(_.printStatement(s)), sql)

    test("delete statement"):
        val cases: List[(SqlStatement.Delete, String)] = List(
            SqlStatement.Delete(SqlTable.Ident("t", None, None, None, None), None) -> """DELETE FROM "t"""",
            SqlStatement.Delete(SqlTable.Ident("t", None, None, None, None), Some(SqlExpr.Column(None, "cond"))) -> """DELETE FROM "t" WHERE "cond"""",
        )
        for (s, sql) <- cases do
            assertEquals(createSql(_.printStatement(s)), sql)

    test("truncate statement"):
        assertEquals(createSql(_.printStatement(SqlStatement.Truncate(SqlTable.Ident("t", None, None, None, None)))), """TRUNCATE TABLE "t"""")