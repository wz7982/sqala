package sqala.test

import sqala.ast.expr.SqlBinaryOperator
import sqala.ir.*
import sqala.test.IrFixture.*
import sqala.util.NonEmptyList

/**
 * DML statement lowering: delete, insert, update, upsert, and the
 * `addWhere` extension.
 */
class I4_Statements extends munit.FunSuite:
    private val user: IrTable.Ident = IrTable.Ident("user", Nil)
    private val uid = col(user, "id")

    test("delete renders alias and optional where"):
        assertEquals(
            pgStmt(IrStatement.Delete(user, Some(IrExpr.Binary(uid, SqlBinaryOperator.Equal, n(1))))),
            """DELETE FROM "user" AS "t1" WHERE "t1"."id" = 1"""
        )
        assertEquals(
            pgStmt(IrStatement.Delete(user, None)),
            """DELETE FROM "user" AS "t1""""
        )

    test("insert renders column list values and subquery"):
        val rows2x2 = NonEmptyList(NonEmptyList(n(1), List(s("a"))), List(NonEmptyList(n(2), List(s("b")))))
        assertEquals(
            pgStmt(IrStatement.Insert(user, List("id", "name"), IrInsertMode.Values(rows2x2))),
            """INSERT INTO "user" ("id", "name") VALUES (1, 'a'), (2, 'b')"""
        )
        assertEquals(
            pgStmt(IrStatement.Insert(user, List("id"), IrInsertMode.Subquery(sel(List(IrSelectItem(n(3), "id")))))),
            lines(
                """INSERT INTO "user" ("id") SELECT""",
                """    3 AS "id""""
            )
        )
        assertEquals(
            pgStmt(IrStatement.Insert(user, Nil, IrInsertMode.Values(NonEmptyList(NonEmptyList(n(1), Nil), Nil)))),
            """INSERT INTO "user" VALUES (1)"""
        )

    test("update renders set pairs and optional where"):
        assertEquals(
            pgStmt(IrStatement.Update(user, NonEmptyList(IrUpdateSetPair("name", s("x")), List(IrUpdateSetPair("age", n(2)))), Some(IrExpr.Binary(uid, SqlBinaryOperator.Equal, n(1))))),
            """UPDATE "user" AS "t1" SET "name" = 'x', "age" = 2 WHERE "t1"."id" = 1"""
        )
        assertEquals(
            pgStmt(IrStatement.Update(user, NonEmptyList(IrUpdateSetPair("name", s("x")), Nil), None)),
            """UPDATE "user" AS "t1" SET "name" = 'x'"""
        )

    test("upsert renders conflict target and excluded update"):
        assertEquals(
            pgStmt(IrStatement.Upsert(user, NonEmptyList("id", List("name")), NonEmptyList(n(1), List(s("a"))), NonEmptyList("id", Nil), NonEmptyList("name", Nil))),
            """INSERT INTO "user" ("id", "name") VALUES (1, 'a') ON CONFLICT ("id") DO UPDATE SET "name" = EXCLUDED."name""""
        )

    test("addWhere extension conjoins successive conditions"):
        assertEquals(
            pgStmt(IrStatement.Delete(user, None).addWhere(uid).addWhere(col("z"))),
            """DELETE FROM "user" AS "t1" WHERE "t1"."id" AND "z""""
        )
        assertEquals(
            pgStmt(IrStatement.Update(user, NonEmptyList(IrUpdateSetPair("name", s("x")), Nil), None).addWhere(uid)),
            """UPDATE "user" AS "t1" SET "name" = 'x' WHERE "t1"."id""""
        )
