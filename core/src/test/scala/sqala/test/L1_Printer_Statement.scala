package sqala.test

import sqala.ast.statement.*
import sqala.ast.table.SqlTableAlias
import sqala.test.Coverage.assertCoverage

class L1_Printer_Statement extends munit.FunSuite:
    import QueryFixture.*

    def lines(xs: String*): String = xs.mkString("\n")

    test("statement coverage"):
        assertCoverage[SqlStatement]("Delete", "Insert", "Update", "Truncate", "Upsert")
        assertCoverage[SqlInsertMode]("Values", "Subquery")
        assertCoverage[SqlQuery]("Select", "Set", "Values", "With")

    test("delete"):
        assertEquals(
            createSql(_.printStatement(SqlStatement.Delete(table("t"), None))),
            """DELETE FROM "t""""
        )
        assertEquals(
            createSql(_.printStatement(SqlStatement.Delete(table("t"), Some(eqTo(col("a"), num(1)))))),
            """DELETE FROM "t" WHERE "a" = 1"""
        )

    test("update"):
        val base: SqlStatement.Update = SqlStatement.Update(table("t"), nel(SqlUpdateSetPair("a", num(1))), None)
        assertEquals(createSql(_.printStatement(base)), """UPDATE "t" SET "a" = 1""")
        assertEquals(
            createSql(_.printStatement(base.copy(
                setPairs = nel(SqlUpdateSetPair("a", num(1)), SqlUpdateSetPair("b", str("x"))),
                where = Some(gt(col("a"), 0))
            ))),
            """UPDATE "t" SET "a" = 1, "b" = 'x' WHERE "a" > 0"""
        )

    test("insert values"):
        val base: SqlStatement.Insert = SqlStatement.Insert(table("t"), Nil, SqlInsertMode.Values(nel(nel(num(1)))))
        assertEquals(createSql(_.printStatement(base)), """INSERT INTO "t" VALUES (1)""")
        assertEquals(
            createSql(_.printStatement(base.copy(
                columns = List("a", "b"),
                mode = SqlInsertMode.Values(nel(nel(num(1), num(2)), nel(num(3), num(4))))
            ))),
            """INSERT INTO "t" ("a", "b") VALUES (1, 2), (3, 4)"""
        )

    test("insert from subquery"):
        assertEquals(
            createSql(_.printStatement(SqlStatement.Insert(table("t"), List("a"), SqlInsertMode.Subquery(simple)))),
            lines("""INSERT INTO "t" ("a") SELECT""", """    "a"""", "FROM", """    "t"""")
        )

    test("truncate"):
        assertEquals(
            createSql(_.printStatement(SqlStatement.Truncate(table("t")))),
            """TRUNCATE TABLE "t""""
        )

    test("upsert renders a MERGE statement"):
        val statement = SqlStatement.Upsert(
            table("t"),
            nel("a", "b"),
            nel(num(1), num(2)),
            nel("a"),
            nel("b")
        )
        assertEquals(
            createSql(_.printStatement(statement)),
            lines(
                """MERGE INTO "t" AS "t1"""",
                "USING (",
                "    SELECT",
                """        1 AS "a",""",
                """        2 AS "b"""",
                """) AS "t2"""",
                """ON ("t1"."a" = "t2"."a")""",
                """WHEN MATCHED THEN UPDATE SET "t1"."b" = "t2"."b"""",
                """WHEN NOT MATCHED THEN INSERT ("a", "b")""",
                "VALUES (1, 2)"
            )
        )

    test("upsert with composite primary key"):
        val statement = SqlStatement.Upsert(
            table("t"),
            nel("a", "b", "c"),
            nel(num(1), num(2), num(3)),
            nel("a", "b"),
            nel("c")
        )
        val printed = createSql(_.printStatement(statement))
        assert(
            printed.contains("""ON ("t1"."a" = "t2"."a" AND "t1"."b" = "t2"."b")"""),
            printed
        )

    test("upsert replaces a user supplied table alias"):
        val aliased = table("t").copy(alias = Some(SqlTableAlias("mine", Nil)))
        val statement = SqlStatement.Upsert(aliased, nel("a"), nel(num(1)), nel("a"), nel("a"))
        val printed = createSql(_.printStatement(statement))
        assert(printed.startsWith("""MERGE INTO "t" AS "t1""""), printed)

    test("addWhere extensions"):
        val delete: SqlStatement.Delete = SqlStatement.Delete(table("t"), None)
        assertEquals(
            createSql(_.printStatement(delete.addWhere(gt(col("a"), 0)))),
            """DELETE FROM "t" WHERE "a" > 0"""
        )
        assertEquals(
            createSql(_.printStatement(delete.addWhere(gt(col("a"), 0)).addWhere(eqTo(col("b"), num(1))))),
            """DELETE FROM "t" WHERE "a" > 0 AND "b" = 1"""
        )

        val update: SqlStatement.Update = SqlStatement.Update(table("t"), nel(SqlUpdateSetPair("a", num(1))), None)
        assertEquals(
            createSql(_.printStatement(update.addWhere(gt(col("a"), 0)).addWhere(eqTo(col("b"), num(1))))),
            """UPDATE "t" SET "a" = 1 WHERE "a" > 0 AND "b" = 1"""
        )

    test("select addWhere addHaving and addSelectItem extensions"):
        val q = simple
            .addWhere(gt(col("a"), 0))
            .addWhere(eqTo(col("b"), num(1)))
            .addHaving(gt(col("c"), 2))
            .addHaving(eqTo(col("d"), num(3)))
            .addSelectItem(item(col("e")))
        assertEquals(
            createSql(_.printQuery(q)),
            lines(
                "SELECT",
                """    "a",""",
                """    "e"""",
                "FROM",
                """    "t"""",
                "WHERE",
                """    "a" > 0 AND "b" = 1""",
                "HAVING",
                """    "c" > 2 AND "d" = 3"""
            )
        )