package sqala.test

import sqala.ast.expr.*
import sqala.ast.table.*
import sqala.test.Coverage.assertCoverage
import sqala.util.NonEmptyList

class L1_Printer_JsonTable extends munit.FunSuite:
    import QueryFixture.*

    def lines(xs: String*): String = xs.mkString("\n")

    def jsonTable(columns: NonEmptyList[SqlJsonColumn]): SqlTable.Json =
        SqlTable.Json(false, col("a"), str("$.b"), None, Nil, columns, None, None, None)

    test("json table coverage"):
        assertCoverage[SqlJsonErrorBehavior]("Error", "Empty", "EmptyArray")
        assertCoverage[SqlJsonColumn]("Ordinality", "Column", "Exists", "Nested")

    test("json table error behavior tokens"):
        assertEquals(createSql(_.printJsonErrorBehavior(SqlJsonErrorBehavior.Error)), "ERROR ON ERROR")
        assertEquals(createSql(_.printJsonErrorBehavior(SqlJsonErrorBehavior.Empty)), "EMPTY ON ERROR")
        assertEquals(createSql(_.printJsonErrorBehavior(SqlJsonErrorBehavior.EmptyArray)), "EMPTY ARRAY ON ERROR")

    test("ordinality column"):
        assertEquals(
            createSql(_.printJsonColumn(SqlJsonColumn.Ordinality("n"))),
            """"n" FOR ORDINALITY"""
        )

    test("regular column combinations"):
        val base: SqlJsonColumn.Column =
            SqlJsonColumn.Column("c", SqlType.Int, None, None, None, None, None, None)
        assertEquals(createSql(_.printJsonColumn(base)), """"c" INTEGER""")
        assertEquals(
            createSql(_.printJsonColumn(base.copy(
                `type` = SqlType.Varchar(Some(20)),
                format = Some(SqlJsonOutputFormat(Some(SqlEncoding.Utf8))),
                path = Some(str("$.c")),
                wrapper = Some(SqlJsonQueryWrapperBehavior.Without(true)),
                quotes = Some(SqlJsonQueryQuotesBehavior(SqlJsonQueryQuotesBehaviorMode.Keep, true)),
                onEmpty = Some(SqlJsonQueryEmptyBehavior.Null),
                onError = Some(SqlJsonQueryErrorBehavior.Error)
            ))),
            """"c" VARCHAR(20) FORMAT JSON ENCODING UTF8 PATH '$.c' WITHOUT ARRAY WRAPPER KEEP QUOTES ON SCALAR STRING NULL ON EMPTY ERROR ON ERROR"""
        )

    test("exists column"):
        val base: SqlJsonColumn.Exists = SqlJsonColumn.Exists("e", SqlType.Boolean, None, None)
        assertEquals(createSql(_.printJsonColumn(base)), """"e" BOOLEAN EXISTS""")
        assertEquals(
            createSql(_.printJsonColumn(base.copy(
                path = Some(str("$.e")),
                onError = Some(SqlJsonExistsErrorBehavior.False)
            ))),
            """"e" BOOLEAN EXISTS PATH '$.e' FALSE ON ERROR"""
        )

    test("nested path alias is printed with AS"):
        val nested = SqlJsonColumn.Nested(str("$.n"), Some("x"), nel(SqlJsonColumn.Ordinality("n")))
        val printed = createSql(_.printJsonColumn(nested))
        assert(printed.contains("""NESTED PATH '$.n' AS "x" COLUMNS("""), printed)

    test("nested path without alias"):
        val nested = SqlJsonColumn.Nested(str("$.n"), None, nel(SqlJsonColumn.Ordinality("n")))
        assertEquals(
            createSql(_.printJsonColumn(nested)),
            lines("""NESTED PATH '$.n' COLUMNS(""", """    "n" FOR ORDINALITY""", ")")
        )

    test("json table"):
        assertEquals(
            createSql(_.printTable(jsonTable(nel(SqlJsonColumn.Ordinality("n"))))),
            lines("JSON_TABLE(", """    "a", '$.b' COLUMNS(""", """        "n" FOR ORDINALITY""", "    )", ")")
        )

    test("json table with error behavior and alias"):
        val t = jsonTable(nel(SqlJsonColumn.Ordinality("n"))).copy(
            withLateral = true,
            pathAlias = Some("p"),
            passingItems = List(SqlJsonPassingItem(col("b"), "v")),
            onError = Some(SqlJsonErrorBehavior.Empty),
            alias = Some(SqlTableAlias("x", Nil))
        )
        assertEquals(
            createSql(_.printTable(t)),
            lines(
                "LATERAL JSON_TABLE(",
                """    "a", '$.b' AS "p" PASSING "b" AS "v" COLUMNS(""",
                """        "n" FOR ORDINALITY""",
                "    )",
                "    EMPTY ON ERROR",
                """) AS "x""""
            )
        )