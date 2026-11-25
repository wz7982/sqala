package sqala.test

import sqala.ast.expr.*
import sqala.ast.order.{SqlOrdering, SqlOrderingItem}

object JsonFixture:
    def col(n: String): SqlExpr = SqlExpr.Column(None, n)
    def num(n: Int): SqlExpr = SqlExpr.NumberLiteral(n)
    def str(s: String): SqlExpr = SqlExpr.StringLiteral(s)
    def path: SqlExpr = str("$.a")
    def ordAsc(e: SqlExpr): SqlOrderingItem = SqlOrderingItem(e, Some(SqlOrdering.Asc), None)
    def gt(e: SqlExpr, n: Int): SqlExpr = SqlExpr.Binary(e, SqlBinaryOperator.GreaterThan, num(n))

    val output: SqlJsonOutput = SqlJsonOutput(SqlType.Varchar(Some(100)), None)
    val outputFormatted: SqlJsonOutput =
        SqlJsonOutput(SqlType.Json, Some(SqlJsonOutputFormat(Some(SqlEncoding.Utf16))))
    val input: SqlJsonInput = SqlJsonInput(None)
    val inputEncoded: SqlJsonInput = SqlJsonInput(Some(SqlEncoding.Utf8))

class L1_Printer_Json extends munit.FunSuite:
    import JsonFixture.*

    test("json input and output clauses"):
        assertEquals(createSql(_.printJsonInput(input)), "FORMAT JSON")
        assertEquals(createSql(_.printJsonInput(inputEncoded)), "FORMAT JSON ENCODING UTF8")
        assertEquals(createSql(_.printJsonOutputFormat(SqlJsonOutputFormat(None))), "FORMAT JSON")
        assertEquals(createSql(_.printJsonOutput(output)), "RETURNING VARCHAR(100)")
        assertEquals(
            createSql(_.printJsonOutput(outputFormatted)),
            "RETURNING JSON FORMAT JSON ENCODING UTF16"
        )

    test("json_serialize"):
        assertEquals(
            createSql(_.printExpr(SqlExpr.JsonSerializeFunc(col("a"), None))),
            """JSON_SERIALIZE("a")"""
        )
        assertEquals(
            createSql(_.printExpr(SqlExpr.JsonSerializeFunc(col("a"), Some(outputFormatted)))),
            """JSON_SERIALIZE("a" RETURNING JSON FORMAT JSON ENCODING UTF16)"""
        )

    test("json parse"):
        assertEquals(createSql(_.printExpr(SqlExpr.JsonParseFunc(str("{}"), None, None))), "JSON('{}')")
        assertEquals(
            createSql(_.printExpr(SqlExpr.JsonParseFunc(str("{}"), Some(inputEncoded), Some(SqlJsonUniquenessMode.With)))),
            "JSON('{}' FORMAT JSON ENCODING UTF8 WITH UNIQUE KEYS)"
        )

    test("json_query combinations"):
        val base: SqlExpr.JsonQueryFunc =
            SqlExpr.JsonQueryFunc(col("a"), path, Nil, None, None, None, None, None)
        assertEquals(createSql(_.printExpr(base)), """JSON_QUERY("a", '$.a')""")

        assertEquals(
            createSql(_.printExpr(base.copy(passingItems = List(SqlJsonPassingItem(col("b"), "v"))))),
            """JSON_QUERY("a", '$.a' PASSING "b" AS "v")"""
        )
        assertEquals(
            createSql(_.printExpr(base.copy(output = Some(output)))),
            """JSON_QUERY("a", '$.a' RETURNING VARCHAR(100))"""
        )
        assertEquals(
            createSql(_.printExpr(base.copy(wrapper = Some(SqlJsonQueryWrapperBehavior.With(None, true))))),
            """JSON_QUERY("a", '$.a' WITH ARRAY WRAPPER)"""
        )
        assertEquals(
            createSql(_.printExpr(base.copy(
                quotes = Some(SqlJsonQueryQuotesBehavior(SqlJsonQueryQuotesBehaviorMode.Omit, true))
            ))),
            """JSON_QUERY("a", '$.a' OMIT QUOTES ON SCALAR STRING)"""
        )
        assertEquals(
            createSql(_.printExpr(base.copy(
                onEmpty = Some(SqlJsonQueryEmptyBehavior.EmptyArray),
                onError = Some(SqlJsonQueryErrorBehavior.Default(num(0)))
            ))),
            """JSON_QUERY("a", '$.a' EMPTY ARRAY ON EMPTY DEFAULT 0 ON ERROR)"""
        )
        assertEquals(
            createSql(_.printExpr(base.copy(
                passingItems = List(SqlJsonPassingItem(col("b"), "v"), SqlJsonPassingItem(num(1), "w")),
                output = Some(outputFormatted),
                wrapper = Some(SqlJsonQueryWrapperBehavior.With(Some(SqlJsonQueryWrapperBehaviorMode.Conditional), true)),
                quotes = Some(SqlJsonQueryQuotesBehavior(SqlJsonQueryQuotesBehaviorMode.Keep, false)),
                onEmpty = Some(SqlJsonQueryEmptyBehavior.Null),
                onError = Some(SqlJsonQueryErrorBehavior.Error)
            ))),
            """JSON_QUERY("a", '$.a' PASSING "b" AS "v", 1 AS "w" RETURNING JSON FORMAT JSON ENCODING UTF16 WITH CONDITIONAL ARRAY WRAPPER KEEP QUOTES NULL ON EMPTY ERROR ON ERROR)"""
        )

    test("json_value combinations"):
        val base: SqlExpr.JsonValueFunc = SqlExpr.JsonValueFunc(col("a"), path, Nil, None, None, None)
        assertEquals(createSql(_.printExpr(base)), """JSON_VALUE("a", '$.a')""")
        assertEquals(
            createSql(_.printExpr(base.copy(
                passingItems = List(SqlJsonPassingItem(col("b"), "v")),
                output = Some(output),
                onEmpty = Some(SqlJsonValueEmptyBehavior.Default(num(0))),
                onError = Some(SqlJsonValueErrorBehavior.Null)
            ))),
            """JSON_VALUE("a", '$.a' PASSING "b" AS "v" RETURNING VARCHAR(100) DEFAULT 0 ON EMPTY NULL ON ERROR)"""
        )

    test("json_object combinations"):
        val item = SqlJsonObjectItem(str("k"), col("a"))
        val base: SqlExpr.JsonObjectFunc = SqlExpr.JsonObjectFunc(List(item), None, None, None)
        assertEquals(createSql(_.printExpr(base)), """JSON_OBJECT('k' VALUE "a")""")
        assertEquals(createSql(_.printExpr(base.copy(items = Nil))), "JSON_OBJECT()")
        assertEquals(
            createSql(_.printExpr(base.copy(
                items = List(item, SqlJsonObjectItem(str("j"), num(1))),
                nullConstructor = Some(SqlJsonNullConstructor.Absent),
                uniquenessMode = Some(SqlJsonUniquenessMode.Without),
                output = Some(output)
            ))),
            """JSON_OBJECT('k' VALUE "a", 'j' VALUE 1 ABSENT ON NULL WITHOUT UNIQUE KEYS RETURNING VARCHAR(100))"""
        )

    test("json_array combinations"):
        val base: SqlExpr.JsonArrayFunc = SqlExpr.JsonArrayFunc(List(SqlJsonArrayItem(col("a"), None)), None, None)
        assertEquals(createSql(_.printExpr(base)), """JSON_ARRAY("a")""")
        assertEquals(createSql(_.printExpr(base.copy(items = Nil))), "JSON_ARRAY()")
        assertEquals(
            createSql(_.printExpr(base.copy(
                items = List(SqlJsonArrayItem(col("a"), Some(inputEncoded)), SqlJsonArrayItem(num(1), None)),
                nullConstructor = Some(SqlJsonNullConstructor.Null),
                output = Some(outputFormatted)
            ))),
            """JSON_ARRAY("a" FORMAT JSON ENCODING UTF8, 1 NULL ON NULL RETURNING JSON FORMAT JSON ENCODING UTF16)"""
        )

    test("json_exists combinations"):
        val base: SqlExpr.JsonExistsFunc = SqlExpr.JsonExistsFunc(col("a"), path, Nil, None)
        assertEquals(createSql(_.printExpr(base)), """JSON_EXISTS("a", '$.a')""")
        assertEquals(
            createSql(_.printExpr(base.copy(
                passingItems = List(SqlJsonPassingItem(col("b"), "v")),
                onError = Some(SqlJsonExistsErrorBehavior.Unknown)
            ))),
            """JSON_EXISTS("a", '$.a' PASSING "b" AS "v" UNKNOWN ON ERROR)"""
        )

    test("json_objectagg combinations"):
        val base: SqlExpr.JsonObjectAggFunc =
            SqlExpr.JsonObjectAggFunc(SqlJsonObjectItem(str("k"), col("a")), None, None, None, None)
        assertEquals(createSql(_.printExpr(base)), """JSON_OBJECTAGG('k' VALUE "a")""")
        assertEquals(
            createSql(_.printExpr(base.copy(
                nullConstructor = Some(SqlJsonNullConstructor.Absent),
                uniquenessMode = Some(SqlJsonUniquenessMode.With),
                output = Some(output),
                filter = Some(gt(col("b"), 0))
            ))),
            """JSON_OBJECTAGG('k' VALUE "a" ABSENT ON NULL WITH UNIQUE KEYS RETURNING VARCHAR(100)) FILTER (WHERE "b" > 0)"""
        )

    test("json_arrayagg combinations"):
        val base: SqlExpr.JsonArrayAggFunc =
            SqlExpr.JsonArrayAggFunc(SqlJsonArrayItem(col("a"), None), Nil, None, None, None)
        assertEquals(createSql(_.printExpr(base)), """JSON_ARRAYAGG("a")""")
        assertEquals(
            createSql(_.printExpr(base.copy(
                item = SqlJsonArrayItem(col("a"), Some(input)),
                orderBy = List(ordAsc(col("b"))),
                nullConstructor = Some(SqlJsonNullConstructor.Null),
                output = Some(outputFormatted),
                filter = Some(gt(col("b"), 0))
            ))),
            """JSON_ARRAYAGG("a" FORMAT JSON ORDER BY "b" ASC NULL ON NULL RETURNING JSON FORMAT JSON ENCODING UTF16) FILTER (WHERE "b" > 0)"""
        )

    test("json test predicate combinations"):
        val base: SqlExpr.JsonTest = SqlExpr.JsonTest(col("a"), None, None, false)
        assertEquals(createSql(_.printExpr(base)), """"a" IS JSON""")
        assertEquals(createSql(_.printExpr(base.copy(withNot = true))), """"a" IS NOT JSON""")
        assertEquals(
            createSql(_.printExpr(base.copy(nodeType = Some(SqlJsonNodeType.Object)))),
            """"a" IS JSON OBJECT"""
        )
        assertEquals(
            createSql(_.printExpr(base.copy(
                nodeType = Some(SqlJsonNodeType.Array),
                uniquenessMode = Some(SqlJsonUniquenessMode.With),
                withNot = true
            ))),
            """"a" IS NOT JSON ARRAY WITH UNIQUE KEYS"""
        )