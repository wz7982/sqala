package sqala.test

import sqala.ast.expr.*
import sqala.ast.token.SqlUnsafeCustomToken
import sqala.test.Coverage.assertCoverage

object ElementFixture:
    def col(name: String): SqlExpr = SqlExpr.Column(None, name)

    val encodings: Map[String, (SqlEncoding, String)] = Map(
        "Utf8" -> (SqlEncoding.Utf8, "UTF8"),
        "Utf16" -> (SqlEncoding.Utf16, "UTF16"),
        "Utf32" -> (SqlEncoding.Utf32, "UTF32")
    )

    val trimModes: Map[String, (SqlTrimMode, String)] = Map(
        "Both" -> (SqlTrimMode.Both, "BOTH"),
        "Leading" -> (SqlTrimMode.Leading, "LEADING"),
        "Trailing" -> (SqlTrimMode.Trailing, "TRAILING")
    )

    val jsonNodeTypes: Map[String, (SqlJsonNodeType, String)] = Map(
        "Value" -> (SqlJsonNodeType.Value, "VALUE"),
        "Object" -> (SqlJsonNodeType.Object, "OBJECT"),
        "Array" -> (SqlJsonNodeType.Array, "ARRAY"),
        "Scalar" -> (SqlJsonNodeType.Scalar, "SCALAR")
    )

class L1_Printer_Element extends munit.FunSuite:
    import ElementFixture.*

    test("encoding tokens"):
        assertCoverage[SqlEncoding]("Utf8", "Utf16", "Utf32", "UnsafeCustom")
        assertEquals(encodings.keySet + "UnsafeCustom", Coverage.casesOf[SqlEncoding])

        for (name, (e, expected)) <- encodings.toList.sortBy(_._1) do
            assertEquals(createSql(_.printEncoding(e)), expected, name)

        assertEquals(
            createSql(_.printEncoding(SqlEncoding.UnsafeCustom(List(SqlUnsafeCustomToken.Keyword("UTF7"))))),
            "UTF7"
        )

    test("trim mode tokens"):
        assertCoverage[SqlTrimMode]("Both", "Leading", "Trailing")
        assertEquals(trimModes.keySet, Coverage.casesOf[SqlTrimMode])

        for (name, (m, expected)) <- trimModes.toList.sortBy(_._1) do
            assertEquals(createSql(_.printTrimMode(m)), expected, name)

    test("trim specification combinations"):
        assertEquals(createSql(_.printTrim(SqlTrim(None, None))), "")
        assertEquals(createSql(_.printTrim(SqlTrim(Some(SqlTrimMode.Both), None))), "BOTH")
        assertEquals(createSql(_.printTrim(SqlTrim(None, Some(col("a"))))), """"a"""")
        assertEquals(
            createSql(_.printTrim(SqlTrim(Some(SqlTrimMode.Leading), Some(col("a"))))),
            """LEADING "a""""
        )

    test("json uniqueness mode tokens"):
        assertCoverage[SqlJsonUniquenessMode]("With", "Without")
        assertEquals(createSql(_.printJsonUniquenessMode(SqlJsonUniquenessMode.With)), "WITH UNIQUE KEYS")
        assertEquals(createSql(_.printJsonUniquenessMode(SqlJsonUniquenessMode.Without)), "WITHOUT UNIQUE KEYS")

    test("json node type tokens"):
        assertCoverage[SqlJsonNodeType]("Value", "Object", "Array", "Scalar")
        assertEquals(jsonNodeTypes.keySet, Coverage.casesOf[SqlJsonNodeType])

        for (name, (t, expected)) <- jsonNodeTypes.toList.sortBy(_._1) do
            assertEquals(createSql(_.printJsonNodeType(t)), expected, name)

    test("json null constructor tokens"):
        assertCoverage[SqlJsonNullConstructor]("Null", "Absent")
        assertEquals(createSql(_.printJsonNullConstructor(SqlJsonNullConstructor.Null)), "NULL ON NULL")
        assertEquals(createSql(_.printJsonNullConstructor(SqlJsonNullConstructor.Absent)), "ABSENT ON NULL")

    test("json wrapper behavior mode tokens"):
        assertCoverage[SqlJsonQueryWrapperBehaviorMode]("Conditional", "Unconditional")
        assertEquals(
            createSql(_.printJsonQueryWrapperBehaviorMode(SqlJsonQueryWrapperBehaviorMode.Conditional)),
            "CONDITIONAL"
        )
        assertEquals(
            createSql(_.printJsonQueryWrapperBehaviorMode(SqlJsonQueryWrapperBehaviorMode.Unconditional)),
            "UNCONDITIONAL"
        )

    test("json wrapper behavior combinations"):
        assertCoverage[SqlJsonQueryWrapperBehavior]("With", "Without")

        val cases: List[(String, SqlJsonQueryWrapperBehavior, String)] = List(
            ("WITHOUT WRAPPER", SqlJsonQueryWrapperBehavior.Without(false), "WITHOUT WRAPPER"),
            ("WITHOUT ARRAY WRAPPER", SqlJsonQueryWrapperBehavior.Without(true), "WITHOUT ARRAY WRAPPER"),
            ("WITH WRAPPER", SqlJsonQueryWrapperBehavior.With(None, false), "WITH WRAPPER"),
            ("WITH ARRAY WRAPPER", SqlJsonQueryWrapperBehavior.With(None, true), "WITH ARRAY WRAPPER"),
            (
                "WITH CONDITIONAL WRAPPER",
                SqlJsonQueryWrapperBehavior.With(Some(SqlJsonQueryWrapperBehaviorMode.Conditional), false),
                "WITH CONDITIONAL WRAPPER"
            ),
            (
                "WITH UNCONDITIONAL ARRAY WRAPPER",
                SqlJsonQueryWrapperBehavior.With(Some(SqlJsonQueryWrapperBehaviorMode.Unconditional), true),
                "WITH UNCONDITIONAL ARRAY WRAPPER"
            )
        )

        for (name, b, expected) <- cases do
            assertEquals(createSql(_.printJsonQueryWrapperBehavior(b)), expected, name)

    test("json quotes behavior mode tokens"):
        assertCoverage[SqlJsonQueryQuotesBehaviorMode]("Keep", "Omit")
        assertEquals(createSql(_.printJsonQueryQuotesBehaviorMode(SqlJsonQueryQuotesBehaviorMode.Keep)), "KEEP")
        assertEquals(createSql(_.printJsonQueryQuotesBehaviorMode(SqlJsonQueryQuotesBehaviorMode.Omit)), "OMIT")

    test("json quotes behavior combinations"):
        assertEquals(
            createSql(_.printJsonQueryQuotesBehavior(
                SqlJsonQueryQuotesBehavior(SqlJsonQueryQuotesBehaviorMode.Keep, false)
            )),
            "KEEP QUOTES"
        )
        assertEquals(
            createSql(_.printJsonQueryQuotesBehavior(
                SqlJsonQueryQuotesBehavior(SqlJsonQueryQuotesBehaviorMode.Omit, true)
            )),
            "OMIT QUOTES ON SCALAR STRING"
        )

    test("json query empty behavior tokens"):
        assertCoverage[SqlJsonQueryEmptyBehavior]("Error", "Null", "EmptyObject", "EmptyArray", "Default")

        val cases: List[(String, SqlJsonQueryEmptyBehavior, String)] = List(
            ("Error", SqlJsonQueryEmptyBehavior.Error, "ERROR ON EMPTY"),
            ("Null", SqlJsonQueryEmptyBehavior.Null, "NULL ON EMPTY"),
            ("EmptyObject", SqlJsonQueryEmptyBehavior.EmptyObject, "EMPTY OBJECT ON EMPTY"),
            ("EmptyArray", SqlJsonQueryEmptyBehavior.EmptyArray, "EMPTY ARRAY ON EMPTY"),
            ("Default", SqlJsonQueryEmptyBehavior.Default(col("a")), """DEFAULT "a" ON EMPTY""")
        )

        for (name, b, expected) <- cases do
            assertEquals(createSql(_.printJsonQueryEmptyBehavior(b)), expected, name)

    test("json query error behavior tokens"):
        assertCoverage[SqlJsonQueryErrorBehavior]("Error", "Null", "EmptyObject", "EmptyArray", "Default")

        val cases: List[(String, SqlJsonQueryErrorBehavior, String)] = List(
            ("Error", SqlJsonQueryErrorBehavior.Error, "ERROR ON ERROR"),
            ("Null", SqlJsonQueryErrorBehavior.Null, "NULL ON ERROR"),
            ("EmptyObject", SqlJsonQueryErrorBehavior.EmptyObject, "EMPTY OBJECT ON ERROR"),
            ("EmptyArray", SqlJsonQueryErrorBehavior.EmptyArray, "EMPTY ARRAY ON ERROR"),
            ("Default", SqlJsonQueryErrorBehavior.Default(col("a")), """DEFAULT "a" ON ERROR""")
        )

        for (name, b, expected) <- cases do
            assertEquals(createSql(_.printJsonQueryErrorBehavior(b)), expected, name)

    test("json value empty behavior tokens"):
        assertCoverage[SqlJsonValueEmptyBehavior]("Error", "Null", "Default")

        assertEquals(createSql(_.printJsonValueEmptyBehavior(SqlJsonValueEmptyBehavior.Error)), "ERROR ON EMPTY")
        assertEquals(createSql(_.printJsonValueEmptyBehavior(SqlJsonValueEmptyBehavior.Null)), "NULL ON EMPTY")
        assertEquals(
            createSql(_.printJsonValueEmptyBehavior(SqlJsonValueEmptyBehavior.Default(col("a")))),
            """DEFAULT "a" ON EMPTY"""
        )

    test("json value error behavior tokens"):
        assertCoverage[SqlJsonValueErrorBehavior]("Error", "Null", "Default")

        assertEquals(createSql(_.printJsonValueErrorBehavior(SqlJsonValueErrorBehavior.Error)), "ERROR ON ERROR")
        assertEquals(createSql(_.printJsonValueErrorBehavior(SqlJsonValueErrorBehavior.Null)), "NULL ON ERROR")
        assertEquals(
            createSql(_.printJsonValueErrorBehavior(SqlJsonValueErrorBehavior.Default(col("a")))),
            """DEFAULT "a" ON ERROR"""
        )

    test("json exists error behavior tokens"):
        assertCoverage[SqlJsonExistsErrorBehavior]("Error", "True", "False", "Unknown")

        assertEquals(createSql(_.printJsonExistsErrorBehavior(SqlJsonExistsErrorBehavior.Error)), "ERROR ON ERROR")
        assertEquals(createSql(_.printJsonExistsErrorBehavior(SqlJsonExistsErrorBehavior.True)), "TRUE ON ERROR")
        assertEquals(createSql(_.printJsonExistsErrorBehavior(SqlJsonExistsErrorBehavior.False)), "FALSE ON ERROR")
        assertEquals(createSql(_.printJsonExistsErrorBehavior(SqlJsonExistsErrorBehavior.Unknown)), "UNKNOWN ON ERROR")

    test("json passing item"):
        assertEquals(
            createSql(_.printJsonPassingItem(SqlJsonPassingItem(col("a"), "x"))),
            """"a" AS "x""""
        )

    test("listagg count mode tokens"):
        assertCoverage[SqlListAggCountMode]("With", "Without")
        assertEquals(createSql(_.printListAggCountMode(SqlListAggCountMode.With)), "WITH COUNT")
        assertEquals(createSql(_.printListAggCountMode(SqlListAggCountMode.Without)), "WITHOUT COUNT")

    test("listagg on overflow tokens"):
        assertCoverage[SqlListAggOnOverflow]("Error", "Truncate")

        assertEquals(createSql(_.printListAggOnOverflow(SqlListAggOnOverflow.Error)), "ON OVERFLOW ERROR")
        assertEquals(
            createSql(_.printListAggOnOverflow(
                SqlListAggOnOverflow.Truncate(col("a"), SqlListAggCountMode.With)
            )),
            """ON OVERFLOW TRUNCATE "a" WITH COUNT"""
        )
        assertEquals(
            createSql(_.printListAggOnOverflow(
                SqlListAggOnOverflow.Truncate(col("a"), SqlListAggCountMode.Without)
            )),
            """ON OVERFLOW TRUNCATE "a" WITHOUT COUNT"""
        )

    test("window nulls mode tokens"):
        assertCoverage[SqlWindowNullsMode]("Respect", "Ignore")
        assertEquals(createSql(_.printWindowNullsMode(SqlWindowNullsMode.Respect)), "RESPECT NULLS")
        assertEquals(createSql(_.printWindowNullsMode(SqlWindowNullsMode.Ignore)), "IGNORE NULLS")

    test("nth value from mode tokens"):
        assertCoverage[SqlNthValueFromMode]("First", "Last")
        assertEquals(createSql(_.printNthValueFromMode(SqlNthValueFromMode.First)), "FROM FIRST")
        assertEquals(createSql(_.printNthValueFromMode(SqlNthValueFromMode.Last)), "FROM LAST")

    test("match phase tokens"):
        assertCoverage[SqlMatchPhase]("Final", "Running")
        assertEquals(createSql(_.printMatchPhase(SqlMatchPhase.Final)), "FINAL")
        assertEquals(createSql(_.printMatchPhase(SqlMatchPhase.Running)), "RUNNING")
        assertEquals(createSql(_.printMatchPhaseExpr(SqlExpr.MatchPhase(SqlMatchPhase.Final, col("a")))), """FINAL "a"""")