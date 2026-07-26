import sqala.ast.expr.*
import sqala.ast.order.*
import sqala.ast.quantifier.SqlQuantifier
import sqala.ast.statement.SqlQuery
import sqala.ast.token.SqlUnsafeCustomToken
import sqala.ast.window.*
import sqala.util.NonEmptyList

class TestExpr extends munit.FunSuite:
    test("binary operator"):
        val cases: List[(SqlBinaryOperator, String)] = List(
            SqlBinaryOperator.Times -> "*",
            SqlBinaryOperator.Div -> "/",
            SqlBinaryOperator.Plus -> "+",
            SqlBinaryOperator.Minus -> "-",
            SqlBinaryOperator.Concat -> "||",
            SqlBinaryOperator.Equal -> "=",
            SqlBinaryOperator.NotEqual -> "<>",
            SqlBinaryOperator.IsDistinctFrom(false) -> "IS DISTINCT FROM",
            SqlBinaryOperator.IsDistinctFrom(true) -> "IS NOT DISTINCT FROM",
            SqlBinaryOperator.Is(false) -> "IS",
            SqlBinaryOperator.Is(true) -> "IS NOT",
            SqlBinaryOperator.GreaterThan -> ">",
            SqlBinaryOperator.GreaterThanEqual -> ">=",
            SqlBinaryOperator.LessThan -> "<",
            SqlBinaryOperator.LessThanEqual -> "<=",
            SqlBinaryOperator.Overlaps -> "OVERLAPS",
            SqlBinaryOperator.And -> "AND",
            SqlBinaryOperator.Or -> "OR",
        )
        for (op, sql) <- cases do
            assertEquals(createSql(_.printBinaryOperator(op)), sql)

    test("unary operator"):
        val cases: List[(SqlUnaryOperator, String)] = List(
            SqlUnaryOperator.Positive -> "+",
            SqlUnaryOperator.Negative -> "-",
            SqlUnaryOperator.Not -> "NOT",
        )
        for (op, sql) <- cases do
            assertEquals(createSql(_.printUnaryOperator(op)), sql)

    test("quantified comparison operator"):
        val cases: List[(SqlQuantifiedComparisonOperator, String)] = List(
            SqlQuantifiedComparisonOperator.Equal -> "=",
            SqlQuantifiedComparisonOperator.NotEqual -> "<>",
            SqlQuantifiedComparisonOperator.GreaterThan -> ">",
            SqlQuantifiedComparisonOperator.GreaterThanEqual -> ">=",
            SqlQuantifiedComparisonOperator.LessThan -> "<",
            SqlQuantifiedComparisonOperator.LessThanEqual -> "<=",
        )
        for (op, sql) <- cases do
            assertEquals(createSql(_.printQuantifiedCompareOperator(op)), sql)

    test("subquery quantifier"):
        val cases: List[(SqlSubqueryQuantifier, String)] = List(
            SqlSubqueryQuantifier.Any -> "ANY",
            SqlSubqueryQuantifier.All -> "ALL",
        )
        for (q, sql) <- cases do
            assertEquals(createSql(_.printSubqueryQuantifier(q)), sql)

    test("time unit"):
        val cases: List[(SqlTimeUnit, String)] = List(
            SqlTimeUnit.Year -> "YEAR",
            SqlTimeUnit.Month -> "MONTH",
            SqlTimeUnit.Day -> "DAY",
            SqlTimeUnit.Hour -> "HOUR",
            SqlTimeUnit.Minute -> "MINUTE",
            SqlTimeUnit.Second -> "SECOND",
        )
        for (u, sql) <- cases do
            assertEquals(createSql(_.printTimeUnit(u)), sql)

    test("interval field"):
        val cases: List[(SqlIntervalField, String)] = List(
            SqlIntervalField.Single(SqlTimeUnit.Day) -> "DAY",
            SqlIntervalField.To(SqlTimeUnit.Year, SqlTimeUnit.Month) -> "YEAR TO MONTH",
        )
        for (f, sql) <- cases do
            assertEquals(createSql(_.printIntervalField(f)), sql)

    test("time type"):
        val cases: List[(SqlTimeType, String)] = List(
            SqlTimeType.Date -> "DATE",
            SqlTimeType.Timestamp(None) -> "TIMESTAMP",
            SqlTimeType.Timestamp(Some(SqlTimeZoneMode.With)) -> "TIMESTAMP WITH TIME ZONE",
            SqlTimeType.Timestamp(Some(SqlTimeZoneMode.Without)) -> "TIMESTAMP WITHOUT TIME ZONE",
            SqlTimeType.Time(None) -> "TIME",
            SqlTimeType.Time(Some(SqlTimeZoneMode.With)) -> "TIME WITH TIME ZONE",
            SqlTimeType.Time(Some(SqlTimeZoneMode.Without)) -> "TIME WITHOUT TIME ZONE",
        )
        for (t, sql) <- cases do
            assertEquals(createSql(_.printTimeType(t)), sql)

    test("time zone mode"):
        val cases: List[(SqlTimeZoneMode, String)] = List(
            SqlTimeZoneMode.With -> "WITH TIME ZONE",
            SqlTimeZoneMode.Without -> "WITHOUT TIME ZONE",
        )
        for (m, sql) <- cases do
            assertEquals(createSql(_.printTimeZoneMode(m)), sql)

    test("type"):
        val cases: List[(SqlType, String)] = List(
            SqlType.Varchar(None) -> "VARCHAR",
            SqlType.Varchar(Some(255)) -> "VARCHAR(255)",
            SqlType.Int -> "INTEGER",
            SqlType.Long -> "BIGINT",
            SqlType.Float -> "REAL",
            SqlType.Double -> "DOUBLE PRECISION",
            SqlType.Decimal(None) -> "DECIMAL",
            SqlType.Decimal(Some((10, 2))) -> "DECIMAL(10, 2)",
            SqlType.Date -> "DATE",
            SqlType.Timestamp(None) -> "TIMESTAMP",
            SqlType.Timestamp(Some(SqlTimeZoneMode.With)) -> "TIMESTAMP WITH TIME ZONE",
            SqlType.Timestamp(Some(SqlTimeZoneMode.Without)) -> "TIMESTAMP WITHOUT TIME ZONE",
            SqlType.Time(None) -> "TIME",
            SqlType.Time(Some(SqlTimeZoneMode.With)) -> "TIME WITH TIME ZONE",
            SqlType.Time(Some(SqlTimeZoneMode.Without)) -> "TIME WITHOUT TIME ZONE",
            SqlType.Json -> "JSON",
            SqlType.Boolean -> "BOOLEAN",
            SqlType.Interval -> "INTERVAL",
            SqlType.Geometry -> "GEOMETRY",
            SqlType.Point -> "POINT",
            SqlType.LineString -> "LINESTRING",
            SqlType.Polygon -> "POLYGON",
            SqlType.MultiPoint -> "MULTIPOINT",
            SqlType.MultiLineString -> "MULTILINESTRING",
            SqlType.MultiPolygon -> "MULTIPOLYGON",
            SqlType.GeometryCollection -> "GEOMETRYCOLLECTION",
            SqlType.Array(SqlType.Int) -> "INTEGER[]",
            SqlType.Array(SqlType.Array(SqlType.Int)) -> "INTEGER[][]",
        )
        for (t, sql) <- cases do
            assertEquals(createSql(_.printType(t)), sql)

    test("in right operand"):
        val cases: List[(SqlInRightOperand, String)] = List(
            SqlInRightOperand.Values(NonEmptyList(SqlExpr.NumberLiteral(1), Nil)) -> "(1)",
            SqlInRightOperand.Values(NonEmptyList(SqlExpr.NumberLiteral(1), List(SqlExpr.NumberLiteral(2)))) -> "(1, 2)",
            SqlInRightOperand.Subquery(SqlQuery.Values(NonEmptyList(NonEmptyList(SqlExpr.NumberLiteral(1), Nil), Nil), Nil, None)) -> "(VALUES (1))",
        )
        for (o, sql) <- cases do
            assertEquals(createSql(_.printInRightOperand(o)), sql)

    test("case branch"):
        assertEquals(createSql(_.printCaseBranch(SqlCaseBranch(SqlExpr.Column(None, "a"), SqlExpr.Column(None, "b")))), """WHEN "a" THEN "b"""")

    test("encoding"):
        val cases: List[(SqlEncoding, String)] = List(
            SqlEncoding.Utf8 -> "UTF8",
            SqlEncoding.Utf16 -> "UTF16",
            SqlEncoding.Utf32 -> "UTF32",
        )
        for (e, sql) <- cases do
            assertEquals(createSql(_.printEncoding(e)), sql)

    test("trim mode"):
        val cases: List[(SqlTrimMode, String)] = List(
            SqlTrimMode.Both -> "BOTH",
            SqlTrimMode.Leading -> "LEADING",
            SqlTrimMode.Trailing -> "TRAILING",
        )
        for (m, sql) <- cases do
            assertEquals(createSql(_.printTrimMode(m)), sql)

    test("trim"):
        val cases: List[(SqlTrim, String)] = List(
            SqlTrim(None, None) -> "",
            SqlTrim(None, Some(SqlExpr.Column(None, "x"))) -> """"x"""",
            SqlTrim(Some(SqlTrimMode.Leading), None) -> "LEADING",
            SqlTrim(Some(SqlTrimMode.Leading), Some(SqlExpr.Column(None, "x"))) -> """LEADING "x"""",
        )
        for (t, sql) <- cases do
            assertEquals(createSql(_.printTrim(t)), sql)

    test("json uniqueness mode"):
        val cases: List[(SqlJsonUniquenessMode, String)] = List(
            SqlJsonUniquenessMode.With -> "WITH UNIQUE KEYS",
            SqlJsonUniquenessMode.Without -> "WITHOUT UNIQUE KEYS",
        )
        for (m, sql) <- cases do
            assertEquals(createSql(_.printJsonUniquenessMode(m)), sql)

    test("json passing item"):
        assertEquals(createSql(_.printJsonPassingItem(SqlJsonPassingItem(SqlExpr.Column(None, "x"), "alias"))), """"x" AS "alias"""")

    test("json node type"):
        val cases: List[(SqlJsonNodeType, String)] = List(
            SqlJsonNodeType.Value -> "VALUE",
            SqlJsonNodeType.Object -> "OBJECT",
            SqlJsonNodeType.Array -> "ARRAY",
            SqlJsonNodeType.Scalar -> "SCALAR",
        )
        for (n, sql) <- cases do
            assertEquals(createSql(_.printJsonNodeType(n)), sql)

    test("json null constructor"):
        val cases: List[(SqlJsonNullConstructor, String)] = List(
            SqlJsonNullConstructor.Null -> "NULL ON NULL",
            SqlJsonNullConstructor.Absent -> "ABSENT ON NULL",
        )
        for (c, sql) <- cases do
            assertEquals(createSql(_.printJsonNullConstructor(c)), sql)

    test("json query wrapper behavior mode"):
        val cases: List[(SqlJsonQueryWrapperBehaviorMode, String)] = List(
            SqlJsonQueryWrapperBehaviorMode.Conditional -> "CONDITIONAL",
            SqlJsonQueryWrapperBehaviorMode.Unconditional -> "UNCONDITIONAL",
        )
        for (m, sql) <- cases do
            assertEquals(createSql(_.printJsonQueryWrapperBehaviorMode(m)), sql)

    test("json query wrapper behavior"):
        val cases: List[(SqlJsonQueryWrapperBehavior, String)] = List(
            SqlJsonQueryWrapperBehavior.Without(false) -> "WITHOUT WRAPPER",
            SqlJsonQueryWrapperBehavior.Without(true) -> "WITHOUT ARRAY WRAPPER",
            SqlJsonQueryWrapperBehavior.With(None, false) -> "WITH WRAPPER",
            SqlJsonQueryWrapperBehavior.With(None, true) -> "WITH ARRAY WRAPPER",
            SqlJsonQueryWrapperBehavior.With(Some(SqlJsonQueryWrapperBehaviorMode.Conditional), false) -> "WITH CONDITIONAL WRAPPER",
            SqlJsonQueryWrapperBehavior.With(Some(SqlJsonQueryWrapperBehaviorMode.Conditional), true) -> "WITH CONDITIONAL ARRAY WRAPPER",
        )
        for (b, sql) <- cases do
            assertEquals(createSql(_.printJsonQueryWrapperBehavior(b)), sql)

    test("json query quotes behavior mode"):
        val cases: List[(SqlJsonQueryQuotesBehaviorMode, String)] = List(
            SqlJsonQueryQuotesBehaviorMode.Keep -> "KEEP",
            SqlJsonQueryQuotesBehaviorMode.Omit -> "OMIT",
        )
        for (m, sql) <- cases do
            assertEquals(createSql(_.printJsonQueryQuotesBehaviorMode(m)), sql)

    test("json query quotes behavior"):
        val cases: List[(SqlJsonQueryQuotesBehavior, String)] = List(
            SqlJsonQueryQuotesBehavior(SqlJsonQueryQuotesBehaviorMode.Keep, false) -> "KEEP QUOTES",
            SqlJsonQueryQuotesBehavior(SqlJsonQueryQuotesBehaviorMode.Keep, true) -> "KEEP QUOTES ON SCALAR STRING",
        )
        for (b, sql) <- cases do
            assertEquals(createSql(_.printJsonQueryQuotesBehavior(b)), sql)
            
    test("json query empty behavior"):
        val cases: List[(SqlJsonQueryEmptyBehavior, String)] = List(
            SqlJsonQueryEmptyBehavior.Error -> "ERROR ON EMPTY",
            SqlJsonQueryEmptyBehavior.Null -> "NULL ON EMPTY",
            SqlJsonQueryEmptyBehavior.EmptyObject -> "EMPTY OBJECT ON EMPTY",
            SqlJsonQueryEmptyBehavior.EmptyArray -> "EMPTY ARRAY ON EMPTY",
            SqlJsonQueryEmptyBehavior.Default(SqlExpr.Column(None, "x")) -> """DEFAULT "x" ON EMPTY""",
        )
        for (b, sql) <- cases do
            assertEquals(createSql(_.printJsonQueryEmptyBehavior(b)), sql)

    test("json query error behavior"):
        val cases: List[(SqlJsonQueryErrorBehavior, String)] = List(
            SqlJsonQueryErrorBehavior.Error -> "ERROR ON ERROR",
            SqlJsonQueryErrorBehavior.Null -> "NULL ON ERROR",
            SqlJsonQueryErrorBehavior.EmptyObject -> "EMPTY OBJECT ON ERROR",
            SqlJsonQueryErrorBehavior.EmptyArray -> "EMPTY ARRAY ON ERROR",
            SqlJsonQueryErrorBehavior.Default(SqlExpr.Column(None, "x")) -> """DEFAULT "x" ON ERROR""",
        )
        for (b, sql) <- cases do
            assertEquals(createSql(_.printJsonQueryErrorBehavior(b)), sql)

    test("json value empty behavior"):
        val cases: List[(SqlJsonValueEmptyBehavior, String)] = List(
            SqlJsonValueEmptyBehavior.Error -> "ERROR ON EMPTY",
            SqlJsonValueEmptyBehavior.Null -> "NULL ON EMPTY",
            SqlJsonValueEmptyBehavior.Default(SqlExpr.Column(None, "x")) -> """DEFAULT "x" ON EMPTY""",
        )
        for (b, sql) <- cases do
            assertEquals(createSql(_.printJsonValueEmptyBehavior(b)), sql)

    test("json value error behavior"):
        val cases: List[(SqlJsonValueErrorBehavior, String)] = List(
            SqlJsonValueErrorBehavior.Error -> "ERROR ON ERROR",
            SqlJsonValueErrorBehavior.Null -> "NULL ON ERROR",
            SqlJsonValueErrorBehavior.Default(SqlExpr.Column(None, "x")) -> """DEFAULT "x" ON ERROR""",
        )
        for (b, sql) <- cases do
            assertEquals(createSql(_.printJsonValueErrorBehavior(b)), sql)

    test("json exists error behavior"):
        val cases: List[(SqlJsonExistsErrorBehavior, String)] = List(
            SqlJsonExistsErrorBehavior.Error -> "ERROR ON ERROR",
            SqlJsonExistsErrorBehavior.True -> "TRUE ON ERROR",
            SqlJsonExistsErrorBehavior.False -> "FALSE ON ERROR",
            SqlJsonExistsErrorBehavior.Unknown -> "UNKNOWN ON ERROR",
        )
        for (b, sql) <- cases do
            assertEquals(createSql(_.printJsonExistsErrorBehavior(b)), sql)
            
    test("json input"):
        val cases: List[(SqlJsonInput, String)] = List(
            SqlJsonInput(None) -> "FORMAT JSON",
            SqlJsonInput(Some(SqlEncoding.Utf8)) -> "FORMAT JSON ENCODING UTF8",
        )
        for (i, sql) <- cases do
            assertEquals(createSql(_.printJsonInput(i)), sql)

    test("json output format"):
        val cases: List[(SqlJsonOutputFormat, String)] = List(
            SqlJsonOutputFormat(None) -> "FORMAT JSON",
            SqlJsonOutputFormat(Some(SqlEncoding.Utf8)) -> "FORMAT JSON ENCODING UTF8",
        )
        for (f, sql) <- cases do
            assertEquals(createSql(_.printJsonOutputFormat(f)), sql)

    test("json output"):
        val cases: List[(SqlJsonOutput, String)] = List(
            SqlJsonOutput(SqlType.Int, None) -> "RETURNING INTEGER",
            SqlJsonOutput(SqlType.Int, Some(SqlJsonOutputFormat(Some(SqlEncoding.Utf8)))) -> "RETURNING INTEGER FORMAT JSON ENCODING UTF8",
        )
        for (o, sql) <- cases do
            assertEquals(createSql(_.printJsonOutput(o)), sql)

    test("json object item"):
        assertEquals(createSql(_.printJsonObjectItem(SqlJsonObjectItem(SqlExpr.Column(None, "k"), SqlExpr.Column(None, "v")))), """"k" VALUE "v"""")

    test("json array item"):
        val cases: List[(SqlJsonArrayItem, String)] = List(
            SqlJsonArrayItem(SqlExpr.Column(None, "x"), None) -> """"x"""",
            SqlJsonArrayItem(SqlExpr.Column(None, "x"), Some(SqlJsonInput(Some(SqlEncoding.Utf8)))) -> """"x" FORMAT JSON ENCODING UTF8""",
        )
        for (i, sql) <- cases do
            assertEquals(createSql(_.printJsonArrayItem(i)), sql)

    test("list agg count mode"):
        val cases: List[(SqlListAggCountMode, String)] = List(
            SqlListAggCountMode.With -> "WITH COUNT",
            SqlListAggCountMode.Without -> "WITHOUT COUNT",
        )
        for (m, sql) <- cases do
            assertEquals(createSql(_.printListAggCountMode(m)), sql)

    test("list agg on overflow"):
        val cases: List[(SqlListAggOnOverflow, String)] = List(
            SqlListAggOnOverflow.Error -> "ON OVERFLOW ERROR",
            SqlListAggOnOverflow.Truncate(SqlExpr.Column(None, "x"), SqlListAggCountMode.With) -> """ON OVERFLOW TRUNCATE "x" WITH COUNT""",
        )
        for (o, sql) <- cases do
            assertEquals(createSql(_.printListAggOnOverflow(o)), sql)

    test("window nulls mode"):
        val cases: List[(SqlWindowNullsMode, String)] = List(
            SqlWindowNullsMode.Respect -> "RESPECT NULLS",
            SqlWindowNullsMode.Ignore -> "IGNORE NULLS",
        )
        for (m, sql) <- cases do
            assertEquals(createSql(_.printWindowNullsMode(m)), sql)

    test("nth value from mode"):
        val cases: List[(SqlNthValueFromMode, String)] = List(
            SqlNthValueFromMode.First -> "FROM FIRST",
            SqlNthValueFromMode.Last -> "FROM LAST",
        )
        for (m, sql) <- cases do
            assertEquals(createSql(_.printNthValueFromMode(m)), sql)

    test("match phase"):
        val cases: List[(SqlMatchPhase, String)] = List(
            SqlMatchPhase.Final -> "FINAL",
            SqlMatchPhase.Running -> "RUNNING",
        )
        for (p, sql) <- cases do
            assertEquals(createSql(_.printMatchPhase(p)), sql)

    test("window frame unit"):
        val cases: List[(SqlWindowFrameUnit, String)] = List(
            SqlWindowFrameUnit.Rows -> "ROWS",
            SqlWindowFrameUnit.Range -> "RANGE",
            SqlWindowFrameUnit.Groups -> "GROUPS",
        )
        for (u, sql) <- cases do
            assertEquals(createSql(_.printWindowFrameUnit(u)), sql)

    test("window frame bound"):
        val cases: List[(SqlWindowFrameBound, String)] = List(
            SqlWindowFrameBound.CurrentRow -> "CURRENT ROW",
            SqlWindowFrameBound.UnboundedPreceding -> "UNBOUNDED PRECEDING",
            SqlWindowFrameBound.UnboundedFollowing -> "UNBOUNDED FOLLOWING",
            SqlWindowFrameBound.Preceding(SqlExpr.NumberLiteral(1)) -> "1 PRECEDING",
            SqlWindowFrameBound.Following(SqlExpr.NumberLiteral(10)) -> "10 FOLLOWING",
        )
        for (b, sql) <- cases do
            assertEquals(createSql(_.printWindowFrameBound(b)), sql)

    test("window frame exclude mode"):
        val cases: List[(SqlWindowFrameExcludeMode, String)] = List(
            SqlWindowFrameExcludeMode.CurrentRow -> "CURRENT ROW",
            SqlWindowFrameExcludeMode.Group -> "GROUP",
            SqlWindowFrameExcludeMode.Ties -> "TIES",
            SqlWindowFrameExcludeMode.NoOthers -> "NO OTHERS",
        )
        for (m, sql) <- cases do
            assertEquals(createSql(_.printWindowFrameExcludeMode(m)), sql)

    test("window frame"):
        val cases: List[(SqlWindowFrame, String)] = List(
            SqlWindowFrame.Start(Nil, SqlWindowFrameUnit.Rows, SqlWindowFrameBound.CurrentRow, None, None) -> "ROWS CURRENT ROW",
            SqlWindowFrame.Start(Nil, SqlWindowFrameUnit.Rows, SqlWindowFrameBound.CurrentRow, Some(SqlWindowFrameExcludeMode.CurrentRow), None) -> "ROWS CURRENT ROW EXCLUDE CURRENT ROW",
            SqlWindowFrame.Between(Nil, SqlWindowFrameUnit.Rows, SqlWindowFrameBound.CurrentRow, SqlWindowFrameBound.UnboundedFollowing, None, None) -> "ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING",
            SqlWindowFrame.Between(Nil, SqlWindowFrameUnit.Rows, SqlWindowFrameBound.CurrentRow, SqlWindowFrameBound.UnboundedFollowing, Some(SqlWindowFrameExcludeMode.Group), None) -> "ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING EXCLUDE GROUP",
        )
        for (f, sql) <- cases do
            assertEquals(createSql(_.printWindowFrame(f)), sql)

    test("window"):
        val frame = SqlWindowFrame.Start(Nil, SqlWindowFrameUnit.Rows, SqlWindowFrameBound.CurrentRow, None, None)
        val item = SqlOrderingItem(SqlExpr.Column(None, "b"), None, None)
        val cases: List[(SqlWindow, String)] = List(
            SqlWindow.Inlined(None, Nil, Nil, None) -> "()",
            SqlWindow.Inlined(None, List(SqlExpr.Column(None, "a")), Nil, None) -> """(PARTITION BY "a")""",
            SqlWindow.Inlined(None, Nil, List(item), None) -> """(ORDER BY "b" ASC)""",
            SqlWindow.Inlined(None, List(SqlExpr.Column(None, "a")), List(item), None) -> """(PARTITION BY "a" ORDER BY "b" ASC)""",
            SqlWindow.Inlined(None, Nil, Nil, Some(frame)) -> "(ROWS CURRENT ROW)",
            SqlWindow.Inlined(None, List(SqlExpr.Column(None, "a")), List(item), Some(frame)) -> """(PARTITION BY "a" ORDER BY "b" ASC ROWS CURRENT ROW)""",
        )
        for (w, sql) <- cases do
            assertEquals(createSql(_.printWindow(w)), sql)

    test("column"):
        val cases: List[(SqlExpr.Column, String)] = List(
            SqlExpr.Column(None, "x") -> """"x"""",
            SqlExpr.Column(Some("t"), "x") -> """"t"."x"""",
            SqlExpr.Column(None, """x"; DROP TABLE users; --""") -> """"x""; DROP TABLE users; --"""",
        )
        for (c, sql) <- cases do
            assertEquals(createSql(_.printExpr(c)), sql)

    test("null literal"):
        assertEquals(createSql(_.printExpr(SqlExpr.NullLiteral)), "NULL")

    test("string literal"):
        val cases: List[(SqlExpr.StringLiteral, String)] = List(
            SqlExpr.StringLiteral("hello") -> "'hello'",
            SqlExpr.StringLiteral("") -> "''",
            SqlExpr.StringLiteral("it's") -> "'it''s'",
        )
        for (s, sql) <- cases do
            assertEquals(createSql(_.printExpr(s)), sql)

    test("number literal"):
        val cases: List[(SqlExpr.NumberLiteral, String)] = List(
            SqlExpr.NumberLiteral(BigDecimal(42)) -> "42",
            SqlExpr.NumberLiteral(BigDecimal("3.14")) -> "3.14",
        )
        for (n, sql) <- cases do
            assertEquals(createSql(_.printExpr(n)), sql)

    test("boolean literal"):
        val cases: List[(SqlExpr.BooleanLiteral, String)] = List(
            SqlExpr.BooleanLiteral(true) -> "TRUE",
            SqlExpr.BooleanLiteral(false) -> "FALSE",
        )
        for (b, sql) <- cases do
            assertEquals(createSql(_.printExpr(b)), sql)

    test("time literal"):
        val cases: List[(SqlExpr.TimeLiteral, String)] = List(
            SqlExpr.TimeLiteral(SqlTimeType.Date, "2023-01-01") -> "DATE '2023-01-01'",
            SqlExpr.TimeLiteral(SqlTimeType.Timestamp(None), "2023-01-01 12:00:00") -> "TIMESTAMP '2023-01-01 12:00:00'",
            SqlExpr.TimeLiteral(SqlTimeType.Time(None), "12:00:00") -> "TIME '12:00:00'",
        )
        for (t, sql) <- cases do
            assertEquals(createSql(_.printExpr(t)), sql)

    test("interval literal"):
        val cases: List[(SqlExpr.IntervalLiteral, String)] = List(
            SqlExpr.IntervalLiteral("1", SqlIntervalField.Single(SqlTimeUnit.Day)) -> "INTERVAL '1' DAY",
            SqlExpr.IntervalLiteral("1-6", SqlIntervalField.To(SqlTimeUnit.Year, SqlTimeUnit.Month)) -> "INTERVAL '1-6' YEAR TO MONTH",
        )
        for (i, sql) <- cases do
            assertEquals(createSql(_.printExpr(i)), sql)

    test("tuple"):
        val cases: List[(SqlExpr.Tuple, String)] = List(
            SqlExpr.Tuple(NonEmptyList(SqlExpr.NumberLiteral(1), Nil)) -> "(1)",
            SqlExpr.Tuple(NonEmptyList(SqlExpr.NumberLiteral(1), List(SqlExpr.NumberLiteral(2), SqlExpr.NumberLiteral(3)))) -> "(1, 2, 3)",
        )
        for (t, sql) <- cases do
            assertEquals(createSql(_.printExpr(t)), sql)

    test("array"):
        val inner1 = SqlExpr.Array(List(SqlExpr.NumberLiteral(1)))
        val inner2 = SqlExpr.Array(List(SqlExpr.NumberLiteral(2)))
        val cases: List[(SqlExpr.Array, String)] = List(
            SqlExpr.Array(List(SqlExpr.NumberLiteral(1))) -> "ARRAY[1]",
            SqlExpr.Array(List(SqlExpr.NumberLiteral(1), SqlExpr.NumberLiteral(2))) -> "ARRAY[1, 2]",
            SqlExpr.Array(List(inner1, inner2)) -> "ARRAY[ARRAY[1], ARRAY[2]]",
        )
        for (a, sql) <- cases do
            assertEquals(createSql(_.printExpr(a)), sql)

    test("unary"):
        val cases: List[(SqlExpr.Unary, String)] = List(
            SqlExpr.Unary(SqlUnaryOperator.Not, SqlExpr.Column(None, "x")) -> """NOT("x")""",
            SqlExpr.Unary(SqlUnaryOperator.Negative, SqlExpr.NumberLiteral(1)) -> "-(1)",
            SqlExpr.Unary(SqlUnaryOperator.Positive, SqlExpr.NumberLiteral(1)) -> "+(1)",
        )
        for (u, sql) <- cases do
            assertEquals(createSql(_.printExpr(u)), sql)

    test("binary"):
        val col = (n: String) => SqlExpr.Column(None, n)
        val colA = col("a") 
        val colB = col("b") 
        val colC = col("c")
        val cases: List[(SqlExpr.Binary, String)] = List(
            SqlExpr.Binary(SqlExpr.NumberLiteral(1), SqlBinaryOperator.Plus, SqlExpr.NumberLiteral(2)) -> "1 + 2",
            SqlExpr.Binary(SqlExpr.NumberLiteral(1), SqlBinaryOperator.Times, SqlExpr.NumberLiteral(2)) -> "1 * 2",
            SqlExpr.Binary(SqlExpr.Binary(colA, SqlBinaryOperator.Plus, colB), SqlBinaryOperator.Times, colC) -> """("a" + "b") * "c"""",
            SqlExpr.Binary(colA, SqlBinaryOperator.Times, SqlExpr.Binary(colB, SqlBinaryOperator.Plus, colC)) -> """"a" * ("b" + "c")""",
            SqlExpr.Binary(SqlExpr.Binary(colA, SqlBinaryOperator.Times, colB), SqlBinaryOperator.Plus, colC) -> """"a" * "b" + "c"""",
            SqlExpr.Binary(colA, SqlBinaryOperator.Plus, SqlExpr.Binary(colB, SqlBinaryOperator.Times, colC)) -> """"a" + "b" * "c"""",
        )
        for (b, sql) <- cases do
            assertEquals(createSql(_.printExpr(b)), sql)

    test("json test"):
        val col = SqlExpr.Column(None, "x")
        val cases: List[(SqlExpr.JsonTest, String)] = List(
            SqlExpr.JsonTest(col, None, None, false) -> """"x" IS JSON""",
            SqlExpr.JsonTest(col, None, Some(SqlJsonUniquenessMode.With), false) -> """"x" IS JSON WITH UNIQUE KEYS""",
            SqlExpr.JsonTest(col, Some(SqlJsonNodeType.Value), None, false) -> """"x" IS JSON VALUE""",
            SqlExpr.JsonTest(col, Some(SqlJsonNodeType.Value), Some(SqlJsonUniquenessMode.With), false) -> """"x" IS JSON VALUE WITH UNIQUE KEYS""",
            SqlExpr.JsonTest(col, None, None, true) -> """"x" IS NOT JSON""",
            SqlExpr.JsonTest(col, None, Some(SqlJsonUniquenessMode.With), true) -> """"x" IS NOT JSON WITH UNIQUE KEYS""",
            SqlExpr.JsonTest(col, Some(SqlJsonNodeType.Value), None, true) -> """"x" IS NOT JSON VALUE""",
            SqlExpr.JsonTest(col, Some(SqlJsonNodeType.Value), Some(SqlJsonUniquenessMode.With), true) -> """"x" IS NOT JSON VALUE WITH UNIQUE KEYS""",
        )
        for (j, sql) <- cases do
            assertEquals(createSql(_.printExpr(j)), sql)

    test("in"):
        val col = SqlExpr.Column(None, "x")
        val cases: List[(SqlExpr.In, String)] = List(
            SqlExpr.In(col, SqlInRightOperand.Values(NonEmptyList(SqlExpr.NumberLiteral(1), Nil)), false) -> """"x" IN (1)""",
            SqlExpr.In(col, SqlInRightOperand.Values(NonEmptyList(SqlExpr.NumberLiteral(1), List(SqlExpr.NumberLiteral(2)))), true) -> """"x" NOT IN (1, 2)""",
        )
        for (i, sql) <- cases do
            assertEquals(createSql(_.printExpr(i)), sql)

    test("between"):
        val col = SqlExpr.Column(None, "x")
        val cases: List[(SqlExpr.Between, String)] = List(
            SqlExpr.Between(col, SqlExpr.NumberLiteral(1), SqlExpr.NumberLiteral(10), false) -> """"x" BETWEEN 1 AND 10""",
            SqlExpr.Between(col, SqlExpr.NumberLiteral(1), SqlExpr.NumberLiteral(10), true) -> """"x" NOT BETWEEN 1 AND 10""",
        )
        for (b, sql) <- cases do
            assertEquals(createSql(_.printExpr(b)), sql)

    test("like"):
        val col = SqlExpr.Column(None, "x")
        val cases: List[(SqlExpr.Like, String)] = List(
            SqlExpr.Like(col, SqlExpr.StringLiteral("%abc%"), None, false) -> """"x" LIKE '%abc%'""",
            SqlExpr.Like(col, SqlExpr.StringLiteral("%abc%"), None, true) -> """"x" NOT LIKE '%abc%'""",
            SqlExpr.Like(col, SqlExpr.StringLiteral("%abc%"), Some(SqlExpr.StringLiteral("\\")), true) -> """"x" NOT LIKE '%abc%' ESCAPE '\'""",
        )
        for (l, sql) <- cases do
            assertEquals(createSql(_.printExpr(l)), sql)

    test("similar to"):
        val col = SqlExpr.Column(None, "x")
        val cases: List[(SqlExpr.SimilarTo, String)] = List(
            SqlExpr.SimilarTo(col, SqlExpr.StringLiteral("%abc%"), None, false) -> """"x" SIMILAR TO '%abc%'""",
            SqlExpr.SimilarTo(col, SqlExpr.StringLiteral("%abc%"), None, true) -> """"x" NOT SIMILAR TO '%abc%'""",
        )
        for (s, sql) <- cases do
            assertEquals(createSql(_.printExpr(s)), sql)

    test("case"):
        val col = (n: String) => SqlExpr.Column(None, n)
        val cases: List[(SqlExpr.Case, String)] = List(
            SqlExpr.Case(NonEmptyList(SqlCaseBranch(col("a"), col("b")), Nil), None) -> """CASE WHEN "a" THEN "b" END""",
            SqlExpr.Case(NonEmptyList(SqlCaseBranch(col("a"), col("b")), List(SqlCaseBranch(col("c"), col("d")))), Some(col("e"))) -> """CASE WHEN "a" THEN "b" WHEN "c" THEN "d" ELSE "e" END""",
        )
        for (c, sql) <- cases do
            assertEquals(createSql(_.printExpr(c)), sql)

    test("simple case"):
        val col = (n: String) => SqlExpr.Column(None, n)
        val cases: List[(SqlExpr.SimpleCase, String)] = List(
            SqlExpr.SimpleCase(col("x"), NonEmptyList(SqlCaseBranch(col("a"), col("b")), Nil), None) -> """CASE "x" WHEN "a" THEN "b" END""",
            SqlExpr.SimpleCase(col("x"), NonEmptyList(SqlCaseBranch(col("a"), col("b")), List(SqlCaseBranch(col("c"), col("d")))), Some(col("e"))) -> """CASE "x" WHEN "a" THEN "b" WHEN "c" THEN "d" ELSE "e" END""",
        )
        for (s, sql) <- cases do
            assertEquals(createSql(_.printExpr(s)), sql)

    test("coalesce"):
        val cases: List[(SqlExpr.Coalesce, String)] = List(
            SqlExpr.Coalesce(NonEmptyList(SqlExpr.NumberLiteral(1), Nil)) -> "COALESCE(1)",
            SqlExpr.Coalesce(NonEmptyList(SqlExpr.NumberLiteral(1), List(SqlExpr.NumberLiteral(2), SqlExpr.NumberLiteral(3)))) -> "COALESCE(1, 2, 3)",
        )
        for (c, sql) <- cases do
            assertEquals(createSql(_.printExpr(c)), sql)

    test("nullif"):
        val col = SqlExpr.Column(None, "x")
        assertEquals(createSql(_.printExpr(SqlExpr.NullIf(col, SqlExpr.Column(None, "y")))), """NULLIF("x", "y")""")

    test("cast"):
        val col = SqlExpr.Column(None, "x")
        val cases: List[(SqlExpr.Cast, String)] = List(
            SqlExpr.Cast(col, SqlType.Int) -> """CAST("x" AS INTEGER)""",
            SqlExpr.Cast(col, SqlType.Varchar(Some(255))) -> """CAST("x" AS VARCHAR(255))""",
        )
        for (c, sql) <- cases do
            assertEquals(createSql(_.printExpr(c)), sql)

    test("window expr"):
        val col = SqlExpr.CountAsteriskFunc(None, None)
        val cases: List[(SqlExpr.Window, String)] = List(
            SqlExpr.Window(col, SqlWindow.Inlined(None, Nil, Nil, None)) -> """COUNT(*) OVER ()""",
            SqlExpr.Window(col, SqlWindow.Inlined(None, List(SqlExpr.Column(None, "y")), Nil, None)) -> """COUNT(*) OVER (PARTITION BY "y")""",
        )
        for (w, sql) <- cases do
            assertEquals(createSql(_.printExpr(w)), sql)

    test("subquery"):
        val q = SqlQuery.Values(NonEmptyList(NonEmptyList(SqlExpr.NumberLiteral(1), Nil), Nil), Nil, None)
        assertEquals(createSql(_.printExpr(SqlExpr.Subquery(q))), "(\n    VALUES (1)\n)")

    test("exists predicate"):
        val q = SqlQuery.Values(NonEmptyList(NonEmptyList(SqlExpr.NumberLiteral(1), Nil), Nil), Nil, None)
        assertEquals(createSql(_.printExpr(SqlExpr.ExistsPredicate(q))), "EXISTS(\n    VALUES (1)\n)")

    test("quantified comparison predicate"):
        val col = SqlExpr.Column(None, "x")
        val q = SqlQuery.Values(NonEmptyList(NonEmptyList(SqlExpr.NumberLiteral(1), Nil), Nil), Nil, None)
        assertEquals(createSql(_.printExpr(SqlExpr.QuantifiedComparisonPredicate(col, SqlQuantifiedComparisonOperator.Equal, SqlSubqueryQuantifier.Any, q))), "\"x\" = ANY(\n    VALUES (1)\n)")

    test("grouping"):
        val col = SqlExpr.Column(None, "x")
        assertEquals(createSql(_.printExpr(SqlExpr.Grouping(NonEmptyList(col, List(SqlExpr.Column(None, "y")))))), """GROUPING("x", "y")""")

    test("ident func"):
        assertEquals(createSql(_.printExpr(SqlExpr.IdentFunc("FUNC"))), "FUNC")
        assertEquals(createSql(_.printExpr(SqlExpr.IdentFunc("FUNC \""))), """FUNC""")

    test("substring func"):
        val col = SqlExpr.Column(None, "x")
        val cases: List[(SqlExpr.SubstringFunc, String)] = List(
            SqlExpr.SubstringFunc(col, SqlExpr.NumberLiteral(2), None) -> """SUBSTRING("x" FROM 2)""",
            SqlExpr.SubstringFunc(col, SqlExpr.NumberLiteral(2), Some(SqlExpr.NumberLiteral(3))) -> """SUBSTRING("x" FROM 2 FOR 3)""",
        )
        for (s, sql) <- cases do
            assertEquals(createSql(_.printExpr(s)), sql)

    test("trim func"):
        val col = SqlExpr.Column(None, "x")
        val cases: List[(SqlExpr.TrimFunc, String)] = List(
            SqlExpr.TrimFunc(None, col) -> """TRIM("x")""",
            SqlExpr.TrimFunc(Some(SqlTrim(None, Some(SqlExpr.Column(None, "y")))), col) -> """TRIM("y" FROM "x")""",
        )
        for (t, sql) <- cases do
            assertEquals(createSql(_.printExpr(t)), sql)

    test("overlay func"):
        val col = SqlExpr.Column(None, "x")
        val cases: List[(SqlExpr.OverlayFunc, String)] = List(
            SqlExpr.OverlayFunc(col, SqlExpr.Column(None, "y"), SqlExpr.NumberLiteral(2), None) -> """OVERLAY("x" PLACING "y" FROM 2)""",
            SqlExpr.OverlayFunc(col, SqlExpr.Column(None, "y"), SqlExpr.NumberLiteral(2), Some(SqlExpr.NumberLiteral(3))) -> """OVERLAY("x" PLACING "y" FROM 2 FOR 3)""",
        )
        for (o, sql) <- cases do
            assertEquals(createSql(_.printExpr(o)), sql)

    test("position func"):
        assertEquals(createSql(_.printExpr(SqlExpr.PositionFunc(SqlExpr.Column(None, "x"), SqlExpr.Column(None, "y")))), """POSITION("x" IN "y")""")

    test("extract func"):
        assertEquals(createSql(_.printExpr(SqlExpr.ExtractFunc(SqlTimeUnit.Year, SqlExpr.Column(None, "x")))), """EXTRACT(YEAR FROM "x")""")

    test("json serialize func"):
        val col = SqlExpr.Column(None, "x")
        val cases: List[(SqlExpr.JsonSerializeFunc, String)] = List(
            SqlExpr.JsonSerializeFunc(col, None) -> """JSON_SERIALIZE("x")""",
            SqlExpr.JsonSerializeFunc(col, Some(SqlJsonOutput(SqlType.Int, None))) -> """JSON_SERIALIZE("x" RETURNING INTEGER)""",
        )
        for (j, sql) <- cases do
            assertEquals(createSql(_.printExpr(j)), sql)

    test("json parse func"):
        val col = SqlExpr.Column(None, "x")
        val cases: List[(SqlExpr.JsonParseFunc, String)] = List(
            SqlExpr.JsonParseFunc(col, None, None) -> """JSON("x")""",
            SqlExpr.JsonParseFunc(col, Some(SqlJsonInput(None)), None) -> """JSON("x" FORMAT JSON)""",
            SqlExpr.JsonParseFunc(col, None, Some(SqlJsonUniquenessMode.With)) -> """JSON("x" WITH UNIQUE KEYS)""",
            SqlExpr.JsonParseFunc(col, Some(SqlJsonInput(None)), Some(SqlJsonUniquenessMode.With)) -> """JSON("x" FORMAT JSON WITH UNIQUE KEYS)""",
        )
        for (j, sql) <- cases do
            assertEquals(createSql(_.printExpr(j)), sql)

    test("json query func"):
        val col = SqlExpr.Column(None, "x")
        val path = SqlExpr.StringLiteral("$.key")
        val passingVariants: List[(List[SqlJsonPassingItem], String)] = List(
            Nil -> "",
            List(SqlJsonPassingItem(col, "alias")) -> """ PASSING "x" AS "alias"""",
            List(SqlJsonPassingItem(col, "a"), SqlJsonPassingItem(col, "b")) -> """ PASSING "x" AS "a", "x" AS "b"""",
        )
        val outputVariants: List[(Option[SqlJsonOutput], String)] = List(
            None -> "",
            Some(SqlJsonOutput(SqlType.Int, None)) -> " RETURNING INTEGER",
        )
        val wrapperVariants: List[(Option[SqlJsonQueryWrapperBehavior], String)] = List(
            None -> "",
            Some(SqlJsonQueryWrapperBehavior.Without(false)) -> " WITHOUT WRAPPER",
        )
        val quotesVariants: List[(Option[SqlJsonQueryQuotesBehavior], String)] = List(
            None -> "",
            Some(SqlJsonQueryQuotesBehavior(SqlJsonQueryQuotesBehaviorMode.Keep, false)) -> " KEEP QUOTES",
        )
        val onEmptyVariants: List[(Option[SqlJsonQueryEmptyBehavior], String)] = List(
            None -> "",
            Some(SqlJsonQueryEmptyBehavior.Error) -> " ERROR ON EMPTY",
        )
        val onErrorVariants: List[(Option[SqlJsonQueryErrorBehavior], String)] = List(
            None -> "",
            Some(SqlJsonQueryErrorBehavior.Null) -> " NULL ON ERROR",
        )
        for
            (passing, pStr) <- passingVariants
            (output, oStr) <- outputVariants
            (wrapper, wStr) <- wrapperVariants
            (quotes, qStr) <- quotesVariants
            (onEmpty, eStr) <- onEmptyVariants
            (onError, rStr) <- onErrorVariants
        do
            val expr = SqlExpr.JsonQueryFunc(col, path, passing, output, wrapper, quotes, onEmpty, onError)
            val expected = s"""JSON_QUERY("x", '$$.key'$pStr$oStr$wStr$qStr$eStr$rStr)"""
            assertEquals(createSql(_.printExpr(expr)), expected)

    test("json value func"):
        val col = SqlExpr.Column(None, "x")
        val path = SqlExpr.StringLiteral("$.key")
        val passingVariants: List[(List[SqlJsonPassingItem], String)] = List(
            Nil -> "",
            List(SqlJsonPassingItem(col, "alias")) -> """ PASSING "x" AS "alias"""",
            List(SqlJsonPassingItem(col, "a"), SqlJsonPassingItem(col, "b")) -> """ PASSING "x" AS "a", "x" AS "b"""",
        )
        val outputVariants: List[(Option[SqlJsonOutput], String)] = List(
            None -> "",
            Some(SqlJsonOutput(SqlType.Int, None)) -> " RETURNING INTEGER",
        )
        val onEmptyVariants: List[(Option[SqlJsonValueEmptyBehavior], String)] = List(
            None -> "",
            Some(SqlJsonValueEmptyBehavior.Null) -> " NULL ON EMPTY",
        )
        val onErrorVariants: List[(Option[SqlJsonValueErrorBehavior], String)] = List(
            None -> "",
            Some(SqlJsonValueErrorBehavior.Error) -> " ERROR ON ERROR",
        )
        for
            (passing, pStr) <- passingVariants
            (output, oStr) <- outputVariants
            (onEmpty, eStr) <- onEmptyVariants
            (onError, rStr) <- onErrorVariants
        do
            val expr = SqlExpr.JsonValueFunc(col, path, passing, output, onEmpty, onError)
            val expected = s"""JSON_VALUE("x", '$$.key'$pStr$oStr$eStr$rStr)"""
            assertEquals(createSql(_.printExpr(expr)), expected)

    test("json object func"):
        val col = SqlExpr.Column(None, "x")
        val itemsVariants: List[(List[SqlJsonObjectItem], String)] = List(
            Nil -> "",
            List(SqlJsonObjectItem(col, col)) -> """"x" VALUE "x"""",
            List(SqlJsonObjectItem(col, col), SqlJsonObjectItem(SqlExpr.Column(None, "k2"), SqlExpr.Column(None, "v2"))) -> """"x" VALUE "x", "k2" VALUE "v2"""",
        )
        val nullConstructorVariants: List[(Option[SqlJsonNullConstructor], String)] = List(
            None -> "",
            Some(SqlJsonNullConstructor.Null) -> " NULL ON NULL",
        )
        val uniquenessModeVariants: List[(Option[SqlJsonUniquenessMode], String)] = List(
            None -> "",
            Some(SqlJsonUniquenessMode.With) -> " WITH UNIQUE KEYS",
        )
        val outputVariants: List[(Option[SqlJsonOutput], String)] = List(
            None -> "",
            Some(SqlJsonOutput(SqlType.Int, None)) -> " RETURNING INTEGER",
        )
        for
            (items, iStr) <- itemsVariants
            (nullConstructor, nStr) <- nullConstructorVariants
            (uniquenessMode, uStr) <- uniquenessModeVariants
            (output, oStr) <- outputVariants
        do
            val expr = SqlExpr.JsonObjectFunc(items, nullConstructor, uniquenessMode, output)
            val expected = s"JSON_OBJECT($iStr$nStr$uStr$oStr)"
            assertEquals(createSql(_.printExpr(expr)), expected)

    test("json array func"):
        val col = SqlExpr.Column(None, "x")
        val itemsVariants: List[(List[SqlJsonArrayItem], String)] = List(
            Nil -> "",
            List(SqlJsonArrayItem(col, None)) -> """"x"""",
            List(SqlJsonArrayItem(col, None), SqlJsonArrayItem(SqlExpr.Column(None, "y"), None)) -> """"x", "y"""",
        )
        val nullConstructorVariants: List[(Option[SqlJsonNullConstructor], String)] = List(
            None -> "",
            Some(SqlJsonNullConstructor.Null) -> " NULL ON NULL",
        )
        val outputVariants: List[(Option[SqlJsonOutput], String)] = List(
            None -> "",
            Some(SqlJsonOutput(SqlType.Int, None)) -> " RETURNING INTEGER",
        )
        for
            (items, iStr) <- itemsVariants
            (nullConstructor, nStr) <- nullConstructorVariants
            (output, oStr) <- outputVariants
        do
            val expr = SqlExpr.JsonArrayFunc(items, nullConstructor, output)
            val expected = s"JSON_ARRAY($iStr$nStr$oStr)"
            assertEquals(createSql(_.printExpr(expr)), expected)

    test("json exists func"):
        val col = SqlExpr.Column(None, "x")
        val path = SqlExpr.StringLiteral("$.key")
        val passingVariants: List[(List[SqlJsonPassingItem], String)] = List(
            Nil -> "",
            List(SqlJsonPassingItem(col, "alias")) -> """ PASSING "x" AS "alias"""",
            List(SqlJsonPassingItem(col, "a"), SqlJsonPassingItem(col, "b")) -> """ PASSING "x" AS "a", "x" AS "b"""",
        )
        val onErrorVariants: List[(Option[SqlJsonExistsErrorBehavior], String)] = List(
            None -> "",
            Some(SqlJsonExistsErrorBehavior.True) -> " TRUE ON ERROR",
        )
        for
            (passing, pStr) <- passingVariants
            (onError, eStr) <- onErrorVariants
        do
            val expr = SqlExpr.JsonExistsFunc(col, path, passing, onError)
            val expected = s"""JSON_EXISTS("x", '$$.key'$pStr$eStr)"""
            assertEquals(createSql(_.printExpr(expr)), expected)

    test("count asterisk func"):
        val cases: List[(SqlExpr.CountAsteriskFunc, String)] = List(
            SqlExpr.CountAsteriskFunc(None, None) -> "COUNT(*)",
            SqlExpr.CountAsteriskFunc(Some("t"), None) -> """COUNT("t".*)""",
            SqlExpr.CountAsteriskFunc(None, Some(SqlExpr.BooleanLiteral(true))) -> """COUNT(*) FILTER (WHERE TRUE)""",
            SqlExpr.CountAsteriskFunc(Some("t"), Some(SqlExpr.BooleanLiteral(true))) -> """COUNT("t".*) FILTER (WHERE TRUE)""",
        )
        for (c, sql) <- cases do
            assertEquals(createSql(_.printExpr(c)), sql)

    test("list agg func"):
        val col = SqlExpr.Column(None, "x")
        val sep = SqlExpr.StringLiteral(",")
        val quantifierVariants: List[(Option[SqlQuantifier], String)] = List(
            None -> "",
            Some(SqlQuantifier.Distinct) -> "DISTINCT ",
        )
        val onOverflowVariants: List[(Option[SqlListAggOnOverflow], String)] = List(
            None -> "",
            Some(SqlListAggOnOverflow.Error) -> " ON OVERFLOW ERROR",
        )
        val withinGroupVariants: List[(List[SqlOrderingItem], String)] = List(
            Nil -> "",
            List(SqlOrderingItem(SqlExpr.Column(None, "b"), None, None)) -> """ WITHIN GROUP (ORDER BY "b" ASC)""",
        )
        val filterVariants: List[(Option[SqlExpr], String)] = List(
            None -> "",
            Some(SqlExpr.BooleanLiteral(true)) -> """ FILTER (WHERE TRUE)""",
        )
        for
            (quantifier, qStr) <- quantifierVariants
            (onOverflow, oStr) <- onOverflowVariants
            (withinGroup, wStr) <- withinGroupVariants
            (filter, fStr) <- filterVariants
        do
            val expr = SqlExpr.ListAggFunc(quantifier, col, sep, onOverflow, withinGroup, filter)
            val expected = s"""LISTAGG($qStr"x", ','$oStr)$wStr$fStr"""
            assertEquals(createSql(_.printExpr(expr)), expected)

    test("json object agg func"):
        val col = SqlExpr.Column(None, "x")
        val nullConstructorVariants: List[(Option[SqlJsonNullConstructor], String)] = List(
            None -> "",
            Some(SqlJsonNullConstructor.Null) -> " NULL ON NULL",
        )
        val uniquenessModeVariants: List[(Option[SqlJsonUniquenessMode], String)] = List(
            None -> "",
            Some(SqlJsonUniquenessMode.With) -> " WITH UNIQUE KEYS",
        )
        val outputVariants: List[(Option[SqlJsonOutput], String)] = List(
            None -> "",
            Some(SqlJsonOutput(SqlType.Int, None)) -> " RETURNING INTEGER",
        )
        val filterVariants: List[(Option[SqlExpr], String)] = List(
            None -> "",
            Some(SqlExpr.BooleanLiteral(true)) -> """ FILTER (WHERE TRUE)""",
        )
        for
            (nullConstructor, nStr) <- nullConstructorVariants
            (uniquenessMode, uStr) <- uniquenessModeVariants
            (output, oStr) <- outputVariants
            (filter, fStr) <- filterVariants
        do
            val expr = SqlExpr.JsonObjectAggFunc(SqlJsonObjectItem(col, col), nullConstructor, uniquenessMode, output, filter)
            val expected = s"""JSON_OBJECTAGG("x" VALUE "x"$nStr$uStr$oStr)$fStr"""
            assertEquals(createSql(_.printExpr(expr)), expected)

    test("json array agg func"):
        val col = SqlExpr.Column(None, "x")
        val orderByVariants: List[(List[SqlOrderingItem], String)] = List(
            Nil -> "",
            List(SqlOrderingItem(SqlExpr.Column(None, "b"), None, None)) -> """ ORDER BY "b" ASC""",
            List(SqlOrderingItem(SqlExpr.Column(None, "a"), None, None), SqlOrderingItem(SqlExpr.Column(None, "b"), None, None)) -> """ ORDER BY "a" ASC, "b" ASC""",
        )
        val nullConstructorVariants: List[(Option[SqlJsonNullConstructor], String)] = List(
            None -> "",
            Some(SqlJsonNullConstructor.Null) -> " NULL ON NULL",
        )
        val outputVariants: List[(Option[SqlJsonOutput], String)] = List(
            None -> "",
            Some(SqlJsonOutput(SqlType.Int, None)) -> " RETURNING INTEGER",
        )
        val filterVariants: List[(Option[SqlExpr], String)] = List(
            None -> "",
            Some(SqlExpr.BooleanLiteral(true)) -> """ FILTER (WHERE TRUE)""",
        )
        for
            (orderBy, bStr) <- orderByVariants
            (nullConstructor, nStr) <- nullConstructorVariants
            (output, oStr) <- outputVariants
            (filter, fStr) <- filterVariants
        do
            val expr = SqlExpr.JsonArrayAggFunc(SqlJsonArrayItem(col, None), orderBy, nullConstructor, output, filter)
            val expected = s"""JSON_ARRAYAGG("x"$bStr$nStr$oStr)$fStr"""
            assertEquals(createSql(_.printExpr(expr)), expected)

    test("nulls treatment func"):
        val col = SqlExpr.Column(None, "x")
        val cases: List[(SqlExpr.NullsTreatmentFunc, String)] = List(
            SqlExpr.NullsTreatmentFunc("LAG", List(col), None) -> """LAG("x")""",
            SqlExpr.NullsTreatmentFunc("LAG", List(col), Some(SqlWindowNullsMode.Respect)) -> """LAG("x") RESPECT NULLS""",
        )
        for (n, sql) <- cases do
            assertEquals(createSql(_.printExpr(n)), sql)

    test("nth value func"):
        val col = SqlExpr.Column(None, "x")
        val fromModeVariants: List[(Option[SqlNthValueFromMode], String)] = List(
            None -> "",
            Some(SqlNthValueFromMode.First) -> " FROM FIRST",
        )
        val nullsModeVariants: List[(Option[SqlWindowNullsMode], String)] = List(
            None -> "",
            Some(SqlWindowNullsMode.Respect) -> " RESPECT NULLS",
        )
        for
            (fromMode, fStr) <- fromModeVariants
            (nullsMode, nStr) <- nullsModeVariants
        do
            val expr = SqlExpr.NthValueFunc(col, SqlExpr.NumberLiteral(3), fromMode, nullsMode)
            val expected = s"""NTH_VALUE("x", 3)$fStr$nStr"""
            assertEquals(createSql(_.printExpr(expr)), expected)

    test("general func"):
        val col = SqlExpr.Column(None, "x")
        val quantifierVariants: List[(Option[SqlQuantifier], String)] = List(
            None -> "",
            Some(SqlQuantifier.All) -> "ALL ",
        )
        val argsVariants: List[(List[SqlExpr], String)] = List(
            Nil -> "",
            List(col) -> """"x"""",
            List(col, SqlExpr.Column(None, "y")) -> """"x", "y"""",
        )
        val orderByVariants: List[(List[SqlOrderingItem], String)] = List(
            Nil -> "",
            List(SqlOrderingItem(SqlExpr.Column(None, "b"), None, None)) -> """ORDER BY "b" ASC""",
        )
        val withinGroupVariants: List[(List[SqlOrderingItem], String)] = List(
            Nil -> "",
            List(SqlOrderingItem(SqlExpr.Column(None, "b"), None, None)) -> """ WITHIN GROUP (ORDER BY "b" ASC)""",
        )
        val filterVariants: List[(Option[SqlExpr], String)] = List(
            None -> "",
            Some(SqlExpr.BooleanLiteral(true)) -> """ FILTER (WHERE TRUE)""",
        )
        for
            (quantifier, qStr) <- quantifierVariants
            (args, aStr) <- argsVariants
            (orderBy, bStr) <- orderByVariants
            (withinGroup, wStr) <- withinGroupVariants
            (filter, fStr) <- filterVariants
        do
            val expr = SqlExpr.GeneralFunc(quantifier, "FUNC", args, orderBy, withinGroup, filter)
            val inner = qStr + aStr + (if bStr.nonEmpty then " " + bStr else "")
            val expected = s"FUNC($inner)$wStr$fStr"
            assertEquals(createSql(_.printExpr(expr)), expected)

    test("match phase"):
        assertEquals(createSql(_.printExpr(SqlExpr.MatchPhase(SqlMatchPhase.Final, SqlExpr.Column(None, "x")))) , """FINAL "x"""")

    test("unsafe custom"):
        val cases: List[(SqlExpr.UnsafeCustom, String)] = List(
            SqlExpr.UnsafeCustom(List(
                SqlUnsafeCustomToken.Keyword("MATCH("),
                SqlUnsafeCustomToken.Expr(SqlExpr.Column(None, "title")),
                SqlUnsafeCustomToken.Keyword(", "),
                SqlUnsafeCustomToken.Expr(SqlExpr.Column(None, "body")),
                SqlUnsafeCustomToken.Keyword(") AGAINST ("),
                SqlUnsafeCustomToken.Expr(SqlExpr.StringLiteral("search text")),
                SqlUnsafeCustomToken.Keyword("IN BOOLEAN MODE)"),
            )) -> """(MATCH( "title" ,  "body" ) AGAINST ( 'search text' IN BOOLEAN MODE))""",
        )
        for (c, sql) <- cases do
            assertEquals(createSql(_.printExpr(c)), sql)