import sqala.ast.expr.*
import sqala.ast.statement.SqlQuery
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
            SqlInRightOperand.Subquery(SqlQuery.Values(NonEmptyList(NonEmptyList(SqlExpr.NumberLiteral(1), Nil), Nil), None)) -> "(VALUES (1))",
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