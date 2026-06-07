package sqala.ast.expr

/**
 * A `WHEN ... THEN ...` branch in a `CASE` expression.
 *
 * Renders as `WHEN expr THEN expr`.
 */
case class SqlCaseBranch(when: SqlExpr, `then`: SqlExpr)

/**
 * A `TRIM` specification with optional mode and value.
 *
 * Renders as `[BOTH|LEADING|TRAILING] [expr]`.
 */
case class SqlTrim(mode: Option[SqlTrimMode], value: Option[SqlExpr])

/**
 * Trim mode for the `TRIM` function.
 */
enum SqlTrimMode:
    /**
     * Trim both sides.
     *
     * Renders as `BOTH`.
     */
    case Both

    /**
     * Trim leading characters.
     *
     * Renders as `LEADING`.
     */
    case Leading

    /**
     * Trim trailing characters.
     *
     * Renders as `TRAILING`.
     */
    case Trailing

/**
 * Uniqueness constraint for JSON keys.
 */
enum SqlJsonUniquenessMode:
    /**
     * Require unique keys.
     *
     * Renders as `WITH UNIQUE KEYS`.
     */
    case With

    /**
     * Allow duplicate keys.
     *
     * Renders as `WITHOUT UNIQUE KEYS`.
     */
    case Without

/**
 * A `PASSING` argument for JSON path expressions.
 *
 * Renders as `expr AS "alias"`.
 */
case class SqlJsonPassing(expr: SqlExpr, alias: String)

/**
 * JSON node type check.
 */
enum SqlJsonNodeType:
    /**
     * JSON value.
     *
     * Renders as `VALUE`.
     */
    case Value

    /**
     * JSON object.
     *
     * Renders as `OBJECT`.
     */
    case Object

    /**
     * JSON array.
     *
     * Renders as `ARRAY`.
     */
    case Array

    /**
     * JSON scalar.
     *
     * Renders as `SCALAR`.
     */
    case Scalar

/**
 * Character encoding for JSON format clause.
 */
enum SqlJsonEncoding:
    /**
     * UTF-8 encoding.
     *
     * Renders as `UTF8`.
     */
    case Utf8

    /**
     * UTF-16 encoding.
     *
     * Renders as `UTF16`.
     */
    case Utf16

    /**
     * UTF-32 encoding.
     *
     * Renders as `UTF32`.
     */
    case Utf32

    /**
     * A custom encoding with a free-form name.
     *
     * Renders as the given `encoding` string directly.
     */
    case Custom(encoding: String)

/**
 * Null handling mode for JSON functions.
 */
enum SqlJsonNullConstructor:
    /**
     * Include NULL values.
     *
     * Renders as `NULL ON NULL`.
     */
    case Null

    /**
     * Omit NULL values.
     *
     * Renders as `ABSENT ON NULL`.
     */
    case Absent

/**
 * Wrapper behavior for `JSON_QUERY` and JSON table columns.
 */
enum SqlJsonQueryWrapperBehavior:
    /**
     * Wrap the result, optionally with a mode and array wrapper.
     *
     * Renders as `WITH [CONDITIONAL|UNCONDITIONAL] [ARRAY] WRAPPER`.
     */
    case With(mode: Option[SqlJsonQueryWrapperBehaviorMode], array: Boolean)

    /**
     * Do not wrap the result, optionally without an array wrapper.
     *
     * Renders as `WITHOUT [ARRAY] WRAPPER`.
     */
    case Without(array: Boolean)

/**
 * Wrapper behavior mode for `JSON_QUERY`.
 */
enum SqlJsonQueryWrapperBehaviorMode:
    /**
     * Conditional wrapper.
     *
     * Renders as `CONDITIONAL`.
     */
    case Conditional

    /**
     * Unconditional wrapper.
     *
     * Renders as `UNCONDITIONAL`.
     */
    case Unconditional

/**
 * Quotes behavior for `JSON_QUERY` and JSON table columns.
 *
 * Renders as `[KEEP|OMIT] QUOTES [ON SCALAR STRING]`.
 */
case class SqlJsonQueryQuotesBehavior(mode: SqlJsonQueryQuotesBehaviorMode, onScalarString: Boolean)

/**
 * Quotes behavior mode.
 */
enum SqlJsonQueryQuotesBehaviorMode:
    /**
     * Keep quotes on string values.
     *
     * Renders as `KEEP QUOTES`.
     */
    case Keep

    /**
     * Omit quotes on string values.
     *
     * Renders as `OMIT QUOTES`.
     */
    case Omit

/**
 * `ON EMPTY` behavior for `JSON_QUERY`.
 */
enum SqlJsonQueryEmptyBehavior:
    /**
     * Raise an error.
     *
     * Renders as `ERROR ON EMPTY`.
     */
    case Error

    /**
     * Return NULL.
     *
     * Renders as `NULL ON EMPTY`.
     */
    case Null

    /**
     * Return an empty object.
     *
     * Renders as `EMPTY OBJECT ON EMPTY`.
     */
    case EmptyObject

    /**
     * Return an empty array.
     *
     * Renders as `EMPTY ARRAY ON EMPTY`.
     */
    case EmptyArray

    /**
     * Return a default expression.
     *
     * Renders as `DEFAULT expr ON EMPTY`.
     */
    case Default(expr: SqlExpr)

/**
 * `ON ERROR` behavior for `JSON_QUERY`.
 */
enum SqlJsonQueryErrorBehavior:
    /**
     * Raise an error.
     *
     * Renders as `ERROR ON ERROR`.
     */
    case Error

    /**
     * Return NULL.
     *
     * Renders as `NULL ON ERROR`.
     */
    case Null

    /**
     * Return an empty object.
     *
     * Renders as `EMPTY OBJECT ON ERROR`.
     */
    case EmptyObject

    /**
     * Return an empty array.
     *
     * Renders as `EMPTY ARRAY ON ERROR`.
     */
    case EmptyArray

    /**
     * Return a default expression.
     *
     * Renders as `DEFAULT expr ON ERROR`.
     */
    case Default(expr: SqlExpr)

/**
 * `ON EMPTY` behavior for `JSON_VALUE`.
 */
enum SqlJsonValueEmptyBehavior:
    /**
     * Raise an error.
     *
     * Renders as `ERROR ON EMPTY`.
     */
    case Error

    /**
     * Return NULL.
     *
     * Renders as `NULL ON EMPTY`.
     */
    case Null

    /**
     * Return a default expression.
     *
     * Renders as `DEFAULT expr ON EMPTY`.
     */
    case Default(expr: SqlExpr)

/**
 * `ON ERROR` behavior for `JSON_VALUE`.
 */
enum SqlJsonValueErrorBehavior:
    /**
     * Raise an error.
     *
     * Renders as `ERROR ON ERROR`.
     */
    case Error

    /**
     * Return NULL.
     *
     * Renders as `NULL ON ERROR`.
     */
    case Null

    /**
     * Return a default expression.
     *
     * Renders as `DEFAULT expr ON ERROR`.
     */
    case Default(expr: SqlExpr)

/**
 * `ON ERROR` behavior for `JSON_EXISTS`.
 */
enum SqlJsonExistsErrorBehavior:
    /**
     * Raise an error.
     *
     * Renders as `ERROR ON ERROR`.
     */
    case Error

    /**
     * Return TRUE.
     *
     * Renders as `TRUE ON ERROR`.
     */
    case True

    /**
     * Return FALSE.
     *
     * Renders as `FALSE ON ERROR`.
     */
    case False

    /**
     * Return UNKNOWN.
     *
     * Renders as `UNKNOWN ON ERROR`.
     */
    case Unknown

/**
 * Input format specification for JSON parsing functions.
 *
 * Renders as `FORMAT JSON [ENCODING UTF8|UTF16|UTF32]`.
 */
case class SqlJsonInput(encoding: Option[SqlJsonEncoding])

/**
 * Output format specification for JSON returning functions.
 *
 * Renders as `FORMAT JSON [ENCODING UTF8|UTF16|UTF32]`.
 */
case class SqlJsonOutputFormat(encoding: Option[SqlJsonEncoding])

/**
 * A `RETURNING` clause for JSON functions.
 *
 * Renders as `RETURNING type [FORMAT JSON [ENCODING UTF8|UTF16|UTF32]]`.
 */
case class SqlJsonOutput(`type`: SqlType, format: Option[SqlJsonOutputFormat])

/**
 * A key and value pair for `JSON_OBJECT` and `JSON_OBJECTAGG`.
 *
 * Renders as `expr VALUE expr`.
 */
case class SqlJsonObjectItem(key: SqlExpr, value: SqlExpr)

/**
 * An element for `JSON_ARRAY` and `JSON_ARRAYAGG`.
 *
 * Renders as `expr [FORMAT JSON [ENCODING UTF8|UTF16|UTF32]]`.
 */
case class SqlJsonArrayItem(value: SqlExpr, input: Option[SqlJsonInput])

/**
 * Overflow behavior for `LISTAGG`.
 */
enum SqlListAggOnOverflow:
    /**
     * Raise an error on overflow.
     *
     * Renders as `ERROR`.
     */
    case Error

    /**
     * Truncate on overflow, with a filler expression and count mode.
     *
     * Renders as `TRUNCATE expr [WITH|WITHOUT COUNT]`.
     */
    case Truncate(expr: SqlExpr, countMode: SqlListAggCountMode)

/**
 * Count mode for `LISTAGG` truncation.
 */
enum SqlListAggCountMode:
    /**
     * Include the count of truncated rows.
     *
     * Renders as `WITH COUNT`.
     */
    case With

    /**
     * Do not include the count of truncated rows.
     *
     * Renders as `WITHOUT COUNT`.
     */
    case Without

/**
 * Nulls handling mode for window functions.
 */
enum SqlWindowNullsMode:
    /**
     * Respect nulls.
     *
     * Renders as `RESPECT NULLS`.
     */
    case Respect

    /**
     * Ignore nulls.
     *
     * Renders as `IGNORE NULLS`.
     */
    case Ignore

/**
 * Counting direction for `NTH_VALUE`.
 */
enum SqlNthValueFromMode:
    /**
     * Count from the first row of the window frame.
     *
     * Renders as `FROM FIRST`.
     */
    case First

    /**
     * Count from the last row of the window frame.
     *
     * Renders as `FROM LAST`.
     */
    case Last

/**
 * Match phase for `MATCH_RECOGNIZE` qualifiers.
 */
enum SqlMatchPhase:
    /**
     * Final match: returns the last row of a match.
     *
     * Renders as `FINAL`.
     */
    case Final

    /**
     * Running match: returns every row of a match.
     *
     * Renders as `RUNNING`.
     */
    case Running