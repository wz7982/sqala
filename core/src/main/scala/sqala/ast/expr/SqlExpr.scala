package sqala.ast.expr

import sqala.ast.order.SqlOrderingItem
import sqala.ast.quantifier.SqlQuantifier
import sqala.ast.statement.SqlQuery

/**
 * An expression enum.
 */
enum SqlExpr:
    /**
     * A qualified or unqualified column reference.
     *
     * Renders as `"table"."column"` when `tableName` is present,
     * or just `"column"` when absent.
     *
     * @param tableName optional table / alias qualifier.
     * @param columnName the column identifier.
     */
    case Column(tableName: Option[String], columnName: String)

    /**
     * A `NULL` literal.
     *
     * Renders as `NULL`.
     */
    case NullLiteral

    /**
     * A string literal, rendered with proper escaping.
     *
     * Renders as `'value'`.
     *
     * @param string the raw string value.
     */
    case StringLiteral(string: String)

    /**
     * A numeric literal, generic over any `Numeric` type.
     *
     * Renders as `42`, `3.14`, etc.
     *
     * @param number the numeric value.
     */
    case NumberLiteral[N: Numeric](number: N)

    /**
     * A boolean literal.
     *
     * Renders as `TRUE|FALSE`.
     *
     * @param boolean the boolean value.
     */
    case BooleanLiteral(boolean: Boolean)

    /**
     * A time / date literal.
     *
     * Renders as `DATE '2023-01-01'|TIMESTAMP '2023-01-01 12:00:00'|TIME '12:00:00'`.
     *
     * @param unit the time literal unit (e.g. DATE, TIME, TIMESTAMP).
     * @param time the date/time string literal value.
     */
    case TimeLiteral(unit: SqlTimeLiteralUnit, time: String)

    /**
     * An interval literal.
     *
     * Renders as `INTERVAL 'value' field`.
     *
     * @param value the interval quantity string (e.g. `"1"`).
     * @param field the interval field (e.g. `DAY`, `YEAR TO MONTH`).
     */
    case IntervalLiteral(value: String, field: SqlIntervalField)

    /**
     * A parenthesized tuple of expressions.
     *
     * Renders as `(expr [, ...])`.
     *
     * @param items the expressions inside the tuple.
     */
    case Tuple(items: List[SqlExpr])

    /**
     * An array expression.
     *
     * Renders as `ARRAY[expr [, ...]]`.
     *
     * @param items the expressions inside the array.
     */
    case Array(items: List[SqlExpr])

    /**
     * A unary operation applied to a single expression.
     *
     * Renders as `operator(expr)`, e.g. `NOT(expr)`, `+(expr)`, `-(expr)`.
     *
     * @param operator the unary operator.
     * @param expr the operand expression.
     */
    case Unary(operator: SqlUnaryOperator, expr: SqlExpr)

    /**
     * A binary operation between two expressions.
     *
     * Renders as `expr operator expr` (e.g. `a + b`, `x = y`, `p AND q`).
     *
     * Parentheses are added automatically based on operator precedence.
     *
     * @param left the left-hand expression.
     * @param operator the binary operator.
     * @param right the right-hand expression.
     */
    case Binary(left: SqlExpr, operator: SqlBinaryOperator, right: SqlExpr)

    /**
     * An `IS JSON` / `IS NOT JSON` predicate.
     *
     * Renders as `expr IS [NOT] JSON [VALUE|OBJECT|ARRAY|SCALAR] [WITH|WITHOUT UNIQUE KEYS]`.
     *
     * @param expr the expression to test.
     * @param nodeType optional JSON node type check (VALUE, OBJECT, ARRAY, SCALAR).
     * @param uniqueness optional uniqueness mode (WITH/WITHOUT UNIQUE KEYS).
     * @param not when `true`, produces `IS NOT JSON` instead of `IS JSON`.
     */
    case JsonTest(
        expr: SqlExpr,
        nodeType: Option[SqlJsonNodeType],
        uniqueness: Option[SqlJsonUniquenessMode],
        not: Boolean
    )

    /**
     * A `BETWEEN` predicate.
     *
     * Renders as `expr [NOT] BETWEEN expr AND expr`.
     *
     * @param expr the expression to test.
     * @param start the lower bound.
     * @param end the upper bound.
     * @param not when `true`, produces `NOT BETWEEN`.
     */
    case Between(expr: SqlExpr, start: SqlExpr, end: SqlExpr, not: Boolean)

    /**
     * A `LIKE` pattern-matching predicate.
     *
     * Renders as `expr [NOT] LIKE pattern [ESCAPE expr]`.
     *
     * @param expr the expression to test.
     * @param pattern the LIKE pattern expression.
     * @param escape optional escape character expression.
     * @param not when `true`, produces `NOT LIKE`.
     */
    case Like(expr: SqlExpr, pattern: SqlExpr, escape: Option[SqlExpr], not: Boolean)

    /**
     * A `SIMILAR TO` pattern-matching predicate.
     *
     * Renders as `expr [NOT] SIMILAR TO pattern [ESCAPE expr]`.
     *
     * @param expr the expression to test.
     * @param pattern the SIMILAR TO pattern expression.
     * @param escape optional escape character expression.
     * @param not when `true`, produces `NOT SIMILAR TO`.
     */
    case SimilarTo(expr: SqlExpr, pattern: SqlExpr, escape: Option[SqlExpr], not: Boolean)

    /**
     * A searched `CASE` expression.
     *
     * Renders as `CASE WHEN expr THEN expr [WHEN expr THEN expr ...] [ELSE expr] END`.
     *
     * @param branches the WHEN/THEN pairs.
     * @param default optional ELSE expression.
     */
    case Case(branches: List[SqlCaseBranch], default: Option[SqlExpr])

    /**
     * A simple `CASE` expression.
     *
     * Renders as `CASE expr WHEN expr THEN expr [WHEN expr THEN expr ...] [ELSE expr] END`.
     *
     * @param expr the expression to compare against each WHEN value.
     * @param branches the WHEN/THEN pairs.
     * @param default optional ELSE expression.
     */
    case SimpleCase(expr: SqlExpr, branches: List[SqlCaseBranch], default: Option[SqlExpr])

    /**
     * A `COALESCE` expression: returns the first non-NULL value in the list.
     *
     * Renders as `COALESCE(expr [, ...])`.
     *
     * @param items the expressions to evaluate.
     */
    case Coalesce(items: List[SqlExpr])

    /**
     * A `NULLIF` expression: returns NULL when `expr` equals `test`,
     * otherwise returns `expr`.
     *
     * Renders as `NULLIF(expr, expr)`.
     *
     * @param expr the expression to return if not equal.
     * @param test the expression to compare against.
     */
    case NullIf(expr: SqlExpr, test: SqlExpr)

    /**
     * A `CAST` expression.
     *
     * Renders as `CAST(expr AS type)`.
     *
     * @param expr the expression to cast.
     * @param `type` the target type.
     */
    case Cast(expr: SqlExpr, `type`: SqlType)

    /**
     * An expression qualified with an `OVER` clause for window functions.
     *
     * Renders as `expr OVER (window_spec)`.
     *
     * @param expr the window function or aggregate expression.
     * @param window the window specification (PARTITION BY, ORDER BY, frame).
     */
    case Window(expr: SqlExpr, window: SqlWindow)

    /**
     * A subquery expression, optionally prefixed with a quantifier.
     *
     * Renders as `[ANY|ALL|EXISTS] (query)`.
     *
     * @param quantifier optional `ANY`, `ALL`, or `EXISTS`.
     * @param query the nested `SELECT` query.
     */
    case Subquery(quantifier: Option[SqlSubqueryQuantifier], query: SqlQuery)

    /**
     * A `GROUPING` expression: returns 1 if a column is being aggregated
     * (i.e. not part of the current grouping set), 0 otherwise.
     *
     * Renders as `GROUPING(expr [, ...])`.
     *
     * @param items the expressions to check.
     */
    case Grouping(items: List[SqlExpr])

    /**
     * A raw, unquoted identifier used as a function name.
     *
     * Renders as the `name` directly into the output without quoting.
     *
     * @param name the raw identifier text (e.g. `"CURRENT_TIME"`).
     */
    case IdentFunc(name: String)

    /**
     * A `SUBSTRING` function.
     *
     * Renders as `SUBSTRING(expr FROM expr [FOR expr])`.
     *
     * @param expr the source string expression.
     * @param from the start position (1-based).
     * @param `for` optional length.
     */
    case SubstringFunc(expr: SqlExpr, from: SqlExpr, `for`: Option[SqlExpr])

    /**
     * A `TRIM` function.
     *
     * Renders as `TRIM([[LEADING|TRAILING|BOTH] [expr] FROM] expr)`.
     *
     * @param expr the string expression to trim.
     * @param trim optional trim specification (mode and/or characters to remove).
     */
    case TrimFunc(expr: SqlExpr, trim: Option[SqlTrim])

    /**
     * An `OVERLAY` function.
     *
     * Renders as `OVERLAY(expr PLACING expr FROM expr [FOR expr])`.
     *
     * @param expr the source string expression.
     * @param placing the replacement string.
     * @param from the start position (1-based).
     * @param `for` optional number of characters to replace.
     */
    case OverlayFunc(expr: SqlExpr, placing: SqlExpr, from: SqlExpr, `for`: Option[SqlExpr])

    /**
     * A `POSITION` function.
     *
     * Renders as `POSITION(expr IN expr)`.
     *
     * @param expr the substring to search for.
     * @param in the string to search within.
     */
    case PositionFunc(expr: SqlExpr, in: SqlExpr)

    /**
     * An `EXTRACT` function.
     *
     * Renders as `EXTRACT(unit FROM expr)`.
     *
     * @param unit the time unit to extract (YEAR, MONTH, DAY, etc.).
     * @param expr the temporal expression.
     */
    case ExtractFunc(unit: SqlTimeUnit, expr: SqlExpr)

    /**
     * A `JSON_SERIALIZE` function: serializes a JSON expression into text.
     *
     * Renders as
     * `JSON_SERIALIZE(expr [RETURNING type [FORMAT JSON [ENCODING UTF8|UTF16|UTF32]]])`.
     *
     * @param expr the JSON expression to serialize.
     * @param output optional return clause specifying the target type
     *               and optionally a format with encoding (UTF8, UTF16, UTF32, or custom).
     */
    case JsonSerializeFunc(expr: SqlExpr, output: Option[SqlJsonOutput])

    /**
     * A `JSON` parsing function: parses a string into a JSON value.
     *
     * Renders as
     * `JSON(expr [FORMAT JSON [ENCODING UTF8|UTF16|UTF32]] [WITH|WITHOUT UNIQUE KEYS])`.
     *
     * @param expr the string expression to parse as JSON.
     * @param input optional input format clause with encoding (UTF8, UTF16, UTF32, or custom).
     * @param uniqueness optional uniqueness constraint (WITH/WITHOUT UNIQUE KEYS).
     */
    case JsonParseFunc(
        expr: SqlExpr,
        input: Option[SqlJsonInput],
        uniqueness: Option[SqlJsonUniquenessMode]
    )

    /**
     * A `JSON_QUERY` function: extracts a JSON object or array at a given path.
     *
     * Renders as
     * `JSON_QUERY(expr, expr
     *   [PASSING expr AS alias [, ...]]
     *   [RETURNING type [FORMAT JSON [ENCODING UTF8|UTF16|UTF32]]]
     *   [WITH|WITHOUT [CONDITIONAL|UNCONDITIONAL] [ARRAY] WRAPPER]
     *   [KEEP|OMIT QUOTES [ON SCALAR STRING]]
     *   [ERROR|NULL|EMPTY OBJECT|EMPTY ARRAY|DEFAULT expr ON EMPTY]
     *   [ERROR|NULL|EMPTY OBJECT|EMPTY ARRAY|DEFAULT expr ON ERROR])`.
     *
     * @param expr the JSON source expression.
     * @param path the JSON path expression.
     * @param passingItems variables passed to the path expression (`expr AS alias`).
     * @param output optional RETURNING clause with target type and format/encoding.
     * @param wrapper optional wrapper behavior:
     *                WITH/WITHOUT, optionally CONDITIONAL/UNCONDITIONAL, optionally ARRAY.
     * @param quotes optional quotes behavior:
     *               KEEP/OMIT QUOTES, optionally ON SCALAR STRING.
     * @param onEmpty behavior when the path yields an empty result.
     * @param onError behavior when a JSON structural error occurs.
     */
    case JsonQueryFunc(
        expr: SqlExpr,
        path: SqlExpr,
        passingItems: List[SqlJsonPassing],
        output: Option[SqlJsonOutput],
        wrapper: Option[SqlJsonQueryWrapperBehavior],
        quotes: Option[SqlJsonQueryQuotesBehavior],
        onEmpty: Option[SqlJsonQueryEmptyBehavior],
        onError: Option[SqlJsonQueryErrorBehavior]
    )

    /**
     * A `JSON_VALUE` function: extracts a scalar value at a given JSON path.
     *
     * Renders as
     * `JSON_VALUE(expr, expr
     *   [PASSING expr AS alias, [, ...]]
     *   [RETURNING type [FORMAT JSON [ENCODING UTF8|UTF16|UTF32]]]
     *   [ERROR|NULL|DEFAULT expr ON EMPTY]
     *   [ERROR|NULL|DEFAULT expr ON ERROR])`.
     *
     * @param expr the JSON source expression.
     * @param path the JSON path expression.
     * @param passingItems variables passed to the path expression (`expr AS alias`).
     * @param output optional RETURNING clause with target type and format/encoding.
     * @param onEmpty behavior when the path yields an empty result.
     * @param onError behavior when a JSON structural error occurs.
     */
    case JsonValueFunc(
        expr: SqlExpr,
        path: SqlExpr,
        passingItems: List[SqlJsonPassing],
        output: Option[SqlJsonOutput],
        onEmpty: Option[SqlJsonValueEmptyBehavior],
        onError: Option[SqlJsonValueErrorBehavior]
    )

    /**
     * A `JSON_OBJECT` function: builds a JSON object from key/value pairs.
     *
     * Renders as
     * `JSON_OBJECT(expr VALUE expr [, ...]
     *   [NULL ON NULL|ABSENT ON NULL]
     *   [WITH|WITHOUT UNIQUE KEYS]
     *   [RETURNING type [FORMAT JSON [ENCODING UTF8|UTF16|UTF32]]])`.
     *
     * @param items the key/value pairs (`key VALUE value`) of the object.
     * @param nullConstructor how NULL values are handled: NULL ON NULL (include) or ABSENT ON NULL (omit).
     * @param uniqueness optional uniqueness constraint (WITH/WITHOUT UNIQUE KEYS).
     * @param output optional RETURNING clause with target type and format/encoding.
     */
    case JsonObjectFunc(
        items: List[SqlJsonObjectItem],
        nullConstructor: Option[SqlJsonNullConstructor],
        uniqueness: Option[SqlJsonUniquenessMode],
        output: Option[SqlJsonOutput]
    )

    /**
     * A `JSON_ARRAY` function: builds a JSON array from values.
     *
     * Renders as
     * `JSON_ARRAY(expr [FORMAT JSON [ENCODING UTF8|UTF16|UTF32]] [, ...]
     *   [NULL ON NULL|ABSENT ON NULL]
     *   [RETURNING type [FORMAT JSON [ENCODING UTF8|UTF16|UTF32]]])`.
     *
     * @param items the array elements, each optionally with a FORMAT JSON / encoding clause.
     * @param nullConstructor how NULL values are handled: NULL ON NULL (include) or ABSENT ON NULL (omit).
     * @param output optional RETURNING clause with target type and format/encoding.
     */
    case JsonArrayFunc(
        items: List[SqlJsonArrayItem],
        nullConstructor: Option[SqlJsonNullConstructor],
        output: Option[SqlJsonOutput]
    )

    /**
     * A `JSON_EXISTS` function: tests whether a JSON path exists in the document.
     *
     * Renders as
     * `JSON_EXISTS(expr, expr
     *   [PASSING expr AS alias [, ...]]
     *   [ERROR|TRUE|FALSE|UNKNOWN ON ERROR])`.
     *
     * @param expr the JSON source expression.
     * @param path the JSON path to test.
     * @param passingItems variables passed to the path expression (`expr AS alias`).
     * @param onError behavior on structural error: ERROR, TRUE, FALSE, or UNKNOWN.
     */
    case JsonExistsFunc(
        expr: SqlExpr,
        path: SqlExpr,
        passingItems: List[SqlJsonPassing],
        onError: Option[SqlJsonExistsErrorBehavior]
    )

    /**
     * A `COUNT(*)` aggregate function.
     *
     * Renders as `COUNT(["table".]*) [FILTER (WHERE expr)]`.
     *
     * @param tableName optional table qualifier (e.g. for `COUNT(t.*)`).
     * @param filter optional FILTER clause expression.
     */
    case CountAsteriskFunc(tableName: Option[String], filter: Option[SqlExpr])

    /**
     * A `LISTAGG` aggregate function: concatenates values with a separator.
     *
     * Renders as
     * `LISTAGG([DISTINCT|ALL] expr, expr
     *   [ON OVERFLOW ERROR|TRUNCATE expr WITH|WITHOUT COUNT])
     *   [WITHIN GROUP (ORDER BY ordering_item [, ...])]
     *   [FILTER (WHERE expr)]`.
     *
     * @param quantifier optional `DISTINCT` / `ALL`.
     * @param expr the expression to aggregate.
     * @param separator the separator string.
     * @param onOverflow overflow behavior: ERROR or TRUNCATE with a filler expression and optional WITH/WITHOUT COUNT.
     * @param withinGroup ordering for the WITHIN GROUP clause.
     * @param filter optional FILTER clause.
     */
    case ListAggFunc(
        quantifier: Option[SqlQuantifier],
        expr: SqlExpr,
        separator: SqlExpr,
        onOverflow: Option[SqlListAggOnOverflow],
        withinGroup: List[SqlOrderingItem],
        filter: Option[SqlExpr]
    )

    /**
     * A `JSON_OBJECTAGG` aggregate function: aggregates key/value pairs into a JSON object.
     *
     * Renders as
     * `JSON_OBJECTAGG(expr VALUE expr
     *   [NULL ON NULL|ABSENT ON NULL]
     *   [WITH|WITHOUT UNIQUE KEYS]
     *   [RETURNING type [FORMAT JSON [ENCODING UTF8|UTF16|UTF32]]])
     *   [FILTER (WHERE expr)]`.
     *
     * @param item the key/value pair.
     * @param nullConstructor how to handle NULL values.
     * @param uniqueness optional uniqueness constraint.
     * @param output optional return type and format.
     * @param filter optional FILTER clause.
     */
    case JsonObjectAggFunc(
        item: SqlJsonObjectItem,
        nullConstructor: Option[SqlJsonNullConstructor],
        uniqueness: Option[SqlJsonUniquenessMode],
        output: Option[SqlJsonOutput],
        filter: Option[SqlExpr]
    )

    /**
     * A `JSON_ARRAYAGG` aggregate function: aggregates values into a JSON array.
     *
     * Renders as
     * `JSON_ARRAYAGG(expr
     *   [FORMAT JSON [ENCODING UTF8|UTF16|UTF32]]
     *   [ORDER BY ordering_item [, ...]]
     *   [NULL ON NULL|ABSENT ON NULL]
     *   [RETURNING type [FORMAT JSON [ENCODING UTF8|UTF16|UTF32]]])
     *   [FILTER (WHERE expr)]`.
     *
     * @param item the array element, optionally with a format specification.
     * @param orderBy ordering clause inside the aggregate.
     * @param nullConstructor how to handle NULL values.
     * @param output optional return type and format.
     * @param filter optional FILTER clause.
     */
    case JsonArrayAggFunc(
        item: SqlJsonArrayItem,
        orderBy: List[SqlOrderingItem],
        nullConstructor: Option[SqlJsonNullConstructor],
        output: Option[SqlJsonOutput],
        filter: Option[SqlExpr]
    )

    /**
     * A window function with `NULLS` treatment, such as `LAG`, `LEAD`, `FIRST_VALUE`, or `LAST_VALUE`.
     *
     * Renders as `name(expr [, ...]) [RESPECT NULLS|IGNORE NULLS]`.
     *
     * @param name the function name.
     * @param args the function arguments.
     * @param nullsMode optional nulls handling mode.
     */
    case NullsTreatmentFunc(
        name: String,
        args: List[SqlExpr],
        nullsMode: Option[SqlWindowNullsMode]
    )

    /**
     * An `NTH_VALUE` window function: returns the n-th row value in a window.
     *
     * Renders as
     * `NTH_VALUE(expr, expr) [FROM FIRST|FROM LAST] [RESPECT NULLS|IGNORE NULLS]`.
     *
     * @param expr the value expression.
     * @param row the N-th row number.
     * @param fromMode the counting direction (FROM FIRST or FROM LAST).
     * @param nullsMode optional nulls handling mode.
     */
    case NthValueFunc(
        expr: SqlExpr,
        row: SqlExpr,
        fromMode: Option[SqlNthValueFromMode],
        nullsMode: Option[SqlWindowNullsMode]
    )

    /**
     * A general-purpose function call, supporting most aggregate/function.
     *
     * Renders as
     * `name([DISTINCT|ALL] expr [, ...]
     *   [ORDER BY ordering_item [, ...]])
     *   [WITHIN GROUP (ORDER BY ordering_item [, ...])]
     *   [FILTER (WHERE expr)]`.
     *
     * @param quantifier optional `DISTINCT` / `ALL`.
     * @param name the function name.
     * @param args the function arguments.
     * @param orderBy ordering inside the function call.
     * @param withinGroup ordering for the WITHIN GROUP clause.
     * @param filter optional FILTER clause.
     */
    case GeneralFunc(
        quantifier: Option[SqlQuantifier],
        name: String,
        args: List[SqlExpr],
        orderBy: List[SqlOrderingItem],
        withinGroup: List[SqlOrderingItem],
        filter: Option[SqlExpr]
    )

    /**
     * A `MATCH_RECOGNIZE` phase qualifier for row pattern recognition.
     *
     * Renders as `[FINAL|RUNNING] expr`.
     *
     * `FINAL` returns the last row of a match; `RUNNING` returns every row.
     *
     * @param phase the match phase (FINAL or RUNNING).
     * @param expr the expression to qualify.
     */
    case MatchPhase(phase: SqlMatchPhase, expr: SqlExpr)

    /**
     * A free-form custom expression with interpolated sub-expressions.
     *
     * Renders as `(words(0) exprs(0) words(1) exprs(1) ... words(n))`.
     *
     * The `words` and `exprs` are interleaved, and the whole
     * expression is wrapped in parentheses.
     *
     * @param words text fragments.
     * @param exprs sub-expressions to be interpolated between the words.
     */
    case Custom(words: List[String], exprs: List[SqlExpr])