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
     */
    case StringLiteral(string: String)

    /**
     * A numeric literal, generic over any `Numeric` type.
     *
     * Renders as `42`, `3.14`, etc.
     */
    case NumberLiteral[N: Numeric](number: N)

    /**
     * A boolean literal.
     *
     * Renders as `TRUE|FALSE`.
     */
    case BooleanLiteral(boolean: Boolean)

    /**
     * A time or date literal.
     *
     * Renders as `DATE '2023-01-01'|TIMESTAMP '2023-01-01 12:00:00'|TIME '12:00:00'`.
     */
    case TimeLiteral(`type`: SqlTimeType, time: String)

    /**
     * An interval literal.
     *
     * Renders as `INTERVAL 'value' field`.
     */
    case IntervalLiteral(value: String, field: SqlIntervalField)

    /**
     * A parenthesized tuple of expressions.
     *
     * Renders as `(expr [, ...])`.
     */
    case Tuple(items: List[SqlExpr])

    /**
     * An array expression.
     *
     * Renders as `ARRAY[expr [, ...]]`.
     */
    case Array(items: List[SqlExpr])

    /**
     * A unary operation applied to a single expression.
     *
     * Renders as `operator(expr)`, e.g. `NOT(expr)`, `+(expr)`, `-(expr)`.
     */
    case Unary(operator: SqlUnaryOperator, expr: SqlExpr)

    /**
     * A binary operation between two expressions.
     *
     * Renders as `expr operator expr` (e.g. `a + b`, `x = y`, `p AND q`).
     *
     * Parentheses are added automatically based on operator precedence.
     */
    case Binary(left: SqlExpr, operator: SqlBinaryOperator, right: SqlExpr)

    /**
     * An `IS JSON` or `IS NOT JSON` predicate.
     *
     * Renders as `expr IS [NOT] JSON [VALUE|OBJECT|ARRAY|SCALAR] [WITH|WITHOUT UNIQUE KEYS]`.
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
     */
    case Between(expr: SqlExpr, start: SqlExpr, end: SqlExpr, not: Boolean)

    /**
     * A `LIKE` pattern-matching predicate.
     *
     * Renders as `expr [NOT] LIKE pattern [ESCAPE expr]`.
     */
    case Like(expr: SqlExpr, pattern: SqlExpr, escape: Option[SqlExpr], not: Boolean)

    /**
     * A `SIMILAR TO` pattern-matching predicate.
     *
     * Renders as `expr [NOT] SIMILAR TO pattern [ESCAPE expr]`.
     */
    case SimilarTo(expr: SqlExpr, pattern: SqlExpr, escape: Option[SqlExpr], not: Boolean)

    /**
     * A searched `CASE` expression.
     *
     * Renders as `CASE WHEN expr THEN expr [WHEN expr THEN expr ...] [ELSE expr] END`.
     */
    case Case(branches: List[SqlCaseBranch], default: Option[SqlExpr])

    /**
     * A simple `CASE` expression.
     *
     * Renders as `CASE expr WHEN expr THEN expr [WHEN expr THEN expr ...] [ELSE expr] END`.
     */
    case SimpleCase(expr: SqlExpr, branches: List[SqlCaseBranch], default: Option[SqlExpr])

    /**
     * A `COALESCE` expression: returns the first non-NULL value in the list.
     *
     * Renders as `COALESCE(expr [, ...])`.
     */
    case Coalesce(items: List[SqlExpr])

    /**
     * A `NULLIF` expression: returns NULL when `expr` equals `test`,
     * otherwise returns `expr`.
     *
     * Renders as `NULLIF(expr, expr)`.
     */
    case NullIf(expr: SqlExpr, test: SqlExpr)

    /**
     * A `CAST` expression.
     *
     * Renders as `CAST(expr AS type)`.
     */
    case Cast(expr: SqlExpr, `type`: SqlType)

    /**
     * An expression qualified with an `OVER` clause for window functions.
     *
     * Renders as `expr OVER (window_spec)`.
     */
    case Window(expr: SqlExpr, window: SqlWindow)

    /**
     * A subquery expression, optionally prefixed with a quantifier.
     *
     * Renders as `[ANY|ALL|EXISTS] (query)`.
     */
    case Subquery(quantifier: Option[SqlSubqueryQuantifier], query: SqlQuery)

    /**
     * A `GROUPING` expression: returns 1 if a column is being aggregated
     * (i.e. not part of the current grouping set), 0 otherwise.
     *
     * Renders as `GROUPING(expr [, ...])`.
     */
    case Grouping(items: List[SqlExpr])

    /**
     * A raw, unquoted identifier used as a function name.
     *
     * Renders as the `name` directly into the output without quoting.
     */
    case IdentFunc(name: String)

    /**
     * A `SUBSTRING` function.
     *
     * Renders as `SUBSTRING(expr FROM expr [FOR expr])`.
     */
    case SubstringFunc(expr: SqlExpr, from: SqlExpr, `for`: Option[SqlExpr])

    /**
     * A `TRIM` function.
     *
     * Renders as `TRIM([[LEADING|TRAILING|BOTH] [expr] FROM] expr)`.
     */
    case TrimFunc(expr: SqlExpr, trim: Option[SqlTrim])

    /**
     * An `OVERLAY` function.
     *
     * Renders as `OVERLAY(expr PLACING expr FROM expr [FOR expr])`.
     */
    case OverlayFunc(expr: SqlExpr, placing: SqlExpr, from: SqlExpr, `for`: Option[SqlExpr])

    /**
     * A `POSITION` function.
     *
     * Renders as `POSITION(expr IN expr)`.
     */
    case PositionFunc(expr: SqlExpr, in: SqlExpr)

    /**
     * An `EXTRACT` function.
     *
     * Renders as `EXTRACT(unit FROM expr)`.
     */
    case ExtractFunc(unit: SqlTimeUnit, expr: SqlExpr)

    /**
     * A `JSON_SERIALIZE` function: serializes a JSON expression into text.
     *
     * Renders as
     * `JSON_SERIALIZE(expr [RETURNING type [FORMAT JSON [ENCODING UTF8|UTF16|UTF32]]])`.
     */
    case JsonSerializeFunc(expr: SqlExpr, output: Option[SqlJsonOutput])

    /**
     * A `JSON` parsing function: parses a string into a JSON value.
     *
     * Renders as
     * `JSON(expr [FORMAT JSON [ENCODING UTF8|UTF16|UTF32]] [WITH|WITHOUT UNIQUE KEYS])`.
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
     *   [PASSING expr AS "alias" [, ...]]
     *   [RETURNING type [FORMAT JSON [ENCODING UTF8|UTF16|UTF32]]]
     *   [WITH|WITHOUT [CONDITIONAL|UNCONDITIONAL] [ARRAY] WRAPPER]
     *   [KEEP|OMIT QUOTES [ON SCALAR STRING]]
     *   [ERROR|NULL|EMPTY OBJECT|EMPTY ARRAY|DEFAULT expr ON EMPTY]
     *   [ERROR|NULL|EMPTY OBJECT|EMPTY ARRAY|DEFAULT expr ON ERROR])`.
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
     *   [PASSING expr AS "alias", [, ...]]
     *   [RETURNING type [FORMAT JSON [ENCODING UTF8|UTF16|UTF32]]]
     *   [ERROR|NULL|DEFAULT expr ON EMPTY]
     *   [ERROR|NULL|DEFAULT expr ON ERROR])`.
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
     * A `JSON_OBJECT` function: builds a JSON object from key and value pairs.
     *
     * Renders as
     * `JSON_OBJECT(expr VALUE expr [, ...]
     *   [NULL ON NULL|ABSENT ON NULL]
     *   [WITH|WITHOUT UNIQUE KEYS]
     *   [RETURNING type [FORMAT JSON [ENCODING UTF8|UTF16|UTF32]]])`.
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
     *   [PASSING expr AS "alias" [, ...]]
     *   [ERROR|TRUE|FALSE|UNKNOWN ON ERROR])`.
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
     * A `JSON_OBJECTAGG` aggregate function: aggregates key and value pairs into a JSON object.
     *
     * Renders as
     * `JSON_OBJECTAGG(expr VALUE expr
     *   [NULL ON NULL|ABSENT ON NULL]
     *   [WITH|WITHOUT UNIQUE KEYS]
     *   [RETURNING type [FORMAT JSON [ENCODING UTF8|UTF16|UTF32]]])
     *   [FILTER (WHERE expr)]`.
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
     */
    case NthValueFunc(
        expr: SqlExpr,
        row: SqlExpr,
        fromMode: Option[SqlNthValueFromMode],
        nullsMode: Option[SqlWindowNullsMode]
    )

    /**
     * A general-purpose function call for aggregate and other functions.
     *
     * Renders as
     * `name([DISTINCT|ALL] expr [, ...]
     *   [ORDER BY ordering_item [, ...]])
     *   [WITHIN GROUP (ORDER BY ordering_item [, ...])]
     *   [FILTER (WHERE expr)]`.
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
     */
    case MatchPhase(phase: SqlMatchPhase, expr: SqlExpr)

    /**
     * A free-form custom expression with interpolated sub-expressions.
     *
     * Renders as `(words(0) exprs(0) words(1) exprs(1) ... words(n))`.
     *
     * The `words` and `exprs` are interleaved, and the whole
     * expression is wrapped in parentheses.
     */
    case Custom(words: List[String], exprs: List[SqlExpr])