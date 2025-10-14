package sqala.ast.expr

import sqala.ast.order.SqlOrderingItem
import sqala.ast.quantifier.SqlQuantifier
import sqala.ast.statement.SqlQuery

enum SqlExpr:
    case Column(tableName: Option[String], columnName: String)
    case NullLiteral
    case StringLiteral(string: String)
    case NumberLiteral[N: Numeric](number: N)
    case BooleanLiteral(boolean: Boolean)
    case TimeLiteral(unit: SqlTimeLiteralUnit, time: String)
    case IntervalLiteral(value: String, field: SqlIntervalField)
    case Tuple(items: List[SqlExpr])
    case Array(items: List[SqlExpr])
    case Unary(operator: SqlUnaryOperator, expr: SqlExpr)
    case Binary(left: SqlExpr, operator: SqlBinaryOperator, right: SqlExpr)
    case NullTest(expr: SqlExpr, not: Boolean)
    case JsonTest(
        expr: SqlExpr, 
        not: Boolean, 
        nodeType: Option[SqlJsonNodeType],
        uniqueness: Option[SqlJsonUniqueness]
    )
    case Between(expr: SqlExpr, start: SqlExpr, end: SqlExpr, not: Boolean)
    case Like(expr: SqlExpr, pattern: SqlExpr, escape: Option[SqlExpr], not: Boolean)
    case SimilarTo(expr: SqlExpr, pattern: SqlExpr, escape: Option[SqlExpr], not: Boolean)
    case Case(branches: List[SqlWhen], default: Option[SqlExpr])
    case SimpleCase(expr: SqlExpr, branches: List[SqlWhen], default: Option[SqlExpr])
    case Coalesce(items: List[SqlExpr])
    case NullIf(expr: SqlExpr, test: SqlExpr)
    case Cast(expr: SqlExpr, `type`: SqlType)
    case Window(expr: SqlExpr, window: SqlWindow)
    case SubQuery(query: SqlQuery)
    case SubLink(quantifier: SqlSubLinkQuantifier, query: SqlQuery)
    case Grouping(items: List[SqlExpr])
    case IdentFunc(name: String)
    case SubstringFunc(expr: SqlExpr, from: SqlExpr, `for`: Option[SqlExpr])
    case TrimFunc(expr: SqlExpr, trim: Option[SqlTrim])
    case OverlayFunc(expr: SqlExpr, placing: SqlExpr, from: SqlExpr, `for`: Option[SqlExpr])
    case PositionFunc(expr: SqlExpr, in: SqlExpr)
    case ExtractFunc(unit: SqlTimeUnit, expr: SqlExpr)
    case VectorDistanceFunc(left: SqlExpr, right: SqlExpr, mode: SqlVectorDistanceMode)
    case JsonSerializeFunc(expr: SqlExpr, output: Option[SqlJsonOutput])
    case JsonParseFunc(
        expr: SqlExpr,
        input: Option[SqlJsonInput],
        uniqueness: Option[SqlJsonUniqueness]
    )
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
    case JsonValueFunc(
        expr: SqlExpr, 
        path: SqlExpr,
        passingItems: List[SqlJsonPassing],
        output: Option[SqlJsonOutput],
        onEmpty: Option[SqlJsonValueEmptyBehavior],
        onError: Option[SqlJsonValueErrorBehavior]
    )
    case JsonObjectFunc(
        items: List[SqlJsonObjectElement],
        nullConstructor: Option[SqlJsonNullConstructor],
        uniqueness: Option[SqlJsonUniqueness],
        output: Option[SqlJsonOutput]
    )
    case JsonArrayFunc(
        items: List[SqlJsonArrayElement],
        nullConstructor: Option[SqlJsonNullConstructor],
        output: Option[SqlJsonOutput]
    )
    case JsonExistsFunc(
        expr: SqlExpr, 
        path: SqlExpr,
        passingItems: List[SqlJsonPassing],
        onError: Option[SqlJsonExistsErrorBehavior]
    )
    case CountAsteriskFunc(tableName: Option[String], filter: Option[SqlExpr])
    case ListAggFunc(
        quantifier: Option[SqlQuantifier],
        expr: SqlExpr,
        separator: SqlExpr,
        onOverflow: Option[SqlListAggOnOverflow],
        withinGroup: List[SqlOrderingItem],
        filter: Option[SqlExpr]
    )
    case JsonObjectAggFunc(
        item: SqlJsonObjectElement,
        nullConstructor: Option[SqlJsonNullConstructor],
        uniqueness: Option[SqlJsonUniqueness],
        output: Option[SqlJsonOutput],
        filter: Option[SqlExpr]
    )
    case JsonArrayAggFunc(
        item: SqlJsonArrayElement,
        orderBy: List[SqlOrderingItem],
        nullConstructor: Option[SqlJsonNullConstructor],
        output: Option[SqlJsonOutput],
        filter: Option[SqlExpr]
    )
    case NullsTreatmentFunc(
        name: String, 
        args: List[SqlExpr], 
        nullsMode: Option[SqlWindowNullsMode]
    )
    case NthValueFunc(
        expr: SqlExpr,
        row: SqlExpr,
        fromMode: Option[SqlNthValueFromMode],
        nullsMode: Option[SqlWindowNullsMode]
    )
    case StandardFunc(
        quantifier: Option[SqlQuantifier],
        name: String,
        args: List[SqlExpr],
        orderBy: List[SqlOrderingItem],
        withinGroup: List[SqlOrderingItem],
        filter: Option[SqlExpr]
    )
    case MatchPhase(phase: SqlMatchPhase, expr: SqlExpr)
    case Custom(snippet: String)