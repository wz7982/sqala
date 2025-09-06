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
    case Vector(expr: SqlExpr)
    case Unary(expr: SqlExpr, operator: SqlUnaryOperator)
    case Binary(left: SqlExpr, operator: SqlBinaryOperator, right: SqlExpr)
    case NullTest(expr: SqlExpr, not: Boolean)
    case JsonTest(expr: SqlExpr, not: Boolean, nodeType: Option[SqlJsonNodeType])
    case Between(expr: SqlExpr, start: SqlExpr, end: SqlExpr, not: Boolean)
    case Case(branches: List[SqlWhen], default: Option[SqlExpr])
    case SimpleCase(expr: SqlExpr, branches: List[SqlWhen], default: Option[SqlExpr])
    case Coalesce(expr: SqlExpr, thenItems: List[SqlExpr])
    case NullIf(expr: SqlExpr, `then`: SqlExpr)
    case Cast(expr: SqlExpr, castType: SqlType)
    case Window(
        expr: SqlExpr, 
        partitionBy: List[SqlExpr], 
        orderBy: List[SqlOrderingItem], 
        frame: Option[SqlWindowFrame]
    )
    case SubQuery(query: SqlQuery)
    case SubLink(query: SqlQuery, quantifier: SqlSubLinkQuantifier)
    case Grouping(expr: SqlExpr, items: List[SqlExpr])
    case StandardValueFunc(name: String, args: List[SqlExpr])
    case IdentValueFunc(name: String)
    case SubstringValueFunc(expr: SqlExpr, from: SqlExpr, `for`: Option[SqlExpr])
    case TrimValueFunc(expr: SqlExpr, trim: Option[(Option[SqlTrimMode], Option[SqlExpr])])
    case OverlayValueFunc(expr: SqlExpr, placing: SqlExpr, from: SqlExpr, `for`: Option[SqlExpr])
    case PositionValueFunc(expr: SqlExpr, in: SqlExpr)
    case ExtractValueFunc(expr: SqlExpr, unit: SqlTimeUnit)
    case VectorDistanceFunc(left: SqlExpr, right: SqlExpr, mode: SqlVectorDistanceMode)
    case JsonSerializeValueFunc(expr: SqlExpr, output: Option[SqlJsonOutput])
    case JsonParseValueFunc(
        expr: SqlExpr,
        input: Option[SqlJsonInput],
        uniqueness: Option[SqlJsonUniqueness]
    )
    case JsonQueryValueFunc(
        expr: SqlExpr,
        path: SqlExpr,
        passingItems: List[SqlJsonPassing],
        output: Option[SqlJsonOutput],
        wrapper: Option[SqlJsonQueryWrapperBehavior],
        quotes: Option[SqlJsonQueryQuotesBehavior],
        onEmpty: Option[SqlJsonQueryEmptyBehavior],
        onError: Option[SqlJsonQueryErrorBehavior]
    )
    case JsonValueValueFunc(
        expr: SqlExpr, 
        path: SqlExpr,
        passingItems: List[SqlJsonPassing],
        output: Option[SqlJsonOutput],
        onEmpty: Option[SqlJsonValueEmptyBehavior],
        onError: Option[SqlJsonValueErrorBehavior]
    )
    case JsonObjectValueFunc(
        items: List[(SqlExpr, SqlExpr)],
        nullConstructor: Option[SqlJsonNullConstructor],
        uniqueness: Option[SqlJsonUniqueness],
        output: Option[SqlJsonOutput]
    )
    case JsonArrayValueFunc(
        items: List[(SqlExpr, Option[SqlJsonInput])],
        nullConstructor: Option[SqlJsonNullConstructor],
        output: Option[SqlJsonOutput]
    )
    case JsonExistsValueFunc(
        expr: SqlExpr, 
        path: SqlExpr,
        passingItems: List[SqlJsonPassing],
        onError: Option[SqlJsonExistsErrorBehavior]
    )
    case StandardAggFunc(
        name: String,
        args: List[SqlExpr],
        quantifier: Option[SqlQuantifier],
        orderBy: List[SqlOrderingItem],
        withinGroup: List[SqlOrderingItem],
        filter: Option[SqlExpr]
    )
    case CountAsteriskAggFunc(tableName: Option[String], filter: Option[SqlExpr])
    case ListAggAggFunc(
        expr: SqlExpr,
        separator: SqlExpr,
        quantifier: Option[SqlQuantifier],
        onOverflow: Option[SqlListAggOnOverflow],
        withinGroup: List[SqlOrderingItem],
        filter: Option[SqlExpr]
    )
    case JsonObjectAggAggFunc(
        item: (SqlExpr, SqlExpr),
        nullConstructor: Option[SqlJsonNullConstructor],
        uniqueness: Option[SqlJsonUniqueness],
        output: Option[SqlJsonOutput],
        filter: Option[SqlExpr]
    )
    case JsonArrayAggAggFunc(
        item: (SqlExpr, Option[SqlJsonInput]),
        orderBy: List[SqlOrderingItem],
        nullConstructor: Option[SqlJsonNullConstructor],
        output: Option[SqlJsonOutput],
        filter: Option[SqlExpr]
    )
    case StandardWindowFunc(name: String, args: List[SqlExpr])
    case NullsTreatmentWindowFunc(
        name: String, 
        args: List[SqlExpr], 
        nullsMode: Option[SqlWindowNullsMode]
    )
    case NthValueWindowFunc(
        arg: SqlExpr,
        fromMode: Option[SqlNthValueFromMode],
        nullsMode: Option[SqlWindowNullsMode]
    )
    case StandardMatchFunc(name: String, args: List[SqlExpr])
    case Custom(snippet: String)