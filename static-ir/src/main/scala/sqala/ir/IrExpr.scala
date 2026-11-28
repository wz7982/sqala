package sqala.ir

import sqala.ast.expr.*
import sqala.ast.quantifier.SqlQuantifier
import sqala.util.NonEmptyList

/**
 * The static DSL's intermediate representation of a SQL expression,
 * later lowered to an AST SqlExpr.
 *
 * It covers column references, literals wrapped as `Native`, unary/binary
 * operators, predicates (`IN`, `BETWEEN`, `LIKE`, `EXISTS`, quantified
 * comparisons), subqueries, `CASE`, casts, JSON functions, aggregate
 * functions, window functions, and row-pattern phases.
 */
enum IrExpr:
    case Native(expr: SqlExpr)
    case Column(table: Option[IrTable], name: String)
    case Tuple(items: NonEmptyList[IrExpr])
    case Array(items: List[IrExpr])
    case Unary(operator: SqlUnaryOperator, expr: IrExpr)
    case Binary(left: IrExpr, operator: SqlBinaryOperator, right: IrExpr)
    case In(expr: IrExpr, in: IrInRightOperand, withNot: Boolean)
    case Between(expr: IrExpr, start: IrExpr, end: IrExpr, withNot: Boolean)
    case Like(expr: IrExpr, pattern: IrExpr, withNot: Boolean)
    case Case(branches: NonEmptyList[IrCaseBranch], default: Option[IrExpr])
    case Coalesce(items: NonEmptyList[IrExpr])
    case NullIf(expr: IrExpr, test: IrExpr)
    case Cast(expr: IrExpr, `type`: SqlType)
    case JsonTest(expr: IrExpr, nodeType: Option[SqlJsonNodeType], withNot: Boolean)
    case Window(
        expr: IrExpr,
        partitionBy: List[IrExpr],
        orderBy: List[IrOrderingItem],
        frame: Option[IrWindowFrame]
    )
    case Subquery(query: IrQuery)
    case ExistsPredicate(query: IrQuery)
    case QuantifiedComparisonPredicate(
        expr: IrExpr,
        operator: SqlQuantifiedComparisonOperator,
        quantifier: SqlSubqueryQuantifier,
        query: IrQuery
    )
    case Grouping(items: NonEmptyList[IrExpr])
    case IdentFunc(name: String)
    case SubstringFunc(expr: IrExpr, from: IrExpr, `for`: Option[IrExpr])
    case OverlayFunc(expr: IrExpr, placing: IrExpr, from: IrExpr, `for`: Option[IrExpr])
    case PositionFunc(expr: IrExpr, in: IrExpr)
    case ExtractFunc(unit: SqlTimeUnit, expr: IrExpr)
    case JsonSerializeFunc(expr: IrExpr)
    case JsonParseFunc(expr: IrExpr)
    case JsonQueryFunc(expr: IrExpr, path: IrExpr)
    case JsonValueFunc(expr: IrExpr, path: IrExpr)
    case JsonExistsFunc(expr: IrExpr, path: IrExpr)
    case JsonObjectFunc(items: List[IrJsonObjectItem])
    case JsonArrayFunc(items: List[IrExpr])
    case JsonObjectAggFunc(
        item: IrJsonObjectItem,
        filter: Option[IrExpr]
    )
    case JsonArrayAggFunc(
        value: IrExpr,
        orderBy: List[IrOrderingItem],
        filter: Option[IrExpr]
    )
    case CountAsteriskFunc(filter: Option[IrExpr])
    case ListAggFunc(
        quantifier: Option[SqlQuantifier],
        expr: IrExpr,
        separator: IrExpr,
        withinGroup: List[IrOrderingItem],
        filter: Option[IrExpr]
    )
    case NullsTreatmentFunc(
        name: String,
        args: List[IrExpr],
        nullsMode: Option[SqlWindowNullsMode]
    )
    case NthValueFunc(
        expr: IrExpr,
        row: IrExpr,
        fromMode: Option[SqlNthValueFromMode],
        nullsMode: Option[SqlWindowNullsMode]
    )
    case GeneralFunc(
        quantifier: Option[SqlQuantifier],
        name: String,
        args: List[IrExpr],
        orderBy: List[IrOrderingItem],
        withinGroup: List[IrOrderingItem],
        filter: Option[IrExpr]
    )
    case MatchPhase(phase: SqlMatchPhase, expr: IrExpr)
    case UnsafeCustom(tokens: List[IrUnsafeToken])