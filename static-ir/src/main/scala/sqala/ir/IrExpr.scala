package sqala.ir

import sqala.ast.expr.SqlExpr
import sqala.util.NonEmptyList
import sqala.ast.expr.SqlUnaryOperator
import sqala.ast.expr.SqlBinaryOperator
import sqala.ast.expr.SqlType
import sqala.ast.expr.SqlQuantifiedComparisonOperator
import sqala.ast.expr.SqlSubqueryQuantifier
import sqala.ast.expr.SqlTimeUnit
import sqala.ast.expr.SqlWindowNullsMode
import sqala.ast.expr.SqlNthValueFromMode
import sqala.ast.expr.SqlMatchPhase
import sqala.ast.quantifier.SqlQuantifier

enum IrExpr:
    case Native(expr: SqlExpr)
    case Column(table: IrTable, name: String)
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
    case JsonObjectAggFunc(
        key: IrExpr,
        value: IrExpr,
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