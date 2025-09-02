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
    case Vector(items: List[Float])
    case Unary(expr: SqlExpr, operator: SqlUnaryOperator)
    case Binary(left: SqlExpr, operator: SqlBinaryOperator, right: SqlExpr)
    case NullTest(expr: SqlExpr, not: Boolean)
    case Func(
        name: String,
        args: List[SqlExpr],
        quantifier: Option[SqlQuantifier] = None,
        orderBy: List[SqlOrderingItem] = Nil,
        withinGroup: List[SqlOrderingItem] = Nil,
        filter: Option[SqlExpr] = None
    )
    case Between(expr: SqlExpr, start: SqlExpr, end: SqlExpr, not: Boolean)
    case Case(branches: List[SqlWhen], default: Option[SqlExpr])
    case Match(expr: SqlExpr, branches: List[SqlWhen], default: Option[SqlExpr])
    case Cast(expr: SqlExpr, castType: SqlType)
    case Window(
        expr: SqlExpr, 
        partitionBy: List[SqlExpr], 
        orderBy: List[SqlOrderingItem], 
        frame: Option[SqlWindowFrame]
    )
    case SubQuery(query: SqlQuery)
    case SubLink(query: SqlQuery, quantifier: SqlSubLinkQuantifier)
    case Extract(expr: SqlExpr, unit: SqlTimeUnit)
    case Custom(snippet: String)