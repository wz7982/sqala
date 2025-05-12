package sqala.ast.expr

import sqala.ast.order.SqlOrderItem
import sqala.ast.param.SqlParam
import sqala.ast.statement.SqlQuery

enum SqlExpr:
    case Column(tableName: Option[String], columnName: String)
    case Null
    case StringLiteral(string: String)
    case NumberLiteral[N: Numeric](number: N)
    case BooleanLiteral(boolean: Boolean)
    case TimeLiteral(unit: SqlTimeLiteralUnit, time: String)
    case Tuple(items: List[SqlExpr])
    case Array(items: List[SqlExpr])
    case Unary(expr: SqlExpr, op: SqlUnaryOperator)
    case Binary(left: SqlExpr, op: SqlBinaryOperator, right: SqlExpr)
    case NullTest(expr: SqlExpr, not: Boolean)
    case Func(
        name: String,
        args: List[SqlExpr],
        param: Option[SqlParam] = None,
        orderBy: List[SqlOrderItem] = Nil,
        withinGroup: List[SqlOrderItem] = Nil,
        filter: Option[SqlExpr] = None
    )
    case Between(expr: SqlExpr, start: SqlExpr, end: SqlExpr, not: Boolean)
    case Case(branches: List[SqlCase], default: SqlExpr)
    case Match(expr: SqlExpr, branches: List[SqlCase], default: SqlExpr)
    case Cast(expr: SqlExpr, castType: SqlCastType)
    case Window(expr: SqlExpr, partitionBy: List[SqlExpr], orderBy: List[SqlOrderItem], frame: Option[SqlWindowFrame])
    case SubQuery(query: SqlQuery)
    case SubLink(query: SqlQuery, linkType: SqlSubLinkType)
    case Interval(value: Double, unit: SqlTimeUnit)
    case Extract(expr: SqlExpr, unit: SqlTimeUnit)
    case Custom(snippet: String)