package sqala.ast.expr

import sqala.ast.order.SqlOrderBy
import sqala.ast.statement.SqlQuery

enum SqlExpr:
    case Column(tableName: Option[String], columnName: String)
    case Null
    case StringLiteral(string: String)
    case NumberLiteral(number: Number)
    case BooleanLiteral(boolean: Boolean)
    case Vector(items: List[SqlExpr])
    case Unary(expr: SqlExpr, op: SqlUnaryOperator)
    case Binary(left: SqlExpr, op: SqlBinaryOperator, right: SqlExpr)
    case NullTest(expr: SqlExpr, not: Boolean)
    case Func(
        name: String, 
        args: List[SqlExpr], 
        distinct: Boolean = false,
        orderBy: List[SqlOrderBy] = Nil,
        withinGroup: List[SqlOrderBy] = Nil,
        filter: Option[SqlExpr] = None
    )
    case Between(expr: SqlExpr, start: SqlExpr, end: SqlExpr, not: Boolean)
    case Case(branches: List[SqlCase], default: SqlExpr)
    case Cast(expr: SqlExpr, castType: String)
    case Window(expr: SqlExpr, partitionBy: List[SqlExpr], orderBy: List[SqlOrderBy], frame: Option[SqlWindowFrame])
    case SubQuery(query: SqlQuery)
    case SubLink(query: SqlQuery, linkType: SqlSubLinkType)
    case Interval(value: Double, unit: SqlTimeUnit)
    case Extract(unit: SqlTimeUnit, expr: SqlExpr)
    case Grouping(items: List[SqlExpr])