package sqala.ast.expr

import sqala.ast.order.SqlOrderBy
import sqala.ast.statement.SqlQuery

enum SqlExpr:
    case AllColumn(tableName: Option[String])
    case Column(tableName: Option[String], columnName: String)
    case Null
    case UnknowValue
    case StringLiteral(string: String)
    case NumberLiteral(number: Number)
    case BooleanLiteral(boolean: Boolean)
    case Vector(items: List[SqlExpr])
    case Unary(expr: SqlExpr, op: SqlUnaryOperator)
    case Binary(left: SqlExpr, op: SqlBinaryOperator, right: SqlExpr)
    case Func(name: String, args: List[SqlExpr])
    case Agg(name: String, args: List[SqlExpr], distinct: Boolean, attrs: Map[String, SqlExpr], orderBy: List[SqlOrderBy])
    case In(expr: SqlExpr, inExpr: SqlExpr, not: Boolean)
    case Between(expr: SqlExpr, start: SqlExpr, end: SqlExpr, not: Boolean)
    case Case(branches: List[SqlCase], default: SqlExpr)
    case Cast(expr: SqlExpr, castType: String)
    case Window(expr: SqlExpr, partitionBy: List[SqlExpr], orderBy: List[SqlOrderBy], frame: Option[SqlWindowFrame])
    case SubQuery(query: SqlQuery)
    case SubQueryPredicate(query: SqlQuery, predicate: SqlSubQueryPredicate)
    case Interval(value: String, unit: Option[SqlIntervalUnit])