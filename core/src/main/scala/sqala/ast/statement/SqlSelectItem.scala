package sqala.ast.statement

import sqala.ast.expr.SqlExpr

enum SqlSelectItem:
    case Wildcard(tableName: Option[String])
    case Expr(expr: SqlExpr, alias: Option[String])