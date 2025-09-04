package sqala.ast.statement

import sqala.ast.expr.SqlExpr

enum SqlSelectItem:
    case Asterisk(tableName: Option[String])
    case Expr(expr: SqlExpr, alias: Option[String])