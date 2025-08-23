package sqala.ast.statement

import sqala.ast.expr.SqlExpr

enum SqlSelectItem:
    case Wildcard(table: Option[String])
    case Expr(expr: SqlExpr, alias: Option[String])