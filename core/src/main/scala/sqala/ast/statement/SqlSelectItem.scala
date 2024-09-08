package sqala.ast.statement

import sqala.ast.expr.SqlExpr

enum SqlSelectItem:
    case Wildcard(table: Option[String])
    case Item(expr: SqlExpr, alias: Option[String])