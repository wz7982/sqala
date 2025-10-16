package sqala.ast.group

import sqala.ast.expr.SqlExpr
import sqala.ast.quantifier.SqlQuantifier

case class SqlGroupBy(quantifier: Option[SqlQuantifier], items: List[SqlGroupingItem])

enum SqlGroupingItem:
    case Expr(item: SqlExpr)
    case Cube(items: List[SqlExpr])
    case Rollup(items: List[SqlExpr])
    case GroupingSets(items: List[SqlExpr])