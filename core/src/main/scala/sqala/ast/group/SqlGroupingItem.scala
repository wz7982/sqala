package sqala.ast.group

import sqala.ast.expr.SqlExpr

enum SqlGroupingItem:
    case Expr(item: SqlExpr)
    case Cube(items: List[SqlExpr])
    case Rollup(items: List[SqlExpr])
    case GroupingSets(items: List[SqlExpr])