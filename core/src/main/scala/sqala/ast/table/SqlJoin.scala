package sqala.ast.table

import sqala.ast.expr.SqlExpr

enum SqlJoinType(val joinType: String):
    case Inner extends SqlJoinType("INNER JOIN")
    case Left extends SqlJoinType("LEFT OUTER JOIN")
    case Right extends SqlJoinType("RIGHT OUTER JOIN")
    case Full extends SqlJoinType("FULL OUTER JOIN")
    case Cross extends SqlJoinType("CROSS JOIN")
    case Semi extends SqlJoinType("SEMI JOIN")
    case Anti extends SqlJoinType("ANTI JOIN")
    case Custom(override val joinType: String) extends SqlJoinType(joinType)

enum SqlJoinCondition:
    case On(condition: SqlExpr)
    case Using(exprs: List[SqlExpr])