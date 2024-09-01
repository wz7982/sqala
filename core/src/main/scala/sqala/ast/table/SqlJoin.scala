package sqala.ast.table

import sqala.ast.expr.SqlExpr

enum SqlJoinType(val joinType: String):
    case InnerJoin extends SqlJoinType("INNER JOIN")
    case LeftJoin extends SqlJoinType("LEFT OUTER JOIN")
    case RightJoin extends SqlJoinType("RIGHT OUTER JOIN")
    case FullJoin extends SqlJoinType("FULL OUTER JOIN")
    case CrossJoin extends SqlJoinType("CROSS JOIN")
    case SemiJoin extends SqlJoinType("SEMI JOIN")
    case AntiJoin extends SqlJoinType("ANTI JOIN")
    case CustomJoin(override val joinType: String) extends SqlJoinType(joinType)

enum SqlJoinCondition:
    case On(condition: SqlExpr)
    case Using(exprs: List[SqlExpr])