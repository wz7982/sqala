package sqala.ast.table

import sqala.ast.expr.SqlExpr

enum SqlJoinType(val joinType: String):
    case InnerJoin extends SqlJoinType("INNER JOIN")
    case LeftJoin extends SqlJoinType("LEFT OUTER JOIN")
    case RightJoin extends SqlJoinType("RIGHT OUTER JOIN")
    case FullJoin extends SqlJoinType("FULL OUTER JOIN")
    case CrossJoin extends SqlJoinType("CROSS JOIN")
    case LeftSemiJoin extends SqlJoinType("LEFT SEMI JOIN")
    case LeftAntiJoin extends SqlJoinType("LEFT ANTI JOIN")
    case RightSemiJoin extends SqlJoinType("RIGHT SEMI JOIN")
    case RightAntiJoin extends SqlJoinType("RIGHT ANTI JOIN")

enum SqlJoinCondition:
    case On(condition: SqlExpr)
    case Using(condition: SqlExpr)