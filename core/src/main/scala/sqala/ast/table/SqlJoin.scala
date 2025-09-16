package sqala.ast.table

import sqala.ast.expr.SqlExpr

enum SqlJoinType(val `type`: String):
    case Inner extends SqlJoinType("INNER JOIN")
    case Left extends SqlJoinType("LEFT OUTER JOIN")
    case Right extends SqlJoinType("RIGHT OUTER JOIN")
    case Full extends SqlJoinType("FULL OUTER JOIN")
    case Cross extends SqlJoinType("CROSS JOIN")
    case Custom(override val `type`: String) extends SqlJoinType(`type`)

enum SqlJoinCondition:
    case On(condition: SqlExpr)
    case Using(columnNames: List[String])