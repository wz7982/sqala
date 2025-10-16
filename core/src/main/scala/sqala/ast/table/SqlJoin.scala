package sqala.ast.table

import sqala.ast.expr.SqlExpr

enum SqlJoinType:
    case Inner
    case Left
    case Right
    case Full
    case Cross
    case Custom(`type`: String)

enum SqlJoinCondition:
    case On(condition: SqlExpr)
    case Using(columnNames: List[String])