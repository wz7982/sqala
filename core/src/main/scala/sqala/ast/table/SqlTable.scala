package sqala.ast.table

import sqala.ast.expr.SqlExpr
import sqala.ast.statement.SqlQuery

enum SqlTable:
    case IdentTable(tableName: String, alias: Option[SqlTableAlias])
    case FuncTable(functionName: String, args: List[SqlExpr], alias: Option[SqlTableAlias])
    case SubQueryTable(query: SqlQuery, lateral: Boolean, alias: SqlTableAlias)
    case JoinTable(left: SqlTable, joinType: SqlJoinType, right: SqlTable, condition: Option[SqlJoinCondition])

case class SqlTableAlias(tableAlias: String, columnAlias: List[String] = Nil)