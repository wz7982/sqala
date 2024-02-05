package sqala.ast.table

import sqala.ast.statement.SqlQuery

enum SqlTable:
    case IdentTable(tableName: String, alias: Option[String])
    case SubQueryTable(query: SqlQuery, lateral: Boolean, alias: SqlSubQueryAlias)
    case JoinTable(left: SqlTable, joinType: SqlJoinType, right: SqlTable, condition: Option[SqlJoinCondition])

case class SqlSubQueryAlias(tableAlias: String, columnAlias: List[String] = Nil)