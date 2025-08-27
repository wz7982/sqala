package sqala.ast.table

import sqala.ast.expr.SqlExpr
import sqala.ast.statement.SqlQuery

enum SqlTable(val alias: Option[SqlTableAlias]):
    case Standard(
        name: String, 
        override val alias: Option[SqlTableAlias]
    ) extends SqlTable(alias)
    case Func(
        name: String, 
        args: List[SqlExpr], 
        override val alias: Option[SqlTableAlias]
    ) extends SqlTable(alias)
    case SubQuery(
        query: SqlQuery, 
        lateral: Boolean, 
        override val alias: Option[SqlTableAlias]
    ) extends SqlTable(alias)
    case Join(
        left: SqlTable, 
        joinType: SqlJoinType, 
        right: SqlTable, 
        condition: Option[SqlJoinCondition],
        override val alias: Option[SqlTableAlias]
    ) extends SqlTable(alias)

case class SqlTableAlias(tableAlias: String, columnAlias: List[String] = Nil)