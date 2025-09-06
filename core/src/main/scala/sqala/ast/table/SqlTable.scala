package sqala.ast.table

import sqala.ast.expr.SqlExpr
import sqala.ast.statement.SqlQuery

enum SqlTable:
    case Standard(
        name: String,
        alias: Option[SqlTableAlias]
    )
    case Func(
        name: String, 
        args: List[SqlExpr],
        lateral: Boolean,
        withOrd: Boolean,
        alias: Option[SqlTableAlias]
    )
    case SubQuery(
        query: SqlQuery, 
        lateral: Boolean, 
        alias: Option[SqlTableAlias]
    )
    case Join(
        left: SqlTable, 
        joinType: SqlJoinType, 
        right: SqlTable, 
        condition: Option[SqlJoinCondition]
    )

case class SqlTableAlias(tableAlias: String, columnAlias: List[String])