package sqala.ast.table

import sqala.ast.expr.SqlExpr
import sqala.ast.statement.SqlQuery

enum SqlTable:
    case Standard(
        name: String,
        alias: Option[SqlTableAlias],
        sample: Option[SqlTableSample]
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

case class SqlTableSample(mode: SqlTableSampleMode, percentage: SqlExpr, repeatable: Option[SqlExpr])

enum SqlTableSampleMode(val mode: String):
    case Bernoulli extends SqlTableSampleMode("BERNOULLI")
    case System extends SqlTableSampleMode("SYSTEM")