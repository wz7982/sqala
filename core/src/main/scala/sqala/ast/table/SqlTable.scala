package sqala.ast.table

import sqala.ast.expr.{SqlExpr, SqlJsonPassing, SqlJsonTableColumn, SqlJsonTableErrorBehavior}
import sqala.ast.statement.{SqlQuery, SqlSelectItem}

enum SqlTable:
    case Standard(
        name: String,
        period: Option[SqlTablePeriodMode],
        alias: Option[SqlTableAlias],
        matchRecognize: Option[SqlMatchRecognize],
        sample: Option[SqlTableSample]
    )
    case Func(
        lateral: Boolean,
        name: String, 
        args: List[SqlExpr],
        withOrd: Boolean,
        alias: Option[SqlTableAlias],
        matchRecognize: Option[SqlMatchRecognize]
    )
    case SubQuery(
        lateral: Boolean,
        query: SqlQuery, 
        alias: Option[SqlTableAlias],
        matchRecognize: Option[SqlMatchRecognize]
    )
    case Json(
        lateral: Boolean,
        expr: SqlExpr,
        path: SqlExpr,
        pathAlias: Option[String],
        passingItems: List[SqlJsonPassing],
        columns: List[SqlJsonTableColumn],
        onError: Option[SqlJsonTableErrorBehavior],
        alias: Option[SqlTableAlias],
        matchRecognize: Option[SqlMatchRecognize]
    )
    case Graph(
        lateral: Boolean,
        name: String,
        `match`: Option[SqlGraphMatchMode],
        patterns: List[SqlGraphPattern],
        where: Option[SqlExpr],
        rows: Option[SqlGraphRowsMode],
        columns: List[SqlSelectItem],
        `export`: Option[SqlGraphExportMode],
        alias: Option[SqlTableAlias],
        matchRecognize: Option[SqlMatchRecognize]
    )
    case Join(
        left: SqlTable, 
        joinType: SqlJoinType, 
        right: SqlTable, 
        condition: Option[SqlJoinCondition]
    )

case class SqlTableAlias(alias: String, columnAliases: List[String])

case class SqlTableSample(mode: SqlTableSampleMode, percentage: SqlExpr, repeatable: Option[SqlExpr])

enum SqlTableSampleMode(val mode: String):
    case Bernoulli extends SqlTableSampleMode("BERNOULLI")
    case System extends SqlTableSampleMode("SYSTEM")
    case Custom(override val mode: String) extends SqlTableSampleMode(mode)

enum SqlTablePeriodMode:
    case ForSystemTimeAsOf(expr: SqlExpr)
    case ForSystemTimeBetween(
        mode: Option[SqlTablePeriodBetweenMode], 
        start: SqlExpr,
        end: SqlExpr
    )
    case ForSystemTimeFrom(from: SqlExpr, to: SqlExpr)

enum SqlTablePeriodBetweenMode(val mode: String):
    case Asymmetric extends SqlTablePeriodBetweenMode("ASYMMETRIC")
    case Symmetric extends SqlTablePeriodBetweenMode("SYMMETRIC")