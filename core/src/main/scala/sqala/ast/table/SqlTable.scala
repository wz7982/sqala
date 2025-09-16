package sqala.ast.table

import sqala.ast.expr.{SqlExpr, SqlJsonPassing, SqlJsonTableColumn, SqlJsonTableErrorBehavior}
import sqala.ast.statement.SqlQuery

enum SqlTable:
    case Standard(
        name: String,
        period: Option[SqlTablePeriodMode],
        alias: Option[SqlTableAlias],
        matchRecognize: Option[SqlMatchRecognize],
        sample: Option[SqlTableSample]
    )
    case Func(
        name: String, 
        args: List[SqlExpr],
        lateral: Boolean,
        withOrd: Boolean,
        alias: Option[SqlTableAlias],
        matchRecognize: Option[SqlMatchRecognize]
    )
    case SubQuery(
        query: SqlQuery, 
        lateral: Boolean, 
        alias: Option[SqlTableAlias],
        matchRecognize: Option[SqlMatchRecognize]
    )
    case Json(
        expr: SqlExpr,
        path: SqlExpr,
        pathAlias: Option[String],
        passingItems: List[SqlJsonPassing],
        columns: List[SqlJsonTableColumn],
        onError: Option[SqlJsonTableErrorBehavior],
        lateral: Boolean,
        alias: Option[SqlTableAlias],
        matchRecognize: Option[SqlMatchRecognize]
    )
    case Graph(
        reference: String,
        rows: Option[SqlGraphRowsMode],
        columns: List[SqlGraphColumn],
        `export`: Option[SqlGraphExportMode],
        lateral: Boolean,
        alias: Option[SqlTableAlias],
        matchRecognize: Option[SqlMatchRecognize]
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