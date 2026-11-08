package sqala.ir

import sqala.ast.table.SqlJoinType
import sqala.ast.expr.SqlType
import sqala.ast.expr.SqlJsonOutputFormat
import sqala.util.NonEmptyList

enum IrTable:
    case Ident(name: String, columnAliases: List[String])
    case Subquery(withLateral: Boolean, query: IrQuery, columnAliases: List[String])
    case Join(left: IrTable, joinType: SqlJoinType, right: IrTable, on: Option[IrExpr])
    case Cte(index: Int, columnAliases: List[String])
    case Func(
        withLateral: Boolean,
        name: String,
        args: List[IrExpr],
        withOrdinality: Boolean,
        columnAliases: List[String]
    )
    case Json(
        withLateral: Boolean,
        expr: IrExpr,
        path: IrExpr,
        pathAlias: Option[String],
        columns: NonEmptyList[IrJsonColumn],
        columnAliases: List[String]
    )

enum IrJsonColumn:
    case Ordinality(name: String)
    case Column(
        name: String,
        `type`: SqlType,
        path: Option[IrExpr]
    )
    case Exists(
        name: String,
        `type`: SqlType,
        path: Option[IrExpr]
    )
    case Nested(
        path: IrExpr,
        pathAlias: Option[String],
        columns: NonEmptyList[IrJsonColumn]
    )