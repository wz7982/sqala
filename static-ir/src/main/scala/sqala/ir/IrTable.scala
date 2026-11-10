package sqala.ir

import sqala.ast.table.SqlJoinType
import sqala.ast.expr.SqlType
import sqala.ast.expr.SqlJsonOutputFormat
import sqala.util.NonEmptyList
import sqala.ast.table.SqlGraphQuantifier
import sqala.ast.table.SqlGraphLabel
import sqala.ast.table.SqlGraphSymbol
import sqala.ast.table.SqlGraphMatchMode
import sqala.ast.table.SqlGraphRowsMode
import sqala.ast.table.SqlGraphExportMode

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
    case Graph(
        withLateral: Boolean,
        name: String,
        matchMode: Option[SqlGraphMatchMode],
        patterns: NonEmptyList[IrGraphPattern],
        where: Option[IrExpr],
        rowsMode: Option[SqlGraphRowsMode],
        columns: NonEmptyList[IrSelectItem],
        exportMode: Option[SqlGraphExportMode],
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

case class IrGraphPattern(
    name: Option[String],
    term: IrGraphPatternTerm
)

enum IrGraphPatternTerm:
    case Quantified(
        term: IrGraphPatternTerm,
        quantifier: IrGraphQuantifier
    )
    case Vertex(
        table: IrTable,
        label: Option[SqlGraphLabel],
        where: Option[IrExpr]
    )
    case Edge(
        leftSymbol: SqlGraphSymbol,
        table: IrTable,
        label: Option[SqlGraphLabel],
        where: Option[IrExpr],
        rightSymbol: SqlGraphSymbol
    )
    case And(
        left: IrGraphPatternTerm,
        right: IrGraphPatternTerm
    )
    case Or(
        left: IrGraphPatternTerm,
        right: IrGraphPatternTerm
    )
    case Alternation(
        left: IrGraphPatternTerm,
        right: IrGraphPatternTerm
    )

enum IrGraphQuantifier:
    case Asterisk
    case Question
    case Plus
    case Between(start: Option[IrExpr], end: Option[IrExpr])
    case Quantity(quantity: IrExpr)