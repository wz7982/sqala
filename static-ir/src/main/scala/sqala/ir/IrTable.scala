package sqala.ir

import sqala.ast.table.SqlJoinType

enum IrTable:
    case Ident(name: String, columnAliases: List[String])
    case Subquery(withLateral: Boolean, query: IrQuery, columnAliases: List[String])
    case Join(left: IrTable, joinType: SqlJoinType, right: IrTable, on: Option[IrExpr])