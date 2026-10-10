package sqala.ir

import sqala.ast.table.SqlJoinType

enum IrTable:
    case Ident(name: String)
    case Subquery(withLateral: Boolean, query: IrQuery)
    case Join(left: IrTable, joinType: SqlJoinType, right: IrTable, on: Option[IrExpr])