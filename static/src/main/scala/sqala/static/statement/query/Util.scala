package sqala.static.statement.query

import sqala.ast.statement.SqlQuery
import sqala.ast.table.SqlTable

private[sqala] inline def replaceTableName[T: SelectItem](
    tables: T,
    tableNames: List[String],
    ast: SqlQuery.Select
): SqlQuery.Select =
    val iterator = tableNames.iterator

    def from(table: SqlTable): SqlTable = table match
        case SqlTable.IdentTable(t, a) =>
            val newTable = SqlTable.IdentTable(
                t,
                a.map(_.copy(tableAlias = iterator.next()))
            )
            newTable
        case SqlTable.SubQueryTable(q, l, a) =>
            SqlTable.SubQueryTable(q, l, a.copy(tableAlias = iterator.next()))
        case SqlTable.JoinTable(l, j, r, c) =>
            SqlTable.JoinTable(from(l), j, from(r), c)
        
    val select = summon[SelectItem[T]].selectItems(tables, tableNames)

    ast.copy(select = select, from = from(ast.from.head) :: Nil)