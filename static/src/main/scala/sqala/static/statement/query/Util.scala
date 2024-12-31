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
        case SqlTable.Range(t, a) =>
            val newTable = SqlTable.Range(
                t,
                a.map(_.copy(tableAlias = iterator.next()))
            )
            newTable
        case SqlTable.Func(n, args, a) =>
            val newTable = SqlTable.Func(
                n,
                args,
                a.map(_.copy(tableAlias = iterator.next()))
            )
            newTable
        case SqlTable.SubQuery(q, l, a) =>
            SqlTable.SubQuery(q, l, a.map(_.copy(tableAlias = iterator.next())))
        case SqlTable.Join(l, j, r, c, _) =>
            SqlTable.Join(from(l), j, from(r), c, None)

    val select = summon[SelectItem[T]].selectItems(tables, tableNames)

    ast.copy(select = select, from = from(ast.from.head) :: Nil)