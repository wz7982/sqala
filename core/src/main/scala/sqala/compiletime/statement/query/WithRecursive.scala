package sqala.compiletime.statement.query

import sqala.ast.statement.*
import sqala.compiletime.{Column, SubQueryTable}

class WithRecursive[T <: Tuple, Table <: Tuple, QueryName <: String, ColumnNames <: Tuple](
    val ast: SqlQuery.Cte, 
    private[sqala] val queryName: QueryName,
    private[sqala] val subQueryTable: SubQueryTable[Table, QueryName, ColumnNames],
    private[sqala] val cols: List[Column[?, ?, ?]] = Nil
) extends Query[T, EmptyTuple]:
    def select[SelectT <: Tuple](f: SubQueryTable[Table, QueryName, ColumnNames] => Query[SelectT, ?]): WithRecursive[SelectT, Table, QueryName, ColumnNames] =
        val selectQuery = f(subQueryTable)
        val selectAst = ast.copy(query = selectQuery.ast)
        new WithRecursive(selectAst, queryName, subQueryTable)

object WithRecursive:
    def apply[T <: Tuple, QueryName <: String, ColumnNames <: Tuple](
        query: Query[T, ColumnNames], 
        queryName: QueryName
    )(
        f: (Query[T, ColumnNames], SubQueryTable[T, QueryName, ColumnNames]) => Query[T, ?]
    ): WithRecursive[T, T, QueryName, ColumnNames] =
        val subQueryTable = SubQueryTable[T, QueryName, ColumnNames](query, queryName, false, true)
        val withQuery = f(query, subQueryTable)
        val ast: SqlQuery.Cte = SqlQuery.Cte(SqlWithItem(queryName, withQuery.ast, Nil) :: Nil, true, withQuery.ast)
        new WithRecursive(ast, queryName, subQueryTable)