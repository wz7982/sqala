package sqala.static.statement.query

import sqala.ast.expr.*
import sqala.ast.statement.*
import sqala.ast.table.SqlTable
import sqala.printer.Dialect
import sqala.static.common.*
import sqala.static.macros.*
import sqala.util.queryToString

import scala.util.TupledFunction

sealed class Query[T](val ast: SqlQuery):
    def sql(dialect: Dialect, prepare: Boolean = true, indent: Int = 4): (String, Array[Any]) =
        queryToString(ast, dialect, prepare, indent)

// 加一个select query
// 加一个join query，join query调用filter 和 sortBy也变成select query
// join query 的join 每次只添加on里最后一个表的别名
// 只有table query和join query 有join方法
// 其他查询调用groupBy 变成group query
// 加一个group query 有having sort map
// map后变成Query

// Table query 和 join query 继承 Select query

class TableQuery[T](
    private val tableName: Option[String],
    override val ast: SqlQuery.Select
) extends Query[Table[T]](ast):
    private inline def replaceTableName(tableName: String): SqlQuery.Select =
        ast.from.head match
            case SqlTable.IdentTable(t, a) =>
                val newTable = SqlTable.IdentTable(
                    t, 
                    a.map(_.copy(tableAlias = tableName))
                )
                val metaData = TableMacro.tableMetaData[T]
                val selectItems = metaData.fieldNames.map: n =>
                    SqlSelectItem.Item(SqlExpr.Column(Some(tableName), n), None)
                ast.copy(select = selectItems, from = newTable :: Nil)
            case _ => ast

    inline def filter(inline f: Table[T] => Boolean): TableQuery[T] =
        val args = ClauseMacro.fetchArgNames(f)
        val newParam = tableName.getOrElse(args.head)
        val newAst = replaceTableName(newParam)
        val cond = ClauseMacro.fetchExpr(f, newParam :: Nil)
        TableQuery(Some(newParam), newAst.addWhere(cond))

    inline def withFilter(inline f: Table[T] => Boolean): TableQuery[T] =
        filter(f)

    inline def filterIf(test: => Boolean)(inline f: Table[T] => Boolean): TableQuery[T] =
        val args = ClauseMacro.fetchArgNames(f)
        val newParam = tableName.getOrElse(args.head)
        val newAst = replaceTableName(newParam)
        val cond = ClauseMacro.fetchExpr(f, newParam :: Nil)
        TableQuery(Some(newParam), if test then newAst.addWhere(cond) else newAst)

    inline def sortBy(inline f: Table[T] => Any): TableQuery[T] =
        val args = ClauseMacro.fetchArgNames(f)
        val newParam = tableName.getOrElse(args.head)
        val newAst = replaceTableName(newParam)
        val sortBy = ClauseMacro.fetchSortBy(f, newParam :: Nil)
        TableQuery(Some(newParam), newAst.copy(orderBy = newAst.orderBy ++ sortBy))