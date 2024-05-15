package sqala.jdbc

import sqala.dsl.Result
import sqala.dsl.statement.dml.*
import sqala.dsl.statement.query.{Query, SelectQuery}
import sqala.util.{queryToString, statementToString}

import java.sql.{Connection, SQLException}
import scala.language.experimental.saferExceptions

class JdbcTransactionContext(val connection: Connection, val dialect: Dialect)

def execute(insert: Insert[?, ?])(using t: JdbcTransactionContext, l: Logger): Int throws SQLException =
    val (sql, args) = statementToString(insert.ast, t.dialect, true)
    l(sql)
    jdbcExec(t.connection, sql, args)

def executeReturnKey(insert: Insert[?, ?])(using t: JdbcTransactionContext, l: Logger): List[Long] throws SQLException =
    val (sql, args) = statementToString(insert.ast, t.dialect, true)
    l(sql)
    jdbcExecReturnKey(t.connection, sql, args)

def execute(update: Update[?, ?])(using t: JdbcTransactionContext, l: Logger): Int throws SQLException =
    val (sql, args) = statementToString(update.ast, t.dialect, true)
    l(sql)
    jdbcExec(t.connection, sql, args)
    
def execute(delete: Delete[?])(using t: JdbcTransactionContext, l: Logger): Int throws SQLException =
    val (sql, args) = statementToString(delete.ast, t.dialect, true)
    l(sql)
    jdbcExec(t.connection, sql, args)

def execute(save: Save)(using t: JdbcTransactionContext, l: Logger): Int throws SQLException =
    val (sql, args) = statementToString(save.ast, t.dialect, true)
    l(sql)
    jdbcExec(t.connection, sql, args)
    
def execute[T](query: Query[T])(using d: JdbcDecoder[Result[T]], t: JdbcTransactionContext, l: Logger): List[Result[T]] throws SQLException =
    val (sql, args) = queryToString(query.ast, t.dialect, true)
    l(sql)
    jdbcQuery(t.connection, sql, args)

def find[T](query: Query[T])(using JdbcDecoder[Result[T]], JdbcTransactionContext, Logger): Option[Result[T]] throws SQLException =
    execute(query).headOption

def size[T](query: SelectQuery[T])(using JdbcTransactionContext, Logger): Long throws SQLException =
    val sizeQuery = query.size
    execute(sizeQuery).head

def exists[T](query: SelectQuery[T])(using JdbcTransactionContext, Logger): Boolean throws SQLException =
    val existsQuery = query.exists
    execute(existsQuery).head

def page[T](
    query: SelectQuery[T], pageSize: Int, pageNo: Int, returnCount: Boolean = true
)(using JdbcDecoder[Result[T]], JdbcTransactionContext, Logger): Page[Result[T]] throws SQLException =
    val data = if pageSize == 0 then Nil
        else execute(query.drop(if pageNo <= 1 then 0 else pageSize * (pageNo - 1)).take(pageSize))
    val count = if returnCount then execute(query.size).head else 0L
    val total = if count == 0 || pageSize == 0 then 0
        else if count % pageSize == 0 then count / pageSize
        else count / pageSize + 1
    Page(total, count, pageNo, pageSize, data)

def showSql[T](query: Query[T])(using t: JdbcTransactionContext): String =
    queryToString(query.ast, t.dialect, true)._1