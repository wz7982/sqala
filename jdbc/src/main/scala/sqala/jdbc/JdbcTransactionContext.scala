package sqala.jdbc

import sqala.dsl.Result
import sqala.dsl.statement.dml.*
import sqala.dsl.statement.native.NativeSql
import sqala.dsl.statement.query.{Query, SelectQuery}
import sqala.printer.Dialect
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

def execute(nativeSql: NativeSql)(using t: JdbcTransactionContext, l: Logger): Int throws SQLException =
    val NativeSql(sql, args) = nativeSql
    l(sql)
    jdbcExec(t.connection, sql, args)

def fetchTo[T](query: Query[?])(using d: JdbcDecoder[T], t: JdbcTransactionContext, l: Logger): List[T] throws SQLException =
    val (sql, args) = queryToString(query.ast, t.dialect, true)
    l(sql)
    jdbcQuery(t.connection, sql, args)
    
def fetch[T](query: Query[T])(using r: Result[T], d: JdbcDecoder[r.R], c: JdbcTransactionContext, l: Logger): List[r.R] throws SQLException =
    fetchTo[r.R](query)

def fetchTo[T](nativeSql: NativeSql)(using d: JdbcDecoder[T], t: JdbcTransactionContext, l: Logger): List[T] throws SQLException =
    val NativeSql(sql, args) = nativeSql
    l(sql)
    jdbcQuery(t.connection, sql, args)

def findTo[T](query: Query[?])(using JdbcDecoder[T], JdbcTransactionContext, Logger): Option[T] throws SQLException =
    fetchTo[T](query).headOption

def find[T](query: Query[T])(using r: Result[T], d: JdbcDecoder[r.R], c: JdbcTransactionContext, l: Logger): Option[r.R] throws SQLException =
    findTo[r.R](query)

def fetchSize[T](query: SelectQuery[T])(using JdbcTransactionContext, Logger): Long throws SQLException =
    val sizeQuery = query.size
    fetch(sizeQuery).head

def exists[T](query: SelectQuery[T])(using JdbcTransactionContext, Logger): Boolean throws SQLException =
    val existsQuery = query.exists
    fetch(existsQuery).head

def pageTo[T](
    query: SelectQuery[?], pageSize: Int, pageNo: Int, returnCount: Boolean = true
)(using JdbcDecoder[T], JdbcTransactionContext, Logger): Page[T] throws SQLException =
    val data = if pageSize == 0 then Nil
        else fetchTo[T](query.drop(if pageNo <= 1 then 0 else pageSize * (pageNo - 1)).take(pageSize))
    val count = if returnCount then fetch(query.size).head else 0L
    val total = if count == 0 || pageSize == 0 then 0
        else if count % pageSize == 0 then count / pageSize
        else count / pageSize + 1
    Page(total, count, pageNo, pageSize, data)

def page[T](
    query: SelectQuery[T], pageSize: Int, pageNo: Int, returnCount: Boolean = true
)(using r: Result[T], d: JdbcDecoder[r.R], c: JdbcTransactionContext, l: Logger): Page[r.R] throws SQLException =
    pageTo[r.R](query, pageSize, pageNo, returnCount)

def showSql[T](query: Query[T])(using t: JdbcTransactionContext): String =
    queryToString(query.ast, t.dialect, true)._1