package sqala.jdbc

import sqala.dsl.statement.dml.Insert
import sqala.util.statementToString

import java.sql.Connection
import sqala.dsl.statement.dml.Update
import sqala.dsl.statement.dml.Delete
import sqala.dsl.statement.dml.Save
import sqala.dsl.statement.query.Query
import sqala.dsl.Result
import sqala.util.queryToString
import sqala.dsl.statement.query.SelectQuery

class TransactionContext(val connection: Connection, val dialect: Dialect)

def execute(insert: Insert[?, ?])(using t: TransactionContext, l: Logger): Int =
    val (sql, args) = statementToString(insert.ast, t.dialect, true)
    l(sql)
    jdbcExec(t.connection, sql, args)

def executeReturnKey(insert: Insert[?, ?])(using t: TransactionContext, l: Logger): List[Long] =
    val (sql, args) = statementToString(insert.ast, t.dialect, true)
    l(sql)
    jdbcExecReturnKey(t.connection, sql, args)

def execute(update: Update[?, ?])(using t: TransactionContext, l: Logger): Int =
    val (sql, args) = statementToString(update.ast, t.dialect, true)
    l(sql)
    jdbcExec(t.connection, sql, args)
    
def execute(delete: Delete[?])(using t: TransactionContext, l: Logger): Int =
    val (sql, args) = statementToString(delete.ast, t.dialect, true)
    l(sql)
    jdbcExec(t.connection, sql, args)

def execute(save: Save)(using t: TransactionContext, l: Logger): Int =
    val (sql, args) = statementToString(save.ast, t.dialect, true)
    l(sql)
    jdbcExec(t.connection, sql, args)
    
def execute[T](query: Query[T])(using d: Decoder[Result[T]], t: TransactionContext, l: Logger): List[Result[T]] =
    val (sql, args) = queryToString(query.ast, t.dialect, true)
    l(sql)
    jdbcQuery(t.connection, sql, args)


def page[T](
    query: SelectQuery[T], pageSize: Int, pageNo: Int, returnCount: Boolean = true
)(using Decoder[Result[T]], TransactionContext, Logger): Page[Result[T]] =
    val data = if pageSize == 0 then Nil
        else execute(query.drop(if pageNo <= 1 then 0 else pageSize * (pageNo - 1)).take(pageSize))
    val count = if returnCount then execute(query.size).head else 0L
    val total = if count == 0 || pageSize == 0 then 0
        else if count % pageSize == 0 then count / pageSize
        else count / pageSize + 1
    Page(total, count, pageNo, pageSize, data)