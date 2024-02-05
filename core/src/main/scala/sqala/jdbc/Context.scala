package sqala.jdbc

import sqala.dsl.Result
import sqala.dsl.statement.dml.{Delete, Insert, Save, Update}
import sqala.dsl.statement.query.{Query, SelectQuery}
import sqala.util.{queryToString, statementToString}

import java.sql.Connection
import javax.sql.DataSource

class Context(val dataSource: DataSource, val dialect: Dialect)(using val logger: Logger):
    private[sqala] inline def execute[T](inline handler: Connection => T): T =
        val conn = dataSource.getConnection().nn
        val result = handler(conn)
        conn.close()
        result

    private[sqala] def executeDml(sql: String, args: Array[Any]): Int =
        logger(sql)
        execute(c => jdbcExec(c, sql, args))

    def execute(insert: Insert[?, ?]): Int =
        val (sql, args) = statementToString(insert.ast, dialect, true)
        executeDml(sql, args)

    def executeReturnKey(insert: Insert[?, ?]): List[Long] =
        val (sql, args) = statementToString(insert.ast, dialect, true)
        logger(sql)
        execute(c => jdbcExecReturnKey(c, sql, args))

    def execute(update: Update[?, ?]): Int =
        val (sql, args) = statementToString(update.ast, dialect, true)
        executeDml(sql, args)

    def execute(delete: Delete[?]): Int =
        val (sql, args) = statementToString(delete.ast, dialect, true)
        executeDml(sql, args)

    def execute(save: Save): Int =
        val (sql, args) = statementToString(save.ast, dialect, true)
        executeDml(sql, args)

    def execute[T](query: Query[T])(using d: Decoder[Result[T]]): List[Result[T]] =
        val (sql, args) = queryToString(query.ast, dialect, true)
        logger(sql)
        execute(c => jdbcQuery(c, sql, args))

    def page[T](query: SelectQuery[T], pageSize: Int, pageNo: Int, returnCount: Boolean = true)(using d: Decoder[Result[T]]): Page[Result[T]] =
        val data = if pageSize == 0 then Nil
            else execute(query.drop(if pageNo <= 1 then 0 else pageSize * (pageNo - 1)).take(pageSize))
        val count = if returnCount then execute(query.size).head else 0L
        val total = if count == 0 || pageSize == 0 then 0
            else if count % pageSize == 0 then count / pageSize
            else count / pageSize + 1
        Page(total, count, pageNo, pageSize, data)

    def transaction[T](block: TransactionContext ?=> T): T =
        val conn = dataSource.getConnection().nn
        conn.setAutoCommit(false)
        try
            given TransactionContext = new TransactionContext(conn, dialect)
            val result = block
            conn.commit()
            result
        catch case e: Exception =>
            conn.rollback()
            throw e
        finally
            conn.setAutoCommit(true)
            conn.close()

    def transactionWithIsolation[T](isolation: Int)(block: TransactionContext ?=> T): T =
        val conn = dataSource.getConnection().nn
        conn.setAutoCommit(false)
        conn.setTransactionIsolation(isolation)
        try
            given TransactionContext = new TransactionContext(conn, dialect)
            val result = block
            conn.commit()
            result
        catch case e: Exception =>
            conn.rollback()
            throw e
        finally
            conn.setAutoCommit(true)
            conn.close()