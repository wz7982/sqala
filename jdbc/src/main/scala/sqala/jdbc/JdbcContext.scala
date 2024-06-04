package sqala.jdbc

import sqala.dsl.Result
import sqala.dsl.statement.dml.{Delete, Insert, Save, Update}
import sqala.dsl.statement.query.{Query, SelectQuery}
import sqala.util.{queryToString, statementToString}

import java.sql.{Connection, SQLException}
import javax.sql.DataSource
import scala.language.experimental.saferExceptions

class JdbcContext(val dataSource: DataSource, val dialect: Dialect)(using val logger: Logger):
    private[sqala] inline def execute[T](inline handler: Connection => T): T =
        val conn = dataSource.getConnection()
        val result = handler(conn)
        conn.close()
        result

    private[sqala] def executeDml(sql: String, args: Array[Any]): Int throws SQLException =
        logger(sql)
        execute(c => jdbcExec(c, sql, args))

    def execute(insert: Insert[?, ?]): Int throws SQLException =
        val (sql, args) = statementToString(insert.ast, dialect.printer(true))
        executeDml(sql, args)

    def executeReturnKey(insert: Insert[?, ?]): List[Long] throws SQLException =
        val (sql, args) = statementToString(insert.ast, dialect.printer(true))
        logger(sql)
        execute(c => jdbcExecReturnKey(c, sql, args))

    def execute(update: Update[?, ?]): Int throws SQLException =
        val (sql, args) = statementToString(update.ast, dialect.printer(true))
        executeDml(sql, args)

    def execute(delete: Delete[?]): Int throws SQLException =
        val (sql, args) = statementToString(delete.ast, dialect.printer(true))
        executeDml(sql, args)

    def execute(save: Save): Int throws SQLException =
        val (sql, args) = statementToString(save.ast, dialect.printer(true))
        executeDml(sql, args)

    def execute[T](query: Query[T])(using d: JdbcDecoder[Result[T]]): List[Result[T]] throws SQLException =
        val (sql, args) = queryToString(query.ast, dialect.printer(true))
        logger(sql)
        execute(c => jdbcQuery(c, sql, args))

    def page[T](query: SelectQuery[T], pageSize: Int, pageNo: Int, returnCount: Boolean = true)(using d: JdbcDecoder[Result[T]]): Page[Result[T]] throws SQLException =
        val data = if pageSize == 0 then Nil
            else execute(query.drop(if pageNo <= 1 then 0 else pageSize * (pageNo - 1)).take(pageSize))
        val count = if returnCount then execute(query.size).head else 0L
        val total = if count == 0 || pageSize == 0 then 0
            else if count % pageSize == 0 then count / pageSize
            else count / pageSize + 1
        Page(total, count, pageNo, pageSize, data)

    def find[T](query: Query[T])(using JdbcDecoder[Result[T]]): Option[Result[T]] throws SQLException =
        execute(query).headOption

    def size[T](query: SelectQuery[T]): Long throws SQLException =
        val sizeQuery = query.size
        execute(sizeQuery).head

    def exists[T](query: SelectQuery[T]): Boolean throws SQLException =
        val existsQuery = query.exists
        execute(existsQuery).head

    def showSql[T](query: Query[T]): String =
        queryToString(query.ast, dialect.printer(true))._1

    def transaction[T](block: JdbcTransactionContext ?=> T): T throws Exception =
        val conn = dataSource.getConnection()
        conn.setAutoCommit(false)
        try
            given JdbcTransactionContext = new JdbcTransactionContext(conn, dialect)
            val result = block
            conn.commit()
            result
        catch case e: Exception =>
            conn.rollback()
            throw e
        finally
            conn.setAutoCommit(true)
            conn.close()

    def transactionWithIsolation[T](isolation: Int)(block: JdbcTransactionContext ?=> T): T throws Exception =
        val conn = dataSource.getConnection()
        conn.setAutoCommit(false)
        conn.setTransactionIsolation(isolation)
        try
            given JdbcTransactionContext = new JdbcTransactionContext(conn, dialect)
            val result = block
            conn.commit()
            result
        catch case e: Exception =>
            conn.rollback()
            throw e
        finally
            conn.setAutoCommit(true)
            conn.close()