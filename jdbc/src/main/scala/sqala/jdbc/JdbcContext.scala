package sqala.jdbc

import sqala.dsl.Result
import sqala.dsl.macros.DmlMacro
import sqala.dsl.statement.dml.*
import sqala.dsl.statement.query.*
import sqala.printer.Dialect
import sqala.util.{queryToString, statementToString}

import java.sql.Connection
import javax.sql.DataSource
import scala.util.NotGiven
import scala.deriving.Mirror

class JdbcContext(val dataSource: DataSource, val dialect: Dialect)(using val logger: Logger):
    private[sqala] inline def execute[T](inline handler: Connection => T): T =
        val conn = dataSource.getConnection()
        val result = handler(conn)
        conn.close()
        result

    private[sqala] def executeDml(sql: String, args: Array[Any]): Int =
        logger(sql, args)
        execute(c => jdbcExec(c, sql, args))

    def execute(insert: Insert[?, ?])(using NotGiven[JdbcTransactionContext]): Int =
        val (sql, args) = statementToString(insert.ast, dialect, true)
        executeDml(sql, args)

    def execute(update: Update[?, ?])(using NotGiven[JdbcTransactionContext]): Int =
        val (sql, args) = statementToString(update.ast, dialect, true)
        executeDml(sql, args)

    def execute(delete: Delete[?])(using NotGiven[JdbcTransactionContext]): Int =
        val (sql, args) = statementToString(delete.ast, dialect, true)
        executeDml(sql, args)

    def execute(nativeSql: NativeSql)(using NotGiven[JdbcTransactionContext]): Int =
        val NativeSql(sql, args) = nativeSql
        executeDml(sql, args)

    inline def insert[A <: Product](entity: A)(using 
        Mirror.ProductOf[A], 
        NotGiven[JdbcTransactionContext]
    ): Int =
        val i = sqala.dsl.insert[A](entity)
        val (sql, args) = statementToString(i.ast, dialect, true)
        executeDml(sql, args)

    inline def insertBatch[A <: Product](entities: List[A])(using 
        Mirror.ProductOf[A], 
        NotGiven[JdbcTransactionContext]
    ): Int =
        val i = sqala.dsl.insert[A](entities)
        val (sql, args) = statementToString(i.ast, dialect, true)
        executeDml(sql, args)

    inline def insertAndReturn[A <: Product](entity: A)(using 
        Mirror.ProductOf[A], 
        NotGiven[JdbcTransactionContext]
    ): A =
        val i = sqala.dsl.insert[A](entity)
        val (sql, args) = statementToString(i.ast, dialect, true)
        val id = execute(c => jdbcExecReturnKey(c, sql, args)).head
        DmlMacro.bindGeneratedPrimaryKey[A](id, entity)

    inline def update[A <: Product](
        entity: A, 
        skipNone: Boolean = false
    )(using 
        Mirror.ProductOf[A], 
        NotGiven[JdbcTransactionContext]
    ): Int =
        val u = sqala.dsl.update(entity, skipNone)
        if u.ast.setList.isEmpty then
            0
        else
            val (sql, args) = statementToString(u.ast, dialect, true)
            executeDml(sql, args)

    inline def save[A <: Product](
        entity: A
    )(using 
        Mirror.ProductOf[A], 
        NotGiven[JdbcTransactionContext]
    ): Int =
        val s = sqala.dsl.save(entity)
        val (sql, args) = statementToString(s.ast, dialect, true)
        executeDml(sql, args)

    def fetchTo[T](query: Query[?, ?])(using JdbcDecoder[T], NotGiven[JdbcTransactionContext]): List[T] =
        val (sql, args) = queryToString(query.ast, dialect, true)
        logger(sql, args)
        execute(c => jdbcQuery(c, sql, args))

    def fetch[T](query: Query[T, ?])(using r: Result[T], d: JdbcDecoder[r.R], n: NotGiven[JdbcTransactionContext]): List[r.R] =
        fetchTo[r.R](query)

    def fetchTo[T](query: WithRecursive[?])(using JdbcDecoder[T], NotGiven[JdbcTransactionContext]): List[T] =
        val (sql, args) = queryToString(query.ast, dialect, true)
        logger(sql, args)
        execute(c => jdbcQuery(c, sql, args))

    def fetch[T](query: WithRecursive[T])(using r: Result[T], d: JdbcDecoder[r.R], n: NotGiven[JdbcTransactionContext]): List[r.R] =
        fetchTo[r.R](query)

    def fetch[T <: Record](nativeSql: StaticNativeSql[T])(using NotGiven[JdbcTransactionContext]): List[T] =
        logger(nativeSql.sql, nativeSql.args)
        execute(c => jdbcQueryToMap(c, nativeSql.sql, nativeSql.args).map(Record(_).asInstanceOf[T]))

    def fetchTo[T](nativeSql: NativeSql)(using JdbcDecoder[T], NotGiven[JdbcTransactionContext]): List[T] =
        val NativeSql(sql, args) = nativeSql
        logger(sql, args)
        execute(c => jdbcQuery(c, sql, args))

    def fetchToMap[T](nativeSql: NativeSql)(using JdbcDecoder[T], NotGiven[JdbcTransactionContext]): List[Map[String, Any]] =
        val NativeSql(sql, args) = nativeSql
        logger(sql, args)
        execute(c => jdbcQueryToMap(c, sql, args))

    def fetchTo[T](nativeSql: (String, Array[Any]))(using JdbcDecoder[T], NotGiven[JdbcTransactionContext]): List[T] =
        val (sql, args) = nativeSql
        logger(sql, args)
        execute(c => jdbcQuery(c, sql, args))

    def fetchToMap[T](nativeSql: (String, Array[Any]))(using JdbcDecoder[T], NotGiven[JdbcTransactionContext]): List[Map[String, Any]] =
        val (sql, args) = nativeSql
        logger(sql, args)
        execute(c => jdbcQueryToMap(c, sql, args))

    def pageTo[T](query: Query[?, ?], pageSize: Int, pageNo: Int, returnCount: Boolean = true)(using JdbcDecoder[T], NotGiven[JdbcTransactionContext]): Page[T] =
        val data = if pageSize == 0 then Nil
            else fetchTo[T](query.drop(if pageNo <= 1 then 0 else pageSize * (pageNo - 1)).take(pageSize))
        val count = if returnCount then fetch(query.size).head else 0L
        val total = if count == 0 || pageSize == 0 then 0
            else if count % pageSize == 0 then count / pageSize
            else count / pageSize + 1
        Page(total, count, pageNo, pageSize, data)

    def page[T](query: Query[T, ?], pageSize: Int, pageNo: Int, returnCount: Boolean = true)(using r: Result[T], d: JdbcDecoder[r.R], n: NotGiven[JdbcTransactionContext]): Page[r.R] =
        pageTo[r.R](query, pageSize, pageNo, returnCount)

    def findTo[T](query: Query[?, ?])(using JdbcDecoder[T], NotGiven[JdbcTransactionContext]): Option[T] =
        fetchTo[T](query.take(1)).headOption

    def find[T](query: Query[T, ?])(using r: Result[T], d: JdbcDecoder[r.R], n: NotGiven[JdbcTransactionContext]): Option[r.R] =
        findTo[r.R](query)

    def fetchSize[T](query: Query[T, ?])(using NotGiven[JdbcTransactionContext]): Long =
        val sizeQuery = query.size
        fetch(sizeQuery).head

    def fetchExists[T](query: Query[T, ?])(using NotGiven[JdbcTransactionContext]): Boolean =
        val existsQuery = query.exists
        fetch(existsQuery).head

    def showSql[T](query: Query[T, ?]): String =
        queryToString(query.ast, dialect, true)._1

    def transaction[T](block: JdbcTransactionContext ?=> T): T =
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

    def transactionWithIsolation[T](isolation: Int)(block: JdbcTransactionContext ?=> T): T =
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