package sqala.jdbc

import sqala.dynamic.dsl.{DynamicQuery, NativeSql}
import sqala.metadata.InsertMacro
import sqala.printer.Dialect
import sqala.static.dsl.Result
import sqala.static.dsl.analysis.AnalysisMacro
import sqala.static.dsl.statement.dml.{Delete, Insert, Update}
import sqala.static.dsl.statement.query.Query
import sqala.util.{queryToString, statementToString}

import java.sql.Connection
import javax.sql.DataSource
import scala.deriving.Mirror
import scala.util.NotGiven

class JdbcContext[D <: Dialect](val dataSource: DataSource, val dialect: D)(using val logger: Logger):
    private[sqala] inline def execute[T](inline handler: Connection => T): T =
        val conn = dataSource.getConnection()
        val result = handler(conn)
        conn.close()
        result

    private[sqala] def executeDml(sql: String, args: Array[Any]): Int =
        logger(sql, args)
        execute(c => jdbcExec(c, sql, args))

    def execute(insert: Insert[?, ?])(using NotGiven[JdbcTransactionContext[?]]): Int =
        val (sql, args) = statementToString(insert.tree, dialect, true)
        executeDml(sql, args)

    def execute(update: Update[?, ?])(using NotGiven[JdbcTransactionContext[?]]): Int =
        val (sql, args) = statementToString(update.tree, dialect, true)
        executeDml(sql, args)

    def execute(delete: Delete[?])(using NotGiven[JdbcTransactionContext[?]]): Int =
        val (sql, args) = statementToString(delete.tree, dialect, true)
        executeDml(sql, args)

    def execute(nativeSql: NativeSql)(using NotGiven[JdbcTransactionContext[?]]): Int =
        val NativeSql(sql, args) = nativeSql
        executeDml(sql, args)

    inline def insert[A <: Product](entity: A)(using
        Mirror.ProductOf[A],
        NotGiven[JdbcTransactionContext[?]]
    ): Int =
        val i = sqala.static.dsl.insert[A](entity)
        val (sql, args) = statementToString(i.tree, dialect, true)
        executeDml(sql, args)

    inline def insertBatch[A <: Product](entities: List[A])(using
        Mirror.ProductOf[A],
        NotGiven[JdbcTransactionContext[?]]
    ): Int =
        val i = sqala.static.dsl.insert[A](entities)
        val (sql, args) = statementToString(i.tree, dialect, true)
        executeDml(sql, args)

    inline def insertAndReturn[A <: Product](entity: A)(using
        Mirror.ProductOf[A],
        NotGiven[JdbcTransactionContext[?]]
    ): A =
        val i = sqala.static.dsl.insert[A](entity)
        val (sql, args) = statementToString(i.tree, dialect, true)
        val id = execute(c => jdbcExecReturnKey(c, sql, args)).head
        InsertMacro.bindGeneratedPrimaryKey[A](id, entity)

    inline def update[A <: Product](
        entity: A,
        skipNone: Boolean = false
    )(using
        Mirror.ProductOf[A],
        NotGiven[JdbcTransactionContext[?]]
    ): Int =
        val u = sqala.static.dsl.update(entity, skipNone)
        if u.tree.setList.isEmpty then
            0
        else
            val (sql, args) = statementToString(u.tree, dialect, true)
            executeDml(sql, args)

    inline def save[A <: Product](
        entity: A
    )(using
        Mirror.ProductOf[A],
        NotGiven[JdbcTransactionContext[?]]
    ): Int =
        val s = sqala.static.dsl.save(entity)
        val (sql, args) = statementToString(s.tree, dialect, true)
        executeDml(sql, args)

    inline def fetchTo[T](inline query: Query[?])(using 
        JdbcDecoder[T], 
        NotGiven[JdbcTransactionContext[?]]
    ): List[T] =
        AnalysisMacro.showQuery(query)
        val (sql, args) = queryToString(query.tree, dialect, true)
        logger(sql, args)
        execute(c => jdbcQuery(c, sql, args))

    inline def fetch[T](inline query: Query[T])(using 
        r: Result[T], 
        d: JdbcDecoder[r.R], 
        n: NotGiven[JdbcTransactionContext[?]]
    ): List[r.R] =
        fetchTo[r.R](query)

    def fetchTo[T](nativeSql: NativeSql)(using 
        JdbcDecoder[T], 
        NotGiven[JdbcTransactionContext[?]]
    ): List[T] =
        val NativeSql(sql, args) = nativeSql
        logger(sql, args)
        execute(c => jdbcQuery(c, sql, args))

    def fetchToMap(nativeSql: NativeSql)(using 
        NotGiven[JdbcTransactionContext[?]]
    ): List[Map[String, Any]] =
        val NativeSql(sql, args) = nativeSql
        logger(sql, args)
        execute(c => jdbcQueryToMap(c, sql, args))

    def fetchTo[T](nativeSql: (String, Array[Any]))(using 
        JdbcDecoder[T], 
        NotGiven[JdbcTransactionContext[?]]
    ): List[T] =
        val (sql, args) = nativeSql
        logger(sql, args)
        execute(c => jdbcQuery(c, sql, args))

    def fetchToMap(nativeSql: (String, Array[Any]))(using 
        NotGiven[JdbcTransactionContext[?]]
    ): List[Map[String, Any]] =
        val (sql, args) = nativeSql
        logger(sql, args)
        execute(c => jdbcQueryToMap(c, sql, args))

    def fetchTo[T](query: DynamicQuery)(using 
        JdbcDecoder[T], 
        NotGiven[JdbcTransactionContext[?]]
    ): List[T] =
        val (sql, args) = queryToString(query.tree, dialect, true)
        logger(sql, args)
        execute(c => jdbcQuery(c, sql, args))

    def fetchToMap(query: DynamicQuery)(using 
        NotGiven[JdbcTransactionContext[?]]
    ): List[Map[String, Any]] =
        val (sql, args) = queryToString(query.tree, dialect, true)
        logger(sql, args)
        execute(c => jdbcQueryToMap(c, sql, args))

    inline def pageTo[T](inline query: Query[?], pageSize: Int, pageNo: Int, returnCount: Boolean = true)(using 
        JdbcDecoder[T], 
        NotGiven[JdbcTransactionContext[?]]
    ): Page[T] =
        AnalysisMacro.showPageQuery(query)
        val data = if pageSize == 0 then Nil
            else fetchTo[T](query.drop(if pageNo <= 1 then 0 else pageSize * (pageNo - 1)).take(pageSize))
        val count = if returnCount then fetch(query.size).head else 0L
        val total = if count == 0 || pageSize == 0 then 0
            else if count % pageSize == 0 then (count / pageSize).toInt
            else (count / pageSize + 1).toInt
        Page(total, count, pageNo, pageSize, data)

    inline def page[T](inline query: Query[T], pageSize: Int, pageNo: Int, returnCount: Boolean = true)(using 
        r: Result[T], 
        d: JdbcDecoder[r.R], 
        n: NotGiven[JdbcTransactionContext[?]]
    ): Page[r.R] =
        pageTo[r.R](query, pageSize, pageNo, returnCount)

    inline def findTo[T](inline query: Query[?])(using 
        JdbcDecoder[T], 
        NotGiven[JdbcTransactionContext[?]]
    ): Option[T] =
        AnalysisMacro.showLimitQuery(query)
        fetchTo[T](query.take(1)).headOption

    inline def find[T](inline query: Query[T])(using 
        r: Result[T], 
        d: JdbcDecoder[r.R], 
        n: NotGiven[JdbcTransactionContext[?]]
    ): Option[r.R] =
        findTo[r.R](query)

    inline def fetchSize[T](inline query: Query[T])(using 
        NotGiven[JdbcTransactionContext[?]]
    ): Long =
        AnalysisMacro.showSizeQuery(query)
        val sizeQuery = query.size
        fetch(sizeQuery).head

    inline def fetchExists(inline query: Query[?])(using 
        NotGiven[JdbcTransactionContext[?]]
    ): Boolean =
        AnalysisMacro.showExistsQuery(query)
        val existsQuery = query.exists
        fetch(existsQuery).head

    def showSql[T](query: Query[T]): String =
        queryToString(query.tree, dialect, true)._1

    inline def cursorFetch[T, R](
        inline query: Query[T],
        fetchSize: Int
    )(using
        r: Result[T],
        d: JdbcDecoder[r.R]
    )(f: Cursor[r.R] => R): Unit =
        AnalysisMacro.showQuery(query)
        val (sql, args) = queryToString(query.tree, dialect, true)
        logger(sql, args)
        val conn = dataSource.getConnection()
        jdbcCursorQuery(conn, sql, args, fetchSize, f)
        conn.close()

    def transaction[T](block: JdbcTransactionContext[D] ?=> T): T =
        val conn = dataSource.getConnection()
        conn.setAutoCommit(false)
        try
            given JdbcTransactionContext[D] = new JdbcTransactionContext(conn, dialect)
            val result = block
            conn.commit()
            result
        catch case e: Exception =>
            conn.rollback()
            throw e
        finally
            conn.setAutoCommit(true)
            conn.close()

    def transactionWithIsolation[T](isolation: Int)(block: JdbcTransactionContext[D] ?=> T): T =
        val conn = dataSource.getConnection()
        conn.setAutoCommit(false)
        conn.setTransactionIsolation(isolation)
        try
            given JdbcTransactionContext[D] = new JdbcTransactionContext(conn, dialect)
            val result = block
            conn.commit()
            result
        catch case e: Exception =>
            conn.rollback()
            throw e
        finally
            conn.setAutoCommit(true)
            conn.close()