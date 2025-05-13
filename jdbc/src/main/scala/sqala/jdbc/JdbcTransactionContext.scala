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
import scala.deriving.Mirror

class JdbcTransactionContext[D <: Dialect](val connection: Connection, val dialect: D)(using 
    val logger: Logger
):
    def execute(insert: Insert[?, ?]): Int =
        val (sql, args) = statementToString(insert.tree, dialect, true)
        logger(sql, args)
        jdbcExec(connection, sql, args)

    def execute(update: Update[?, ?]): Int =
        val (sql, args) = statementToString(update.tree, dialect, true)
        logger(sql, args)
        jdbcExec(connection, sql, args)
        
    def execute(delete: Delete[?]): Int =
        val (sql, args) = statementToString(delete.tree, dialect, true)
        logger(sql, args)
        jdbcExec(connection, sql, args)

    def execute(nativeSql: NativeSql): Int =
        val NativeSql(sql, args) = nativeSql
        logger(sql, args)
        jdbcExec(connection, sql, args)

    inline def insert[A <: Product](entity: A)(using 
        m: Mirror.ProductOf[A]
    ): Int =
        val i = sqala.static.dsl.insert[A](entity)
        val (sql, args) = statementToString(i.tree, dialect, true)
        jdbcExec(connection, sql, args)

    inline def insertBatch[A <: Product](entities: List[A])(using 
        m: Mirror.ProductOf[A]
    ): Int =
        val i = sqala.static.dsl.insert[A](entities)
        val (sql, args) = statementToString(i.tree, dialect, true)
        jdbcExec(connection, sql, args)

    inline def insertAndReturn[A <: Product](entity: A)(using 
        m: Mirror.ProductOf[A]
    ): A =
        val i = sqala.static.dsl.insert[A](entity)
        val (sql, args) = statementToString(i.tree, dialect, true)
        val id = jdbcExecReturnKey(connection, sql, args).head
        InsertMacro.bindGeneratedPrimaryKey[A](id, entity)

    inline def update[A <: Product](
        entity: A, 
        skipNone: Boolean = false
    )(using 
        m: Mirror.ProductOf[A]
    ): Int =
        val u = sqala.static.dsl.update(entity, skipNone)
        if u.tree.setList.isEmpty then
            0
        else
            val (sql, args) = statementToString(u.tree, dialect, true)
            jdbcExec(connection, sql, args)

    inline def save[A <: Product](
        entity: A
    )(using 
        m: Mirror.ProductOf[A]
    ): Int =
        val s = sqala.static.dsl.save(entity)
        val (sql, args) = statementToString(s.tree, dialect, true)
        jdbcExec(connection, sql, args)

    inline def fetchTo[T](inline query: Query[?])(using 
        d: JdbcDecoder[T]
    ): List[T] =
        AnalysisMacro.showQuery[D](query)
        val (sql, args) = queryToString(query.tree, dialect, true)
        logger(sql, args)
        jdbcQuery(connection, sql, args)
        
    inline def fetch[T](inline query: Query[T])(using 
        r: Result[T], 
        d: JdbcDecoder[r.R]
    ): List[r.R] =
        fetchTo[r.R](query)

    def fetchTo[T](nativeSql: NativeSql)(using 
        d: JdbcDecoder[T]
    ): List[T] =
        val NativeSql(sql, args) = nativeSql
        logger(sql, args)
        jdbcQuery(connection, sql, args)

    def fetchToMap(nativeSql: NativeSql): List[Map[String, Any]] =
        val NativeSql(sql, args) = nativeSql
        logger(sql, args)
        jdbcQueryToMap(connection, sql, args)

    def fetchTo[T](nativeSql: (String, Array[Any]))(using 
        d: JdbcDecoder[T]
    ): List[T] =
        val (sql, args) = nativeSql
        logger(sql, args)
        jdbcQuery(connection, sql, args)

    def fetchToMap(nativeSql: (String, Array[Any])): List[Map[String, Any]] =
        val (sql, args) = nativeSql
        logger(sql, args)
        jdbcQueryToMap(connection, sql, args)

    def fetchTo[T](query: DynamicQuery)(using 
        d: JdbcDecoder[T]
    ): List[T] =
        val (sql, args) = queryToString(query.tree, dialect, true)
        logger(sql, args)
        jdbcQuery(connection, sql, args)

    def fetchToMap(query: DynamicQuery): List[Map[String, Any]] =
        val (sql, args) = queryToString(query.tree, dialect, true)
        logger(sql, args)
        jdbcQueryToMap(connection, sql, args)

    inline def findTo[T](inline query: Query[?])(using 
        JdbcDecoder[T]
    ): Option[T] =
        AnalysisMacro.showLimitQuery[D](query)
        fetchTo[T](query.take(1)).headOption

    inline def find[T](inline query: Query[T])(using 
        r: Result[T], 
        d: JdbcDecoder[r.R]
    ): Option[r.R] =
        findTo[r.R](query)

    inline def pageTo[T](
        inline query: Query[?], pageSize: Int, pageNo: Int, returnCount: Boolean = true
    )(using 
        JdbcDecoder[T]
    ): Page[T] =
        AnalysisMacro.showPageQuery[D](query)
        val data = if pageSize == 0 then Nil
            else fetchTo[T](query.drop(if pageNo <= 1 then 0 else pageSize * (pageNo - 1)).take(pageSize))
        val count = if returnCount then fetch(query.size).head else 0L
        val total = if count == 0 || pageSize == 0 then 0
            else if count % pageSize == 0 then (count / pageSize).toInt
            else (count / pageSize + 1).toInt
        Page(total, count, pageNo, pageSize, data)

    inline def page[T](
        inline query: Query[T], pageSize: Int, pageNo: Int, returnCount: Boolean = true
    )(using 
        r: Result[T], 
        d: JdbcDecoder[r.R]
    ): Page[r.R] =
        pageTo[r.R](query, pageSize, pageNo, returnCount)

    inline def fetchSize[T](inline query: Query[T]): Long =
        AnalysisMacro.showSizeQuery[D](query)
        val sizeQuery = query.size
        fetch(sizeQuery).head

    inline def fetchExists[T](inline query: Query[T]): Boolean =
        AnalysisMacro.showExistsQuery[D](query)
        val existsQuery = query.exists
        fetch(existsQuery).head