package sqala.jdbc

import sqala.dynamic.dsl.{DynamicQuery, NativeSql}
import sqala.printer.Dialect
import sqala.static.dsl.Result
import sqala.static.dsl.statement.dml.{Delete, Insert, Save, Update}
import sqala.static.dsl.statement.query.Query
import sqala.static.metadata.{FetchPk, InsertMacro}
import sqala.util.{queryToString, statementToString}

import java.sql.Connection
import javax.sql.DataSource
import scala.deriving.Mirror

class JdbcContext[Url <: String, Username <: String, Password <: String, Driver <: String](
    val dataSource: DataSource, 
    val dialect: Dialect
):
    private[sqala] inline def execute[T](inline handler: Connection => T): T =
        val conn = dataSource.getConnection()
        val result = handler(conn)
        conn.close()
        result

    private[sqala] def executeDml(sql: String, args: Array[Any])(using l: Logger): Int =
        l(sql, args)
        execute(c => jdbcExec(c, sql, args))

    def execute(insert: Insert[?, ?])(using Logger): Int =
        val (sql, args) = statementToString(insert.tree, dialect, true)
        executeDml(sql, args)

    def execute(update: Update[?, ?])(using Logger): Int =
        val (sql, args) = statementToString(update.tree, dialect, true)
        executeDml(sql, args)

    def execute(delete: Delete[?])(using Logger): Int =
        val (sql, args) = statementToString(delete.tree, dialect, true)
        executeDml(sql, args)

    def execute(nativeSql: NativeSql)(using Logger): Int =
        val NativeSql(sql, args) = nativeSql
        executeDml(sql, args)

    inline def insert[A <: Product](entity: A)(using
        Mirror.ProductOf[A],
        Logger
    ): Int =
        val i = Insert.insertByEntities[A](entity :: Nil)
        val (sql, args) = statementToString(i.tree, dialect, true)
        executeDml(sql, args)

    inline def insertBatch[A <: Product](entities: Seq[A])(using
        Mirror.ProductOf[A],
        Logger
    ): Int =
        val i = Insert.insertByEntities[A](entities)
        val (sql, args) = statementToString(i.tree, dialect, true)
        executeDml(sql, args)

    inline def insertAndReturn[A <: Product](entity: A)(using
        Mirror.ProductOf[A],
        Logger
    ): A =
        val i = Insert.insertByEntities[A](entity :: Nil)
        val (sql, args) = statementToString(i.tree, dialect, true)
        val id = execute(c => jdbcExecReturnKey(c, sql, args)).head
        InsertMacro.bindGeneratedPrimaryKey[A](id, entity)

    inline def update[A <: Product](
        entity: A,
        skipNone: Boolean = false
    )(using
        Mirror.ProductOf[A],
        Logger
    ): Int =
        val u = Update.updateByEntity[A](entity, skipNone)
        if u.tree.setList.isEmpty then
            0
        else
            val (sql, args) = statementToString(u.tree, dialect, true)
            executeDml(sql, args)

    inline def save[A <: Product](
        entity: A
    )(using
        Mirror.ProductOf[A],
        Logger
    ): Int =
        val s = Save.saveByEntity[A](entity)
        val (sql, args) = statementToString(s.tree, dialect, true)
        executeDml(sql, args)

    inline def fetchByPrimaryKeys[T](using 
        fp: FetchPk[T], 
        d: JdbcDecoder[T],
        l: Logger
    )(pks: Seq[fp.R]): List[T] =
        val tree = fp.createTree(pks)
        val (sql, args) = queryToString(tree, dialect, true)
        l(sql, args)
        execute(c => jdbcQuery(c, sql, args))

    inline def findByPrimaryKey[T](using 
        fp: FetchPk[T], 
        d: JdbcDecoder[T],
        l: Logger
    )(pk: fp.R): Option[T] =
        val tree = fp.createTree(pk :: Nil)
        val (sql, args) = queryToString(tree, dialect, true)
        l(sql, args)
        execute(c => jdbcQuery(c, sql, args)).headOption

    inline def fetchTo[T](inline query: Query[?])(using 
        d: JdbcDecoder[T],
        l: Logger
    ): List[T] =
        val (sql, args) = queryToString(query.tree, dialect, true)
        l(sql, args)
        execute(c => jdbcQuery(c, sql, args))

    inline def fetch[T](inline query: Query[T])(using 
        r: Result[T], 
        d: JdbcDecoder[r.R],
        l: Logger
    ): List[r.R] =
        fetchTo[r.R](query)

    def fetchTo[T](nativeSql: NativeSql)(using 
        d: JdbcDecoder[T],
        l: Logger
    ): List[T] =
        val NativeSql(sql, args) = nativeSql
        l(sql, args)
        execute(c => jdbcQuery(c, sql, args))

    transparent inline def fetch(inline nativeSql: NativeSql)(using l: Logger): Any =
        val exec = (c: Connection) => (n: NativeSql) =>
            val NativeSql(sql, args) = n
            l(sql, args)
            val result = jdbcQueryToMap(c, sql, args)
            c.close()
            result
        GenerateRecord.run[Url, Username, Password, Driver](dataSource.getConnection, exec, nativeSql)

    def fetchToMap(nativeSql: NativeSql)(using l: Logger): List[Map[String, Any]] =
        val NativeSql(sql, args) = nativeSql
        l(sql, args)
        execute(c => jdbcQueryToMap(c, sql, args))

    def fetchTo[T](nativeSql: (String, Array[Any]))(using 
        d: JdbcDecoder[T],
        l: Logger
    ): List[T] =
        val (sql, args) = nativeSql
        l(sql, args)
        execute(c => jdbcQuery(c, sql, args))

    def fetchToMap(nativeSql: (String, Array[Any]))(using l: Logger): List[Map[String, Any]] =
        val (sql, args) = nativeSql
        l(sql, args)
        execute(c => jdbcQueryToMap(c, sql, args))

    def fetchTo[T](query: DynamicQuery)(using 
        d: JdbcDecoder[T],
        l: Logger
    ): List[T] =
        val (sql, args) = queryToString(query.tree, dialect, true)
        l(sql, args)
        execute(c => jdbcQuery(c, sql, args))

    def fetchToMap(query: DynamicQuery)(using l: Logger): List[Map[String, Any]] =
        val (sql, args) = queryToString(query.tree, dialect, true)
        l(sql, args)
        execute(c => jdbcQueryToMap(c, sql, args))

    inline def pageTo[T]( 
        inline query: Query[?], pageSize: Int, pageNo: Int, returnCount: Boolean = true
    )(using 
        JdbcDecoder[T],
        Logger
    ): Page[T] =
        val data = if pageSize == 0 then Nil
            else fetchTo[T](query.drop(if pageNo <= 1 then 0 else pageSize * (pageNo - 1)).take(pageSize))
        val count = if returnCount then fetch(query.size).head else 0L
        val total = if count == 0 || pageSize == 0 then 0
            else if count % pageSize == 0 then (count / pageSize).toInt
            else (count / pageSize + 1).toInt
        Page(total, count, pageSize, pageNo, data)

    inline def page[T](
        inline query: Query[T], pageSize: Int, pageNo: Int, returnCount: Boolean = true
    )(using 
        r: Result[T], 
        d: JdbcDecoder[r.R],
        l: Logger
    ): Page[r.R] =
        pageTo[r.R](query, pageSize, pageNo, returnCount)

    inline def findTo[T](inline query: Query[?])(using 
        d: JdbcDecoder[T],
        l: Logger
    ): Option[T] =
        fetchTo[T](query.take(1)).headOption

    inline def find[T](inline query: Query[T])(using 
        r: Result[T], 
        d: JdbcDecoder[r.R],
        l: Logger
    ): Option[r.R] =
        findTo[r.R](query)

    inline def fetchSize[T](inline query: Query[T])(using Logger): Long =
        val sizeQuery = query.size
        fetch(sizeQuery).head

    inline def fetchExists(inline query: Query[?])(using Logger): Boolean =
        val existsQuery = query.exists
        fetch(existsQuery).head

    def showSql[T](query: Query[T]): String =
        queryToString(query.tree, dialect, true)._1

    inline def cursorFetch[T, R](
        inline query: Query[T],
        fetchSize: Int
    )(using
        r: Result[T],
        d: JdbcDecoder[r.R],
        l: Logger
    )(f: Cursor[r.R] => R): Unit =
        val (sql, args) = queryToString(query.tree, dialect, true)
        l(sql, args)
        val conn = dataSource.getConnection()
        jdbcCursorQuery(conn, sql, args, fetchSize, f)
        conn.close()

    transparent inline def executeTransaction[T](
        block: JdbcTransactionContext ?=> T
    ): T =
        val conn = dataSource.getConnection()
        conn.setAutoCommit(false)
        try
            given JdbcTransactionContext = 
                new JdbcTransactionContext(conn, dialect)
            val result = block
            conn.commit()
            result
        catch case e: Exception =>
            conn.rollback()
            throw e
        finally
            conn.setAutoCommit(true)
            conn.close()

    transparent inline def executeTransactionWithIsolation[T](
        isolation: TransactionIsolation
    )(block: JdbcTransactionContext ?=> T): T =
        val conn = dataSource.getConnection()
        conn.setAutoCommit(false)
        conn.setTransactionIsolation(isolation.jdbcIsolation)
        try
            given JdbcTransactionContext = 
                new JdbcTransactionContext(conn, dialect)
            val result = block
            conn.commit()
            result
        catch case e: Exception =>
            conn.rollback()
            throw e
        finally
            conn.setAutoCommit(true)
            conn.close()

object JdbcContext:
    inline def apply[S <: DataSource](
        dialect: Dialect,
        url: String, 
        username: String, 
        password: String, 
        driverClassName: String
    )(using c: JdbcConnection[S]): JdbcContext[url.type, username.type, password.type, driverClassName.type] =
        new JdbcContext(c.init(url, username, password, driverClassName), dialect)