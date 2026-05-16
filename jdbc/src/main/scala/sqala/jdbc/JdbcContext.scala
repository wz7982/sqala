package sqala.jdbc

import sqala.metadata.{Dialect, FetchPrimaryKey, InsertMacro}
import sqala.dynamic.native.NativeSql
import sqala.static.dsl.{QuerySize, Result}
import sqala.static.dsl.statement.dml.{Delete, Insert, Save, Update}
import sqala.static.dsl.statement.query.Query
import sqala.util.{queryToString, statementToString}

import java.sql.Connection
import javax.sql.DataSource
import scala.deriving.Mirror
import scala.language.dynamics
import scala.util.NotGiven

final class JdbcContext(
    private[sqala] val dataSource: DataSource,
    private[sqala] val dialect: Dialect,
    private[sqala] val standardEscapeStrings: Boolean
) extends Dynamic:
    private[sqala] def execute[T](handler: Connection => T)(using ec: ExecuteContext): T =
        val conn = ec.connection(this)
        val result = handler(conn)
        if !ec.inTransaction then
            conn.close()
        result

    private[sqala] def executeDml(sql: String, args: Array[Any])(using ec: ExecuteContext, l: Logger): Int =
        l(sql, args)
        execute(c => jdbcExec(c, sql, args))

    def execute(insert: Insert[?, ?])(using ExecuteContext, Logger): Int =
        val sql = statementToString(insert.tree, dialect, standardEscapeStrings)
        executeDml(sql, Array.empty[Any])

    def execute(update: Update[?, ?])(using ExecuteContext, Logger): Int =
        val sql = statementToString(update.tree, dialect, standardEscapeStrings)
        executeDml(sql, Array.empty[Any])

    def execute(delete: Delete[?])(using ExecuteContext, Logger): Int =
        val sql = statementToString(delete.tree, dialect, standardEscapeStrings)
        executeDml(sql, Array.empty[Any])

    def execute(nativeSql: NativeSql)(using ExecuteContext, Logger): Int =
        val NativeSql(sql, args) = nativeSql
        executeDml(sql, args)

    inline def insert[A <: Product](entity: A)(using
        ExecuteContext,
        Mirror.ProductOf[A],
        Logger
    ): Int =
        val i = Insert.insertByEntities[A](entity :: Nil)
        val sql = statementToString(i.tree, dialect, standardEscapeStrings)
        executeDml(sql, Array.empty[Any])

    inline def insertBatch[A <: Product](entities: Seq[A])(using
        ExecuteContext,
        Mirror.ProductOf[A],
        Logger
    ): Int =
        val i = Insert.insertByEntities[A](entities)
        val sql = statementToString(i.tree, dialect, standardEscapeStrings)
        executeDml(sql, Array.empty[Any])

    inline def insertAndReturn[A <: Product](entity: A)(using
        ec: ExecuteContext,
        m: Mirror.ProductOf[A],
        l: Logger
    ): A =
        val i = Insert.insertByEntities[A](entity :: Nil)
        val sql = statementToString(i.tree, dialect, standardEscapeStrings)
        l(sql, Array.empty[Any])
        val id = execute(c => jdbcExecReturnKey(c, sql, Array.empty[Any])).head
        InsertMacro.bindGeneratedPrimaryKey[A](id, entity)

    inline def update[A <: Product](
        entity: A,
        skipNone: Boolean = false
    )(using
        ExecuteContext,
        Mirror.ProductOf[A],
        Logger
    ): Int =
        val u = Update.updateByEntity[A](entity, skipNone)
        if u.tree.setList.isEmpty then
            0
        else
            val sql = statementToString(u.tree, dialect, standardEscapeStrings)
            executeDml(sql, Array.empty[Any])

    inline def save[A <: Product](
        entity: A
    )(using
        ExecuteContext,
        Mirror.ProductOf[A],
        Logger
    ): Int =
        val s = Save.saveByEntity[A](entity)
        val sql = statementToString(s.tree, dialect, standardEscapeStrings)
        executeDml(sql, Array.empty[Any])

    def deleteByPrimaryKey[T](using
        ec: ExecuteContext,
        fp: FetchPrimaryKey[T],
        l: Logger
    )(primaryKey: fp.Args): Int =
        val tree = fp.createDeleteTree(primaryKey :: Nil)
        val sql = statementToString(tree, dialect, standardEscapeStrings)
        executeDml(sql, Array.empty[Any])

    def deleteByPrimaryKeys[T](using
        ec: ExecuteContext,
        fp: FetchPrimaryKey[T],
        l: Logger
    )(primaryKeys: Seq[fp.Args]): Int =
        val tree = fp.createDeleteTree(primaryKeys)
        val sql = statementToString(tree, dialect, standardEscapeStrings)
        executeDml(sql, Array.empty[Any])

    def findByPrimaryKey[T](using
        ec: ExecuteContext,
        fp: FetchPrimaryKey[T],
        d: JdbcDecoder[T],
        l: Logger
    )(primaryKey: fp.Args): Option[T] =
        val tree = fp.createQueryTree(primaryKey :: Nil)
        val sql = queryToString(tree, dialect, standardEscapeStrings)
        l(sql, Array.empty[Any])
        execute(c => jdbcQuery(c, sql, Array.empty[Any])).headOption

    def fetchByPrimaryKeys[T](using
        ec: ExecuteContext,
        fp: FetchPrimaryKey[T],
        d: JdbcDecoder[T],
        l: Logger
    )(primaryKeys: Seq[fp.Args]): List[T] =
        val tree = fp.createQueryTree(primaryKeys)
        val sql = queryToString(tree, dialect, standardEscapeStrings)
        l(sql, Array.empty[Any])
        execute(c => jdbcQuery(c, sql, Array.empty[Any]))

    def fetchTo[T](query: Query[?, ?, ?, ?])(using
        ec: ExecuteContext,
        d: JdbcDecoder[T],
        l: Logger
    ): List[T] =
        val sql = queryToString(query.tree, dialect, standardEscapeStrings)
        l(sql, Array.empty[Any])
        execute(c => jdbcQuery(c, sql, Array.empty[Any]))

    def fetch[T, OKS <: Tuple, L <: Int, S <: QuerySize](query: Query[T, OKS, L, S])(using
        ec: ExecuteContext,
        r: Result[T],
        d: JdbcDecoder[r.R],
        l: Logger
    ): List[r.R] =
        fetchTo[r.R](query)

    def fetchTo[T](nativeSql: NativeSql)(using
        ec: ExecuteContext,
        d: JdbcDecoder[T],
        l: Logger
    ): List[T] =
        val NativeSql(sql, args) = nativeSql
        l(sql, args)
        execute(c => jdbcQuery(c, sql, args))

    def fetchToMap(nativeSql: NativeSql)(using ec: ExecuteContext, l: Logger): List[Map[String, Any]] =
        val NativeSql(sql, args) = nativeSql
        l(sql, args)
        execute(c => jdbcQueryToMap(c, sql, args))

    def fetchTo[T](nativeSql: (String, Array[Any]))(using
        ec: ExecuteContext,
        d: JdbcDecoder[T],
        l: Logger
    ): List[T] =
        val (sql, args) = nativeSql
        l(sql, args)
        execute(c => jdbcQuery(c, sql, args))

    def fetchToMap(nativeSql: (String, Array[Any]))(using ec: ExecuteContext, l: Logger): List[Map[String, Any]] =
        val (sql, args) = nativeSql
        l(sql, args)
        execute(c => jdbcQueryToMap(c, sql, args))

    def pageTo[T](
        query: Query[?, ?, ?, ?],
        pageSize: Int,
        pageNo: Int,
        returnCount: Boolean = true
    )(using
        ExecuteContext,
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

    def page[T, OKS <: Tuple, L <: Int, S <: QuerySize](
        query: Query[T, OKS, L, S],
        pageSize: Int,
        pageNo: Int,
        returnCount: Boolean = true
    )(using
        ec: ExecuteContext,
        r: Result[T],
        d: JdbcDecoder[r.R],
        l: Logger
    ): Page[r.R] =
        pageTo[r.R](query, pageSize, pageNo, returnCount)

    def findTo[T](query: Query[?, ?, ?, ?])(using
        ec: ExecuteContext,
        d: JdbcDecoder[T],
        l: Logger
    ): Option[T] =
        fetchTo[T](query.take(1)).headOption

    def find[T, OKS <: Tuple, L <: Int, S <: QuerySize](query: Query[T, OKS, L, S])(using
        ec: ExecuteContext,
        r: Result[T],
        d: JdbcDecoder[r.R],
        l: Logger
    ): Option[r.R] =
        findTo[r.R](query)

    def fetchCount[T, OKS <: Tuple, L <: Int, S <: QuerySize](query: Query[T, OKS, L, S])(using ExecuteContext, Logger): Long =
        val sizeQuery = query.size
        fetch(sizeQuery).head

    def fetchExists[T, OKS <: Tuple, L <: Int, S <: QuerySize](query: Query[T, OKS, L, S])(using ExecuteContext, Logger): Boolean =
        val existsQuery = query.exists
        fetch(existsQuery).head

    def applyDynamic[T](name: String)(using
        ec: ExecuteContext,
        r: Repository[T, name.type],
        d: JdbcDecoder[T],
        l: Logger
    )(args: r.Args): r.R =
        r.createQuery(
            dialect,
            standardEscapeStrings,
            args,
            q => fetchTo[T](q),
            q => findTo[T](q),
            (q, ps, pn, rc) => pageTo[T](q, ps, pn, rc),
            q => fetchCount(q),
            q => fetchExists(q)
        )

    def showSql[T, OKS <: Tuple, L <: Int, S <: QuerySize](query: Query[T, OKS, L, S]): String =
        queryToString(query.tree, dialect, standardEscapeStrings)

    def cursorFetch[T, OKS <: Tuple, L <: Int, S <: QuerySize, R](
        query: Query[T, OKS, L, S],
        fetchSize: Int
    )(using
        ec: ExecuteContext,
        r: Result[T],
        d: JdbcDecoder[r.R],
        l: Logger
    )(f: Cursor[r.R] => R): Unit =
        val sql = queryToString(query.tree, dialect, standardEscapeStrings)
        l(sql, Array.empty[Any])
        val conn = dataSource.getConnection()
        jdbcCursorQuery(conn, sql, Array.empty[Any], fetchSize, f)
        conn.close()

    def executeTransaction[T](
        block: JdbcTransactionContext ?=> T
    ): T =
        val conn = dataSource.getConnection()
        conn.setAutoCommit(false)
        try
            given JdbcTransactionContext =
                JdbcTransactionContext(conn)
            val result = block
            conn.commit()
            result
        catch case e: Exception =>
            conn.rollback()
            throw e
        finally
            conn.setAutoCommit(true)
            conn.close()

    def executeTransactionWithIsolation[T](
        isolation: TransactionIsolation
    )(block: JdbcTransactionContext ?=> T): T =
        val conn = dataSource.getConnection()
        conn.setAutoCommit(false)
        conn.setTransactionIsolation(isolation.jdbcIsolation)
        try
            given JdbcTransactionContext =
                JdbcTransactionContext(conn)
            val result = block
            conn.commit()
            result
        catch case e: Exception =>
            conn.rollback()
            throw e
        finally
            conn.setAutoCommit(true)
            conn.close()

final class JdbcTransactionContext(
    private[sqala] val connection: Connection
)

enum TransactionIsolation(private[sqala] val jdbcIsolation: Int):
    case None extends TransactionIsolation(Connection.TRANSACTION_NONE)
    case ReadUncommitted extends TransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED)
    case ReadCommitted extends TransactionIsolation(Connection.TRANSACTION_READ_COMMITTED)
    case RepeatableRead extends TransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ)
    case Serializable extends TransactionIsolation(Connection.TRANSACTION_SERIALIZABLE)

trait ExecuteContext:
    def inTransaction: Boolean

    def connection(context: JdbcContext): Connection

object ExecuteContext:
    given withTransaction(using c: JdbcTransactionContext): ExecuteContext with
        def inTransaction: Boolean =
            true

        def connection(context: JdbcContext): Connection =
            c.connection

    given withoutTransaction(using NotGiven[JdbcTransactionContext]): ExecuteContext with
        def inTransaction: Boolean =
            false

        def connection(context: JdbcContext): Connection =
            context.dataSource.getConnection()