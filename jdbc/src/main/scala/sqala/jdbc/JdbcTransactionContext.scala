package sqala.jdbc

import sqala.dynamic.dsl.{DynamicQuery, NativeSql}
import sqala.printer.Dialect
import sqala.static.dsl.Result
import sqala.static.dsl.statement.dml.{Delete, Insert, Save, Update}
import sqala.static.dsl.statement.query.Query
import sqala.static.metadata.{FetchPk, InsertMacro}
import sqala.util.{queryToString, statementToString}

import java.sql.Connection
import scala.deriving.Mirror
import scala.language.dynamics

class JdbcTransactionContext(
    val connection: Connection, 
    val dialect: Dialect,
    val standardEscapeStrings: Boolean
)

enum TransactionIsolation(val jdbcIsolation: Int):
    case None extends TransactionIsolation(Connection.TRANSACTION_NONE)
    case ReadUncommitted extends TransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED)
    case ReadCommitted extends TransactionIsolation(Connection.TRANSACTION_READ_COMMITTED)
    case RepeatableRead extends TransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ)
    case Serializable extends TransactionIsolation(Connection.TRANSACTION_SERIALIZABLE)

val transaction = Transaction

object Transaction extends Dynamic:
    def execute(insert: Insert[?, ?])(using t: JdbcTransactionContext, l: Logger): Int =
        val sql = statementToString(insert.tree, t.dialect, t.standardEscapeStrings)
        l(sql, Array.empty[Any])
        jdbcExec(t.connection, sql, Array.empty[Any])

    def execute(update: Update[?, ?])(using t: JdbcTransactionContext, l: Logger): Int =
        val sql = statementToString(update.tree, t.dialect, t.standardEscapeStrings)
        l(sql, Array.empty[Any])
        jdbcExec(t.connection, sql, Array.empty[Any])
        
    def execute(delete: Delete[?])(using t: JdbcTransactionContext, l: Logger): Int =
        val sql = statementToString(delete.tree, t.dialect, t.standardEscapeStrings)
        l(sql, Array.empty[Any])
        jdbcExec(t.connection, sql, Array.empty[Any])

    def execute(nativeSql: NativeSql)(using t: JdbcTransactionContext, l: Logger): Int =
        val NativeSql(sql, args) = nativeSql
        l(sql, args)
        jdbcExec(t.connection, sql, args)

    inline def insert[A <: Product](entity: A)(using 
        m: Mirror.ProductOf[A],
        t: JdbcTransactionContext, 
        l: Logger
    ): Int =
        val i = Insert.insertByEntities[A](entity :: Nil)
        val sql = statementToString(i.tree, t.dialect, t.standardEscapeStrings)
        jdbcExec(t.connection, sql, Array.empty[Any])

    inline def insertBatch[A <: Product](entities: Seq[A])(using 
        m: Mirror.ProductOf[A],
        t: JdbcTransactionContext, 
        l: Logger
    ): Int =
        val i = Insert.insertByEntities[A](entities)
        val sql = statementToString(i.tree, t.dialect, t.standardEscapeStrings)
        jdbcExec(t.connection, sql, Array.empty[Any])

    inline def insertAndReturn[A <: Product](entity: A)(using 
        m: Mirror.ProductOf[A],
        t: JdbcTransactionContext, 
        l: Logger
    ): A =
        val i = Insert.insertByEntities[A](entity :: Nil)
        val sql = statementToString(i.tree, t.dialect, t.standardEscapeStrings)
        val id = jdbcExecReturnKey(t.connection, sql, Array.empty[Any]).head
        InsertMacro.bindGeneratedPrimaryKey[A](id, entity)

    inline def update[A <: Product](
        entity: A, 
        skipNone: Boolean = false
    )(using 
        m: Mirror.ProductOf[A],
        t: JdbcTransactionContext, 
        l: Logger
    ): Int =
        val u = Update.updateByEntity[A](entity, skipNone)
        if u.tree.setList.isEmpty then
            0
        else
            val sql = statementToString(u.tree, t.dialect, t.standardEscapeStrings)
            jdbcExec(t.connection, sql, Array.empty[Any])

    inline def save[A <: Product](
        entity: A
    )(using 
        m: Mirror.ProductOf[A],
        t: JdbcTransactionContext, 
        l: Logger
    ): Int =
        val s = Save.saveByEntity[A](entity)
        val sql = statementToString(s.tree, t.dialect, t.standardEscapeStrings)
        jdbcExec(t.connection, sql, Array.empty[Any])

    inline def fetchByPrimaryKeys[T](using 
        fp: FetchPk[T],
        d: JdbcDecoder[T], 
        t: JdbcTransactionContext, 
        l: Logger
    )(pks: Seq[fp.Args]): List[T] =
        val tree = fp.createTree(pks)
        val sql = queryToString(tree, t.dialect, t.standardEscapeStrings)
        l(sql, Array.empty[Any])
        jdbcQuery(t.connection, sql, Array.empty[Any])

    inline def findByPrimaryKey[T](using 
        fp: FetchPk[T],
        d: JdbcDecoder[T], 
        t: JdbcTransactionContext, 
        l: Logger
    )(pk: fp.Args): Option[T] =
        val tree = fp.createTree(pk :: Nil)
        val sql = queryToString(tree, t.dialect, t.standardEscapeStrings)
        l(sql, Array.empty[Any])
        jdbcQuery(t.connection, sql, Array.empty[Any]).headOption

    inline def fetchTo[T](inline query: Query[?])(using 
        d: JdbcDecoder[T], 
        t: JdbcTransactionContext, 
        l: Logger
    ): List[T] =
        val sql = queryToString(query.tree, t.dialect, t.standardEscapeStrings)
        l(sql, Array.empty[Any])
        jdbcQuery(t.connection, sql, Array.empty[Any])
        
    inline def fetch[T](inline query: Query[T])(using 
        r: Result[T], 
        d: JdbcDecoder[r.R], 
        c: JdbcTransactionContext, 
        l: Logger
    ): List[r.R] =
        fetchTo[r.R](query)

    def fetchTo[T](nativeSql: NativeSql)(using 
        d: JdbcDecoder[T], 
        t: JdbcTransactionContext, 
        l: Logger
    ): List[T] =
        val NativeSql(sql, args) = nativeSql
        l(sql, args)
        jdbcQuery(t.connection, sql, args)

    def fetchToMap(nativeSql: NativeSql)(using 
        t: JdbcTransactionContext, 
        l: Logger
    ): List[Map[String, Any]] =
        val NativeSql(sql, args) = nativeSql
        l(sql, args)
        jdbcQueryToMap(t.connection, sql, args)

    def fetchTo[T](nativeSql: (String, Array[Any]))(using 
        d: JdbcDecoder[T], 
        t: JdbcTransactionContext, 
        l: Logger
    ): List[T] =
        val (sql, args) = nativeSql
        l(sql, args)
        jdbcQuery(t.connection, sql, args)

    def fetchToMap(nativeSql: (String, Array[Any]))(using 
        t: JdbcTransactionContext, 
        l: Logger
    ): List[Map[String, Any]] =
        val (sql, args) = nativeSql
        l(sql, args)
        jdbcQueryToMap(t.connection, sql, args)

    def fetchTo[T](query: DynamicQuery)(using 
        d: JdbcDecoder[T], 
        t: JdbcTransactionContext, 
        l: Logger
    ): List[T] =
        val sql = queryToString(query.tree, t.dialect, t.standardEscapeStrings)
        l(sql, Array.empty[Any])
        jdbcQuery(t.connection, sql, Array.empty[Any])

    def fetchToMap(query: DynamicQuery)(using 
        t: JdbcTransactionContext, 
        l: Logger
    ): List[Map[String, Any]] =
        val sql = queryToString(query.tree, t.dialect, t.standardEscapeStrings)
        l(sql, Array.empty[Any])
        jdbcQueryToMap(t.connection, sql, Array.empty[Any])

    inline def findTo[T](inline query: Query[?])(using 
        JdbcDecoder[T], 
        JdbcTransactionContext, 
        Logger
    ): Option[T] =
        fetchTo[T](query.take(1)).headOption

    inline def find[T](inline query: Query[T])(using 
        r: Result[T], 
        d: JdbcDecoder[r.R], 
        c: JdbcTransactionContext, 
        l: Logger
    ): Option[r.R] =
        findTo[r.R](query)

    inline def pageTo[T](
        inline query: Query[?], pageSize: Int, pageNo: Int, returnCount: Boolean = true
    )(using 
        JdbcDecoder[T], 
        JdbcTransactionContext, 
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
        c: JdbcTransactionContext, 
        l: Logger
    ): Page[r.R] =
        pageTo[r.R](query, pageSize, pageNo, returnCount)

    inline def fetchCount[T](inline query: Query[T])(using 
        JdbcTransactionContext, 
        Logger
    ): Long =
        val sizeQuery = query.size
        fetch(sizeQuery).head

    inline def fetchExists[T](inline query: Query[T])(using 
        JdbcTransactionContext, 
        Logger
    ): Boolean =
        val existsQuery = query.exists
        fetch(existsQuery).head

    inline def applyDynamic[T](name: String)(using
        r: Repository[T, name.type],
        d: JdbcDecoder[T],
        c: JdbcTransactionContext,
        l: Logger
    )(args: r.Args): r.R =
        r.createQuery(
            c.dialect, 
            c.standardEscapeStrings, 
            args, 
            q => fetchTo[T](q),
            q => findTo[T](q),
            (q, ps, pn, rc) => pageTo[T](q, ps, pn, rc),
            q => fetchCount(q),
            q => fetchExists(q)
        )