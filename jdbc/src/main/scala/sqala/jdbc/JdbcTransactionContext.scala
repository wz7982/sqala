package sqala.jdbc

import sqala.dynamic.dsl.{DynamicQuery, NativeSql}
import sqala.metadata.InsertMacro
import sqala.printer.Dialect
import sqala.static.dsl.Result
import sqala.static.dsl.statement.dml.{Delete, Insert, Update}
import sqala.static.dsl.statement.query.Query
import sqala.util.{queryToString, statementToString}

import java.sql.Connection
import scala.deriving.Mirror

class JdbcTransactionContext(val connection: Connection, val dialect: Dialect)

def execute(insert: Insert[?, ?])(using t: JdbcTransactionContext, l: Logger): Int =
    val (sql, args) = statementToString(insert.tree, t.dialect, true)
    l(sql, args)
    jdbcExec(t.connection, sql, args)

def execute(update: Update[?, ?])(using t: JdbcTransactionContext, l: Logger): Int =
    val (sql, args) = statementToString(update.tree, t.dialect, true)
    l(sql, args)
    jdbcExec(t.connection, sql, args)
    
def execute(delete: Delete[?])(using t: JdbcTransactionContext, l: Logger): Int =
    val (sql, args) = statementToString(delete.tree, t.dialect, true)
    l(sql, args)
    jdbcExec(t.connection, sql, args)

def execute(nativeSql: NativeSql)(using t: JdbcTransactionContext, l: Logger): Int =
    val NativeSql(sql, args) = nativeSql
    l(sql, args)
    jdbcExec(t.connection, sql, args)

inline def insert[A <: Product](entity: A)(using 
    m: Mirror.ProductOf[A],
    t: JdbcTransactionContext, 
    l: Logger
): Int =
    val i = sqala.static.dsl.insert[A](entity)
    val (sql, args) = statementToString(i.tree, t.dialect, true)
    jdbcExec(t.connection, sql, args)

inline def insertBatch[A <: Product](entities: List[A])(using 
    m: Mirror.ProductOf[A],
    t: JdbcTransactionContext, 
    l: Logger
): Int =
    val i = sqala.static.dsl.insert[A](entities)
    val (sql, args) = statementToString(i.tree, t.dialect, true)
    jdbcExec(t.connection, sql, args)

inline def insertAndReturn[A <: Product](entity: A)(using 
    m: Mirror.ProductOf[A],
    t: JdbcTransactionContext, 
    l: Logger
): A =
    val i = sqala.static.dsl.insert[A](entity)
    val (sql, args) = statementToString(i.tree, t.dialect, true)
    val id = jdbcExecReturnKey(t.connection, sql, args).head
    InsertMacro.bindGeneratedPrimaryKey[A](id, entity)

inline def update[A <: Product](
    entity: A, 
    skipNone: Boolean = false
)(using 
    m: Mirror.ProductOf[A],
    t: JdbcTransactionContext, 
    l: Logger
): Int =
    val u = sqala.static.dsl.update(entity, skipNone)
    if u.tree.setList.isEmpty then
        0
    else
        val (sql, args) = statementToString(u.tree, t.dialect, true)
        jdbcExec(t.connection, sql, args)

inline def save[A <: Product](
    entity: A
)(using 
    m: Mirror.ProductOf[A],
    t: JdbcTransactionContext, 
    l: Logger
): Int =
    val s = sqala.static.dsl.save(entity)
    val (sql, args) = statementToString(s.tree, t.dialect, true)
    jdbcExec(t.connection, sql, args)

inline def fetchTo[T](inline query: Query[?])(using 
    d: JdbcDecoder[T], 
    t: JdbcTransactionContext, 
    l: Logger
): List[T] =
    val (sql, args) = queryToString(query.tree, t.dialect, true)
    l(sql, args)
    jdbcQuery(t.connection, sql, args)
    
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
    val (sql, args) = queryToString(query.tree, t.dialect, true)
    l(sql, args)
    jdbcQuery(t.connection, sql, args)

def fetchToMap(query: DynamicQuery)(using 
    t: JdbcTransactionContext, 
    l: Logger
): List[Map[String, Any]] =
    val (sql, args) = queryToString(query.tree, t.dialect, true)
    l(sql, args)
    jdbcQueryToMap(t.connection, sql, args)

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
    Page(total, count, pageNo, pageSize, data)

inline def page[T](
    inline query: Query[T], pageSize: Int, pageNo: Int, returnCount: Boolean = true
)(using 
    r: Result[T], 
    d: JdbcDecoder[r.R], 
    c: JdbcTransactionContext, 
    l: Logger
): Page[r.R] =
    pageTo[r.R](query, pageSize, pageNo, returnCount)

inline def fetchSize[T](inline query: Query[T])(using 
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