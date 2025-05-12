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

class JdbcTransactionContext[D <: Dialect](val connection: Connection, val dialect: D)

def execute[D <: Dialect](insert: Insert[?, ?])(using t: JdbcTransactionContext[D], l: Logger): Int =
    val (sql, args) = statementToString(insert.tree, t.dialect, true)
    l(sql, args)
    jdbcExec(t.connection, sql, args)

def execute[D <: Dialect](update: Update[?, ?])(using t: JdbcTransactionContext[D], l: Logger): Int =
    val (sql, args) = statementToString(update.tree, t.dialect, true)
    l(sql, args)
    jdbcExec(t.connection, sql, args)
    
def execute[D <: Dialect](delete: Delete[?])(using t: JdbcTransactionContext[D], l: Logger): Int =
    val (sql, args) = statementToString(delete.tree, t.dialect, true)
    l(sql, args)
    jdbcExec(t.connection, sql, args)

def execute[D <: Dialect](nativeSql: NativeSql)(using t: JdbcTransactionContext[D], l: Logger): Int =
    val NativeSql(sql, args) = nativeSql
    l(sql, args)
    jdbcExec(t.connection, sql, args)

inline def insert[A <: Product, D <: Dialect](entity: A)(using 
    m: Mirror.ProductOf[A],
    t: JdbcTransactionContext[D], 
    l: Logger
): Int =
    val i = sqala.static.dsl.insert[A](entity)
    val (sql, args) = statementToString(i.tree, t.dialect, true)
    jdbcExec(t.connection, sql, args)

inline def insertBatch[A <: Product, D <: Dialect](entities: List[A])(using 
    m: Mirror.ProductOf[A],
    t: JdbcTransactionContext[D], 
    l: Logger
): Int =
    val i = sqala.static.dsl.insert[A](entities)
    val (sql, args) = statementToString(i.tree, t.dialect, true)
    jdbcExec(t.connection, sql, args)

inline def insertAndReturn[A <: Product, D <: Dialect](entity: A)(using 
    m: Mirror.ProductOf[A],
    t: JdbcTransactionContext[D], 
    l: Logger
): A =
    val i = sqala.static.dsl.insert[A](entity)
    val (sql, args) = statementToString(i.tree, t.dialect, true)
    val id = jdbcExecReturnKey(t.connection, sql, args).head
    InsertMacro.bindGeneratedPrimaryKey[A](id, entity)

inline def update[A <: Product, D <: Dialect](
    entity: A, 
    skipNone: Boolean = false
)(using 
    m: Mirror.ProductOf[A],
    t: JdbcTransactionContext[D], 
    l: Logger
): Int =
    val u = sqala.static.dsl.update(entity, skipNone)
    if u.tree.setList.isEmpty then
        0
    else
        val (sql, args) = statementToString(u.tree, t.dialect, true)
        jdbcExec(t.connection, sql, args)

inline def save[A <: Product, D <: Dialect](
    entity: A
)(using 
    m: Mirror.ProductOf[A],
    t: JdbcTransactionContext[D], 
    l: Logger
): Int =
    val s = sqala.static.dsl.save(entity)
    val (sql, args) = statementToString(s.tree, t.dialect, true)
    jdbcExec(t.connection, sql, args)

inline def fetchTo[T, D <: Dialect](inline query: Query[?])(using 
    d: JdbcDecoder[T], 
    t: JdbcTransactionContext[D], 
    l: Logger
): List[T] =
    AnalysisMacro.showQuery(query)
    val (sql, args) = queryToString(query.tree, t.dialect, true)
    l(sql, args)
    jdbcQuery(t.connection, sql, args)
    
inline def fetch[T, D <: Dialect](inline query: Query[T])(using 
    r: Result[T], 
    d: JdbcDecoder[r.R], 
    c: JdbcTransactionContext[D], 
    l: Logger
): List[r.R] =
    fetchTo[r.R, D](query)

def fetchTo[T, D <: Dialect](nativeSql: NativeSql)(using 
    d: JdbcDecoder[T], 
    t: JdbcTransactionContext[D], 
    l: Logger
): List[T] =
    val NativeSql(sql, args) = nativeSql
    l(sql, args)
    jdbcQuery(t.connection, sql, args)

def fetchToMap[D <: Dialect](nativeSql: NativeSql)(using 
    t: JdbcTransactionContext[D], 
    l: Logger
): List[Map[String, Any]] =
    val NativeSql(sql, args) = nativeSql
    l(sql, args)
    jdbcQueryToMap(t.connection, sql, args)

def fetchTo[T, D <: Dialect](nativeSql: (String, Array[Any]))(using 
    d: JdbcDecoder[T], 
    t: JdbcTransactionContext[D], 
    l: Logger
): List[T] =
    val (sql, args) = nativeSql
    l(sql, args)
    jdbcQuery(t.connection, sql, args)

def fetchToMap[D <: Dialect](nativeSql: (String, Array[Any]))(using 
    t: JdbcTransactionContext[D], 
    l: Logger
): List[Map[String, Any]] =
    val (sql, args) = nativeSql
    l(sql, args)
    jdbcQueryToMap(t.connection, sql, args)

def fetchTo[T, D <: Dialect](query: DynamicQuery)(using 
    d: JdbcDecoder[T], 
    t: JdbcTransactionContext[D], 
    l: Logger
): List[T] =
    val (sql, args) = queryToString(query.tree, t.dialect, true)
    l(sql, args)
    jdbcQuery(t.connection, sql, args)

def fetchToMap[D <: Dialect](query: DynamicQuery)(using 
    t: JdbcTransactionContext[D], 
    l: Logger
): List[Map[String, Any]] =
    val (sql, args) = queryToString(query.tree, t.dialect, true)
    l(sql, args)
    jdbcQueryToMap(t.connection, sql, args)

inline def findTo[T, D <: Dialect](inline query: Query[?])(using 
    JdbcDecoder[T], 
    JdbcTransactionContext[D], 
    Logger
): Option[T] =
    AnalysisMacro.showLimitQuery(query)
    fetchTo[T, D](query.take(1)).headOption

inline def find[T, D <: Dialect](inline query: Query[T])(using 
    r: Result[T], 
    d: JdbcDecoder[r.R], 
    c: JdbcTransactionContext[D], 
    l: Logger
): Option[r.R] =
    findTo[r.R, D](query)

inline def pageTo[T, D <: Dialect](
    inline query: Query[?], pageSize: Int, pageNo: Int, returnCount: Boolean = true
)(using 
    JdbcDecoder[T], 
    JdbcTransactionContext[D], 
    Logger
): Page[T] =
    AnalysisMacro.showPageQuery(query)
    val data = if pageSize == 0 then Nil
        else fetchTo[T, D](query.drop(if pageNo <= 1 then 0 else pageSize * (pageNo - 1)).take(pageSize))
    val count = if returnCount then fetch(query.size).head else 0L
    val total = if count == 0 || pageSize == 0 then 0
        else if count % pageSize == 0 then (count / pageSize).toInt
        else (count / pageSize + 1).toInt
    Page(total, count, pageNo, pageSize, data)

inline def page[T, D <: Dialect](
    inline query: Query[T], pageSize: Int, pageNo: Int, returnCount: Boolean = true
)(using 
    r: Result[T], 
    d: JdbcDecoder[r.R], 
    c: JdbcTransactionContext[D], 
    l: Logger
): Page[r.R] =
    pageTo[r.R, D](query, pageSize, pageNo, returnCount)

inline def fetchSize[T, D <: Dialect](inline query: Query[T])(using 
    JdbcTransactionContext[D], 
    Logger
): Long =
    AnalysisMacro.showSizeQuery(query)
    val sizeQuery = query.size
    fetch(sizeQuery).head

inline def fetchExists[T, D <: Dialect](inline query: Query[T])(using 
    JdbcTransactionContext[D], 
    Logger
): Boolean =
    AnalysisMacro.showExistsQuery(query)
    val existsQuery = query.exists
    fetch(existsQuery).head