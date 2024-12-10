package sqala.jdbc

import sqala.static.macros.*
import sqala.static.statement.dml.*
import sqala.static.statement.query.*
import sqala.printer.Dialect
import sqala.util.{queryToString, statementToString}

import java.sql.Connection
import scala.deriving.Mirror

class JdbcTransactionContext(val connection: Connection, val dialect: Dialect)

def execute(insert: Insert[?, ?])(using t: JdbcTransactionContext, l: Logger): Int =
    val (sql, args) = statementToString(insert.ast, t.dialect, true)
    l(sql, args)
    jdbcExec(t.connection, sql, args)

def execute(update: Update[?, ?])(using t: JdbcTransactionContext, l: Logger): Int =
    val (sql, args) = statementToString(update.ast, t.dialect, true)
    l(sql, args)
    jdbcExec(t.connection, sql, args)
    
def execute(delete: Delete[?])(using t: JdbcTransactionContext, l: Logger): Int =
    val (sql, args) = statementToString(delete.ast, t.dialect, true)
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
    val (sql, args) = statementToString(i.ast, t.dialect, true)
    jdbcExec(t.connection, sql, args)

inline def insertBatch[A <: Product](entities: List[A])(using 
    m: Mirror.ProductOf[A],
    t: JdbcTransactionContext, 
    l: Logger
): Int =
    val i = sqala.static.dsl.insert[A](entities)
    val (sql, args) = statementToString(i.ast, t.dialect, true)
    jdbcExec(t.connection, sql, args)

inline def insertAndReturn[A <: Product](entity: A)(using 
    m: Mirror.ProductOf[A],
    t: JdbcTransactionContext, 
    l: Logger
): A =
    val i = sqala.static.dsl.insert[A](entity)
    val (sql, args) = statementToString(i.ast, t.dialect, true)
    val id = jdbcExecReturnKey(t.connection, sql, args).head
    ClauseMacro.bindGeneratedPrimaryKey[A](id, entity)

inline def update[A <: Product](
    entity: A, 
    skipNone: Boolean = false
)(using 
    m: Mirror.ProductOf[A],
    t: JdbcTransactionContext, 
    l: Logger
): Int =
    val u = sqala.static.dsl.update(entity, skipNone)
    if u.ast.setList.isEmpty then
        0
    else
        val (sql, args) = statementToString(u.ast, t.dialect, true)
        jdbcExec(t.connection, sql, args)

inline def save[A <: Product](
    entity: A
)(using 
    m: Mirror.ProductOf[A],
    t: JdbcTransactionContext, 
    l: Logger
): Int =
    val s = sqala.static.dsl.save(entity)
    val (sql, args) = statementToString(s.ast, t.dialect, true)
    jdbcExec(t.connection, sql, args)

def fetchTo[T](query: Query[?, ?])(using d: JdbcDecoder[T], t: JdbcTransactionContext, l: Logger): List[T] =
    val (sql, args) = queryToString(query.ast, t.dialect, true)
    l(sql, args)
    jdbcQuery(t.connection, sql, args)
    
def fetch[T](query: Query[T, ?])(using r: Result[T], d: JdbcDecoder[r.R], c: JdbcTransactionContext, l: Logger): List[r.R] =
    fetchTo[r.R](query)

def fetchTo[T](query: WithRecursive[?])(using d: JdbcDecoder[T], t: JdbcTransactionContext, l: Logger): List[T] =
    val (sql, args) = queryToString(query.ast, t.dialect, true)
    l(sql, args)
    jdbcQuery(t.connection, sql, args)
    
def fetch[T](query: WithRecursive[T])(using r: Result[T], d: JdbcDecoder[r.R], c: JdbcTransactionContext, l: Logger): List[r.R] =
    fetchTo[r.R](query)

def fetch[T <: Record](nativeSql: StaticNativeSql[T])(using t: JdbcTransactionContext, l: Logger): List[T] =
    l(nativeSql.sql, nativeSql.args)
    jdbcQueryToMap(t.connection, nativeSql.sql, nativeSql.args).map(Record(_).asInstanceOf[T])

def fetchTo[T](nativeSql: NativeSql)(using d: JdbcDecoder[T], t: JdbcTransactionContext, l: Logger): List[T] =
    val NativeSql(sql, args) = nativeSql
    l(sql, args)
    jdbcQuery(t.connection, sql, args)

def fetchToMap[T](nativeSql: NativeSql)(using d: JdbcDecoder[T], t: JdbcTransactionContext, l: Logger): List[Map[String, Any]] =
    val NativeSql(sql, args) = nativeSql
    l(sql, args)
    jdbcQueryToMap(t.connection, sql, args)

def fetchTo[T](nativeSql: (String, Array[Any]))(using d: JdbcDecoder[T], t: JdbcTransactionContext, l: Logger): List[T] =
    val (sql, args) = nativeSql
    l(sql, args)
    jdbcQuery(t.connection, sql, args)

def fetchToMap[T](nativeSql: (String, Array[Any]))(using d: JdbcDecoder[T], t: JdbcTransactionContext, l: Logger): List[Map[String, Any]] =
    val (sql, args) = nativeSql
    l(sql, args)
    jdbcQueryToMap(t.connection, sql, args)

def findTo[T](query: Query[?, ?])(using JdbcDecoder[T], JdbcTransactionContext, Logger): Option[T] =
    fetchTo[T](query.take(1)).headOption

def find[T](query: Query[T, ?])(using r: Result[T], d: JdbcDecoder[r.R], c: JdbcTransactionContext, l: Logger): Option[r.R] =
    findTo[r.R](query)

def pageTo[T](
    query: Query[?, ?], pageSize: Int, pageNo: Int, returnCount: Boolean = true
)(using JdbcDecoder[T], JdbcTransactionContext, Logger): Page[T] =
    val data = if pageSize == 0 then Nil
        else fetchTo[T](query.drop(if pageNo <= 1 then 0 else pageSize * (pageNo - 1)).take(pageSize))
    val count = if returnCount then fetch(query.size).head else 0L
    val total = if count == 0 || pageSize == 0 then 0
        else if count % pageSize == 0 then (count / pageSize).toInt
        else (count / pageSize + 1).toInt
    Page(total, count, pageNo, pageSize, data)

def page[T](
    query: Query[T, ?], pageSize: Int, pageNo: Int, returnCount: Boolean = true
)(using r: Result[T], d: JdbcDecoder[r.R], c: JdbcTransactionContext, l: Logger): Page[r.R] =
    pageTo[r.R](query, pageSize, pageNo, returnCount)

def fetchSize[T](query: Query[T, ?])(using JdbcTransactionContext, Logger): Long =
    val sizeQuery = query.size
    fetch(sizeQuery).head

def fetchExists[T](query: Query[T, ?])(using JdbcTransactionContext, Logger): Boolean =
    val existsQuery = query.exists
    fetch(existsQuery).head

def showSql[T](query: Query[T, ?])(using t: JdbcTransactionContext): String =
    queryToString(query.ast, t.dialect, true)._1