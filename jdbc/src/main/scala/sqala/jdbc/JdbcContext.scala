package sqala.jdbc

import sqala.ast.statement.{SqlQuery, SqlStatement}
import sqala.dynamic.native.NativeSql
import sqala.ir.{IrQuery, IrStatement, ToAst}
import sqala.jdbc.dialect.JdbcDialect
import sqala.metadata.{FetchPrimaryKey, InsertMacro}
import sqala.static.dsl.statement.dml.{Delete, Insert, Save, Update}
import sqala.static.dsl.statement.query.{Query, SelectQuery}
import sqala.static.dsl.{QuerySize, Result}

import java.sql.Connection
import javax.sql.DataSource
import scala.deriving.Mirror
import scala.util.NotGiven

/**
 * JDBC execution context wrapping a `DataSource` and `Dialect`.
 * Provides typed methods for executing DML statements, fetching
 * query results, pagination, transaction control.
 */
final class JdbcContext(
    private[sqala] val dataSource: DataSource,
    private[sqala] val dialect: JdbcDialect,
    private[sqala] val standardEscapeStrings: Boolean
):
    /**
     * Generates the SQL string and bound parameters for a query AST using
     * the configured dialect and string escaping mode.
     *
     * The returned parameter array matches the `?` placeholders in the SQL
     * string, in order.
     */
    private def renderQuery(query: SqlQuery): (String, Array[Any]) =
        val printer = dialect.printer(standardEscapeStrings)
        printer.printQuery(query)
        printer.sql -> printer.args

    /**
     * Lowers an IR query to the SQL AST, then generates its SQL string and
     * bound parameters.
     */
    private def renderQuery(query: IrQuery): (String, Array[Any]) =
        renderQuery(new ToAst().toAst(query))

    /**
     * Generates the SQL string and bound parameters for a DML statement AST
     * using the configured dialect and string escaping mode.
     *
     * The returned parameter array matches the `?` placeholders in the SQL
     * string, in order.
     */
    private def renderStatement(statement: SqlStatement): (String, Array[Any]) =
        val printer = dialect.printer(standardEscapeStrings)
        printer.printStatement(statement)
        printer.sql -> printer.args

    /**
     * Lowers an IR statement to the SQL AST, then generates its SQL string
     * and bound parameters.
     */
    private def renderStatement(statement: IrStatement): (String, Array[Any]) =
        renderStatement(new ToAst().toAst(statement))

    /**
     * Obtains a connection and executes a handler. Automatically
     * closes the connection outside of transactions.
     */
    private def execute[T](handler: Connection => T)(using ec: ExecuteContext): T =
        var conn: Connection | Null = null
        try
            conn = ec.fetchConnection(this)
            val result = handler(conn)
            result
        finally
            if !ec.inTransaction && conn != null then
                conn.close()

    /**
     * Logs and executes a DML statement, returning the affected
     * row count.
     */
    private def executeDml(sql: String, args: Array[Any])(using ec: ExecuteContext, l: Logger): Int =
        l.printLog(sql, args)
        execute(c => jdbcExec(c, sql, args))

    /**
     * Executes a static `insert` statement and returns the affected
     * row count.
     *
     * {{{
     * db.execute(insert[User](u => (u.id, u.name)).values((1, "Alice")))
     * }}}
     */
    def execute(insert: Insert[?, ?])(using ExecuteContext, Logger): Int =
        val (sql, args) = renderStatement(insert.toSqlStatement)
        executeDml(sql, args)

    /**
     * Executes a static `update` statement and returns the affected
     * row count.
     *
     * {{{
     * db.execute(update[User].set(u => u.name := "Bob").where(u => u.id == 1))
     * }}}
     */
    def execute(update: Update[?, ?])(using ExecuteContext, Logger): Int =
        val (sql, args) = renderStatement(update.toSqlStatement)
        executeDml(sql, args)

    /**
     * Executes a static `delete` statement and returns the affected
     * row count.
     *
     * {{{
     * db.execute(delete[User].where(u => u.id == 1))
     * }}}
     */
    def execute(delete: Delete[?])(using ExecuteContext, Logger): Int =
        val (sql, args) = renderStatement(delete.tree)
        executeDml(sql, args)

    /**
     * Executes a native SQL DML statement and returns the affected
     * row count.
     *
     * {{{
     * db.execute(sql"DELETE FROM user WHERE id = $id")
     * }}}
     */
    def execute(nativeSql: NativeSql)(using ExecuteContext, Logger): Int =
        executeDml(nativeSql.sql, nativeSql.args)

    /**
     * Inserts a single entity and returns the affected row count.
     * Auto-increment columns are automatically excluded.
     *
     * {{{
     * db.insert(User(1, "Alice"))
     * }}}
     */
    inline def insert[A <: Product](entity: A)(using
        ExecuteContext,
        Mirror.ProductOf[A],
        Logger
    ): Int =
        val i = Insert.insertByEntities[A](entity :: Nil)
        val (sql, args) = renderStatement(i.toSqlStatement)
        executeDml(sql, args)

    /**
     * Batch-inserts a sequence of entities and returns the total
     * affected row count.
     *
     * {{{
     * db.insertBatch(List(User(1, "Alice"), User(2, "Bob")))
     * }}}
     */
    inline def insertBatch[A <: Product](entities: Seq[A])(using
        ExecuteContext,
        Mirror.ProductOf[A],
        Logger
    ): Int =
        val i = Insert.insertByEntities[A](entities)
        val (sql, args) = renderStatement(i.toSqlStatement)
        executeDml(sql, args)

    /**
     * Inserts a single entity and returns the entity with
     * auto-generated primary key fields populated.
     *
     * {{{
     * db.insertAndReturn(User(0, "Alice"))
     * }}}
     */
    inline def insertAndReturn[A <: Product](entity: A)(using
        ec: ExecuteContext,
        m: Mirror.ProductOf[A],
        l: Logger
    ): A =
        val i = Insert.insertByEntities[A](entity :: Nil)
        val (sql, args) = renderStatement(i.toSqlStatement)
        l.printLog(sql, args)
        val id = execute(c => jdbcExecReturnKey(c, sql, args)).head
        InsertMacro.bindGeneratedPrimaryKey[A](id, entity)

    /**
     * Updates an entity by primary key. Primary key fields form
     * the `WHERE` clause; remaining fields become `SET` assignments.
     * When `skipNone` is `true`, fields with `None` values are
     * omitted from the update.
     *
     * {{{
     * db.update(user, skipNone = true)
     * }}}
     */
    inline def update[A <: Product](
        entity: A,
        skipNone: Boolean = false
    )(using
        ExecuteContext,
        Mirror.ProductOf[A],
        Logger
    ): Int =
        val u = Update.updateByEntity[A](entity, skipNone)
        if u.tree.setPairs.isEmpty then
            0
        else
            val (sql, args) = renderStatement(u.toSqlStatement)
            executeDml(sql, args)

    /**
     * Upserts an entity by primary key. Inserts if the key does
     * not exist, updates otherwise.
     *
     * {{{
     * db.save(user)
     * }}}
     */
    inline def save[A <: Product](
        entity: A
    )(using
        ExecuteContext,
        Mirror.ProductOf[A],
        Logger
    ): Int =
        val s = Save.saveByEntity[A](entity)
        val (sql, args) = renderStatement(s.tree)
        executeDml(sql, args)

    /**
     * Deletes a single row by primary key.
     *
     * {{{
     * db.deleteByPrimaryKey[User](1)
     * }}}
     */
    def deleteByPrimaryKey[T](using
        ec: ExecuteContext,
        fp: FetchPrimaryKey[T],
        l: Logger
    )(primaryKey: fp.Args): Int =
        val tree = fp.createDeleteTree(primaryKey :: Nil, false)
        val (sql, args) = renderStatement(tree)
        executeDml(sql, args)

    /**
     * Deletes multiple rows by primary keys.
     *
     * {{{
     * db.deleteByPrimaryKeys[User](List(1, 2, 3))
     * }}}
     */
    def deleteByPrimaryKeys[T](using
        ec: ExecuteContext,
        fp: FetchPrimaryKey[T],
        l: Logger
    )(primaryKeys: Seq[fp.Args]): Int =
        val tree = fp.createDeleteTree(primaryKeys, false)
        val (sql, args) = renderStatement(tree)
        executeDml(sql, args)

    /**
     * Finds a single row by primary key.
     *
     * {{{
     * db.findByPrimaryKey[User](1)
     * }}}
     */
    def findByPrimaryKey[T](using
        ec: ExecuteContext,
        fp: FetchPrimaryKey[T],
        d: JdbcDecoder[T],
        l: Logger
    )(primaryKey: fp.Args): Option[T] =
        val tree = fp.createQueryTree(primaryKey :: Nil, false)
        val (sql, args) = renderQuery(tree)
        l.printLog(sql, args)
        execute(c => jdbcQuery(c, sql, args)).headOption

    /**
     * Fetches multiple rows by primary keys.
     *
     * {{{
     * db.fetchByPrimaryKeys[User](List(1, 2, 3))
     * }}}
     */
    def fetchByPrimaryKeys[T](using
        ec: ExecuteContext,
        fp: FetchPrimaryKey[T],
        d: JdbcDecoder[T],
        l: Logger
    )(primaryKeys: Seq[fp.Args]): List[T] =
        val tree = fp.createQueryTree(primaryKeys, false)
        val (sql, args) = renderQuery(tree)
        l.printLog(sql, args)
        execute(c => jdbcQuery(c, sql, args))

    /**
     * Fetches a typed query result using an explicit decoder.
     *
     * {{{
     * db.fetchTo[User](from(User))
     * }}}
     */
    def fetchTo[T](query: Query[?, ?, ?, ?])(using
        ec: ExecuteContext,
        d: JdbcDecoder[T],
        l: Logger
    ): List[T] =
        val (sql, args) = renderQuery(query.tree)
        l.printLog(sql, args)
        execute(c => jdbcQuery(c, sql, args))

    /**
     * Fetches a static query result with automatic result type
     * derivation. The return type is derived from the query
     * projection at compile time.
     *
     * {{{
     * db.fetch(from(User).map(u => (name = u.name)))
     * }}}
     */
    def fetch[T, OKS <: Tuple, L <: Int, S <: QuerySize](query: Query[T, OKS, L, S])(using
        ec: ExecuteContext,
        r: Result[T],
        d: JdbcDecoder[r.R],
        l: Logger
    ): List[r.R] =
        fetchTo[r.R](query)

    /**
     * Fetches a native SQL query result using an explicit decoder.
     *
     * {{{
     * db.fetchTo[User](sql"SELECT * FROM user WHERE id = $id")
     * }}}
     */
    def fetchTo[T](nativeSql: NativeSql)(using
        ec: ExecuteContext,
        d: JdbcDecoder[T],
        l: Logger
    ): List[T] =
        l.printLog(nativeSql.sql, nativeSql.args)
        execute(c => jdbcQuery(c, nativeSql.sql, nativeSql.args))

    /**
     * Fetches a native SQL query result as `List[Map[String, Any]]`.
     *
     * {{{
     * db.fetchToMap(sql"SELECT * FROM user")
     * }}}
     */
    def fetchToMap(nativeSql: NativeSql)(using ec: ExecuteContext, l: Logger): List[Map[String, Any]] =
        l.printLog(nativeSql.sql, nativeSql.args)
        execute(c => jdbcQueryToMap(c, nativeSql.sql, nativeSql.args))

    /**
     * Fetches a raw SQL tuple result using an explicit decoder.
     *
     * {{{
     * db.fetchTo[User]("SELECT * FROM user", Array.empty[Any])
     * }}}
     */
    def fetchTo[T](nativeSql: (String, Array[Any]))(using
        ec: ExecuteContext,
        d: JdbcDecoder[T],
        l: Logger
    ): List[T] =
        val (sql, args) = nativeSql
        l.printLog(sql, args)
        execute(c => jdbcQuery(c, sql, args))

    /**
     * Fetches a raw SQL tuple result as `List[Map[String, Any]]`.
     *
     * {{{
     * db.fetchToMap[User]("SELECT * FROM user", Array.empty[Any])
     * }}}
     */
    def fetchToMap(nativeSql: (String, Array[Any]))(using ec: ExecuteContext, l: Logger): List[Map[String, Any]] =
        val (sql, args) = nativeSql
        l.printLog(sql, args)
        execute(c => jdbcQueryToMap(c, sql, args))

    /**
     * Paginates a query result using an explicit decoder.
     *
     * {{{
     * db.pageTo[User](from(User), pageSize = 10, pageNo = 1)
     * }}}
     */
    def pageTo[T](
        query: SelectQuery[?, ?, ?, ?],
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

    /**
     * Paginates a static query result with automatic result type
     * derivation.
     *
     * {{{
     * db.page(from(User).map(u => (name = u.name)), pageSize = 10, pageNo = 1)
     * }}}
     */
    def page[T, OKS <: Tuple, L <: Int, S <: QuerySize](
        query: SelectQuery[T, OKS, L, S],
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

    /**
     * Finds the first row from a query using an explicit decoder.
     *
     * {{{
     * db.findTo[User](from(User).filter(u => u.id == 1))
     * }}}
     */
    def findTo[T](query: SelectQuery[?, ?, ?, ?])(using
        ec: ExecuteContext,
        d: JdbcDecoder[T],
        l: Logger
    ): Option[T] =
        fetchTo[T](query.take(1)).headOption

    /**
     * Finds the first row from a static query with automatic
     * result type derivation.
     *
     * {{{
     * db.find(from(User).filter(u => u.id == 1).map(u => u.name))
     * }}}
     */
    def find[T, OKS <: Tuple, L <: Int, S <: QuerySize](query: SelectQuery[T, OKS, L, S])(using
        ec: ExecuteContext,
        r: Result[T],
        d: JdbcDecoder[r.R],
        l: Logger
    ): Option[r.R] =
        findTo[r.R](query)

    /**
     * Returns the total row count of a query.
     *
     * {{{
     * db.fetchCount(from(User))
     * }}}
     */
    def fetchCount[T, OKS <: Tuple, L <: Int, S <: QuerySize](query: Query[T, OKS, L, S])(using ExecuteContext, Logger): Long =
        val sizeQuery = query.size
        fetch(sizeQuery).head

    /**
     * Tests whether a query returns at least one row.
     *
     * {{{
     * db.fetchExists(from(User).filter(u => u.id == 1))
     * }}}
     */
    def fetchExists[T, OKS <: Tuple, L <: Int, S <: QuerySize](query: Query[T, OKS, L, S])(using ExecuteContext, Logger): Boolean =
        val existsQuery = query.exists
        fetch(existsQuery).head

    /**
     * Generates the SQL string for a query without executing it.
     * Useful for debugging.
     *
     * {{{
     * db.showSql(from(User))
     * }}}
     */
    def showSql[T, OKS <: Tuple, L <: Int, S <: QuerySize](query: Query[T, OKS, L, S]): (String, Array[Any]) =
        renderQuery(query.tree)

    /**
     * Streams a query result in cursor batches, invoking the callback
     * for each batch. Useful for processing large result sets without
     * loading all rows into memory.
     *
     * {{{
     * db.cursorFetch(from(User), fetchSize = 100)(cursor =>
     *     cursor.data.foreach(println)
     * )
     * }}}
     */
    def cursorFetch[T, OKS <: Tuple, L <: Int, S <: QuerySize, R](
        query: Query[T, OKS, L, S],
        fetchSize: Int
    )(using
        ec: ExecuteContext,
        r: Result[T],
        d: JdbcDecoder[r.R],
        l: Logger
    )(f: Cursor[r.R] => R): Unit =
        val (sql, args) = renderQuery(query.tree)
        l.printLog(sql, args)
        execute(c => jdbcCursorQuery(c, sql, args, fetchSize, f))

    /**
     * Executes a block of operations within a database transaction.
     * If any exception is thrown, the transaction is rolled back;
     * otherwise it is committed.
     *
     * {{{
     * db.executeTransaction {
     *     db.insert(User(1, "Alice"))
     *     db.insert(User(2, "Bob"))
     * }
     * }}}
     */
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

    /**
     * Executes a block of operations within a database transaction
     * at the specified isolation level.
     *
     * {{{
     * db.executeTransactionWithIsolation(TransactionIsolation.Serializable) {
     *     db.insert(User(1, "Alice"))
     *     db.insert(User(2, "Bob"))
     * }
     * }}}
     */
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

/**
 * Transaction context providing the underlying JDBC connection
 * to `ExecuteContext` during a transaction block.
 */
final class JdbcTransactionContext(
    private[sqala] val connection: Connection
)

/**
 * JDBC transaction isolation levels, maps to
 * `java.sql.Connection` constants.
 */
enum TransactionIsolation(private[sqala] val jdbcIsolation: Int):
    case None extends TransactionIsolation(Connection.TRANSACTION_NONE)
    case ReadUncommitted extends TransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED)
    case ReadCommitted extends TransactionIsolation(Connection.TRANSACTION_READ_COMMITTED)
    case RepeatableRead extends TransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ)
    case Serializable extends TransactionIsolation(Connection.TRANSACTION_SERIALIZABLE)

/**
 * Context for obtaining JDBC connections, resolved automatically
 * depending on whether the current scope is inside a transaction
 * or not.
 */
trait ExecuteContext:
    /**
     * Returns `true` if the current scope is inside a transaction,
     * so the connection is reused rather than closed after each
     * operation.
     */
    def inTransaction: Boolean

    /**
     * Obtains a JDBC connection. Inside a transaction this returns
     * the shared transaction connection; outside a transaction it
     * obtains a new connection from the data source.
     */
    def fetchConnection(context: JdbcContext): Connection

object ExecuteContext:
    given withTransaction(using c: JdbcTransactionContext): ExecuteContext with
        def inTransaction: Boolean =
            true

        def fetchConnection(context: JdbcContext): Connection =
            c.connection

    given withoutTransaction(using NotGiven[JdbcTransactionContext]): ExecuteContext with
        def inTransaction: Boolean =
            false

        def fetchConnection(context: JdbcContext): Connection =
            context.dataSource.getConnection()