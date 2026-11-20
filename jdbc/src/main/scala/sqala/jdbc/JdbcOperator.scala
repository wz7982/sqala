package sqala.jdbc

import java.sql.Types.*
import java.sql.{Connection, PreparedStatement, ResultSet, Statement}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.language.unsafeNulls

private def safeClose(closeable: AutoCloseable | Null): Unit =
    if closeable != null then
        try closeable.close()
        catch case _: Throwable => ()

private def bindParameters(stmt: PreparedStatement, args: Array[Any]): Unit =
    var i = 1
    while i <= args.length do
        val arg = args(i - 1)
        arg match
            case n: BigDecimal => stmt.setBigDecimal(i, n.bigDecimal)
            case n: Int => stmt.setInt(i, n)
            case n: Long => stmt.setLong(i, n)
            case n: Float => stmt.setFloat(i, n)
            case n: Double => stmt.setDouble(i, n)
            case b: Boolean => stmt.setBoolean(i, b)
            case s: String => stmt.setString(i, s)
            case _ => stmt.setObject(i, arg)
        i += 1

private def logSuccess(
    logger: Logger,
    operation: LogOperation,
    sql: String,
    args: Array[Any],
    start: Long,
    rowCount: Option[Int]
): Unit =
    val level = logger.logLevel
    if logger.enabled(level) then
        logger.log(
            LogEvent(
                level,
                operation,
                sql,
                args,
                System.nanoTime() - start,
                rowCount
            )
        )

private def logFailure(
    logger: Logger,
    operation: LogOperation,
    sql: String,
    args: Array[Any],
    start: Long,
    error: Throwable
): Unit =
    if logger.enabled(LogLevel.Error) then
        logger.log(
            LogEvent(
                LogLevel.Error,
                operation,
                sql,
                args,
                System.nanoTime() - start,
                None,
                Some(error)
            )
        )

/**
 * Executes a query and decodes the result set into a list of
 * typed values using the given `JdbcDecoder`. Parameters are
 * bound by their Scala type to the appropriate JDBC setter.
 */
private[sqala] def jdbcQuery[T](conn: Connection, sql: String, args: Array[Any])(using decoder: JdbcDecoder[T], l: Logger): List[T] =
    val start = System.nanoTime()
    var stmt: PreparedStatement = null
    var rs: ResultSet = null
    val result = ListBuffer[T]()
    try
        stmt = conn.prepareStatement(sql)
        bindParameters(stmt, args)
        rs = stmt.executeQuery()
        while rs.next() do
            result.addOne(decoder.decode(rs, 1))
        val rows = result.toList
        logSuccess(l, LogOperation.Query, sql, args, start, Some(rows.size))
        rows
    catch case e: Throwable =>
        logFailure(l, LogOperation.Query, sql, args, start, e)
        throw e
    finally
        safeClose(rs)
        safeClose(stmt)

/**
 * Streams a query result in cursor batches using a forward‑only
 * result set. Invokes the callback `f` for each batch of `size`
 * rows. Supports large result sets without loading all rows into
 * memory.
 */
private[sqala] def jdbcCursorQuery[T, R](
    conn: Connection,
    sql: String,
    args: Array[Any],
    size: Int,
    f: Cursor[T] => R
)(using decoder: JdbcDecoder[T], l: Logger): Unit =
    val start = System.nanoTime()
    var stmt: PreparedStatement = null
    var rs: ResultSet = null
    val result = ListBuffer[T]()
    var batchNo = 1
    var totalRows = 0
    try
        stmt = conn.prepareStatement(
            sql,
            ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_READ_ONLY
        )
        stmt.setFetchSize(size)
        bindParameters(stmt, args)
        rs = stmt.executeQuery()
        while rs.next() do
            result.addOne(decoder.decode(rs, 1))
            totalRows += 1
            if result.size == size then
                f(Cursor(batchNo, size, result.toList))
                result.clear()
                batchNo += 1
        if result.nonEmpty then
            f(Cursor(batchNo, size, result.toList))
        logSuccess(l, LogOperation.Cursor, sql, args, start, Some(totalRows))
    catch case e: Throwable =>
        logFailure(l, LogOperation.Cursor, sql, args, start, e)
        throw e
    finally
        safeClose(rs)
        safeClose(stmt)

/**
 * Executes a query and decodes the result set into a list of
 * `Map[String, Any]`, mapping each column label to its value.
 * Column nullability is reflected by wrapping nullable values
 * in `Option`.
 */
private[sqala] def jdbcQueryToMap[T](conn: Connection, sql: String, args: Array[Any])(using l: Logger): List[Map[String, Any]] =
    val start = System.nanoTime()
    var stmt: PreparedStatement = null
    var rs: ResultSet = null
    val result = ListBuffer[Map[String, Any]]()
    try
        stmt = conn.prepareStatement(sql)
        bindParameters(stmt, args)
        rs = stmt.executeQuery()
        val metaData = stmt.getMetaData()
        val columnCount = metaData.getColumnCount()
        while rs.next() do
            val map = mutable.Map[String, Any]()
            for i <- 1 to columnCount do
                val key = metaData.getColumnLabel(i)
                val value: Any = metaData.getColumnType(i) -> (metaData.isNullable(i) != java.sql.ResultSetMetaData.columnNoNulls) match
                    case (SMALLINT, true) => Option(rs.getInt(i))
                    case (SMALLINT, false) => rs.getInt(i)
                    case (INTEGER, true) => Option(rs.getInt(i))
                    case (INTEGER, false) => rs.getInt(i)
                    case (BIGINT, true) => Option(rs.getLong(i))
                    case (BIGINT, false) => rs.getLong(i)
                    case (FLOAT, true) => Option(rs.getFloat(i))
                    case (FLOAT, false) => rs.getFloat(i)
                    case (DOUBLE, true) => Option(rs.getDouble(i))
                    case (DOUBLE, false) => rs.getDouble(i)
                    case (DECIMAL | NUMERIC, true) => Option(rs.getBigDecimal(i)).map(BigDecimal(_))
                    case (DECIMAL | NUMERIC, false) => BigDecimal(rs.getBigDecimal(i))
                    case (BOOLEAN, true) => Option(rs.getBoolean(i))
                    case (BOOLEAN, false) => rs.getBoolean(i)
                    case (VARCHAR | CHAR | NVARCHAR | LONGVARCHAR | LONGNVARCHAR, true) => Option(rs.getString(i))
                    case (VARCHAR | CHAR | NVARCHAR | LONGVARCHAR | LONGNVARCHAR, false) => rs.getString(i)
                    case (DATE, true) => Option(rs.getTimestamp(i))
                    case (DATE, false) => rs.getTimestamp(i)
                    case (TIMESTAMP, true) => Option(rs.getTimestamp(i).toInstant())
                    case (TIMESTAMP, false) => rs.getTimestamp(i).toInstant()
                    case (_, true) => Option(rs.getObject(i))
                    case (_, false) => rs.getObject(i)
                map.put(key, value)
            result.addOne(map.toMap)
        val rows = result.toList
        logSuccess(l, LogOperation.Query, sql, args, start, Some(rows.size))
        rows
    catch case e: Throwable =>
        logFailure(l, LogOperation.Query, sql, args, start, e)
        throw e
    finally
        safeClose(rs)
        safeClose(stmt)

/**
 * Executes a DML statement and returns the affected row count.
 */
private[sqala] def jdbcExec(conn: Connection, sql: String, args: Array[Any])(using l: Logger): Int =
    val start = System.nanoTime()
    var stmt: PreparedStatement = null
    try
        stmt = conn.prepareStatement(sql)
        bindParameters(stmt, args)
        val count = stmt.executeUpdate()
        logSuccess(l, LogOperation.Update, sql, args, start, Some(count))
        count
    catch case e: Throwable =>
        logFailure(l, LogOperation.Update, sql, args, start, e)
        throw e
    finally
        safeClose(stmt)

/**
 * Executes a DML statement and returns the generated keys as a
 * list of `Long` values. Used for retrieving auto‑increment
 * primary keys after inserts.
 */
private[sqala] def jdbcExecReturnKey(conn: Connection, sql: String, args: Array[Any])(using l: Logger): List[Long] =
    val start = System.nanoTime()
    var stmt: PreparedStatement = null
    var resultSet: ResultSet = null
    val result = ListBuffer[Long]()
    try
        stmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)
        bindParameters(stmt, args)
        stmt.executeUpdate()
        resultSet = stmt.getGeneratedKeys
        while resultSet.next() do
            result += resultSet.getLong(1)
        val rows = result.toList
        logSuccess(l, LogOperation.Update, sql, args, start, Some(rows.size))
        rows
    catch case e: Throwable =>
        logFailure(l, LogOperation.Update, sql, args, start, e)
        throw e
    finally
        safeClose(resultSet)
        safeClose(stmt)