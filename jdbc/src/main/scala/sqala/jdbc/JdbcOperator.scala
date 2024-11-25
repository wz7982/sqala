package sqala.jdbc

import java.sql.{Connection, PreparedStatement, ResultSet, Statement}
import java.sql.Types.*
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.language.unsafeNulls

private[sqala] def jdbcQuery[T](conn: Connection, sql: String, args: Array[Any])(using decoder: JdbcDecoder[T]): List[T] =
    var stmt: PreparedStatement = null
    var rs: ResultSet = null
    val result = ListBuffer[T]()
    try
        stmt = conn.prepareStatement(sql)
        for i <- 1 to args.length do
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
        rs = stmt.executeQuery()
        while rs.next() do
            result.addOne(decoder.decode(rs, 1))
        result.toList
    finally
        if stmt != null then stmt.close()
        if rs != null then rs.close()

private[sqala] def jdbcQueryToMap[T](conn: Connection, sql: String, args: Array[Any]): List[Map[String, Any]] =
    var stmt: PreparedStatement = null
    var rs: ResultSet = null
    val result = ListBuffer[Map[String, Any]]()
    try
        stmt = conn.prepareStatement(sql)
        for i <- 1 to args.length do
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
                    case (DECIMAL, true) => Option(rs.getBigDecimal(i)).map(BigDecimal(_))
                    case (DECIMAL, false) => BigDecimal(rs.getBigDecimal(i))
                    case (BOOLEAN, true) => Option(rs.getBoolean(i))
                    case (BOOLEAN, false) => rs.getBoolean(i)
                    case (VARCHAR | CHAR | NVARCHAR | LONGVARCHAR | LONGNVARCHAR, true) => Option(rs.getString(i))
                    case (VARCHAR | CHAR | NVARCHAR | LONGVARCHAR | LONGNVARCHAR, false) => rs.getString(i)
                    case (DATE | TIME | TIMESTAMP, true) => Option(rs.getDate(i))
                    case (DATE | TIME | TIMESTAMP, false) => rs.getDate(i)
                    case (_, true) => Option(rs.getObject(i))
                    case (_, false) => rs.getObject(i)
                map.put(key, value)
            result.addOne(map.toMap)
        result.toList
    finally
        if stmt != null then stmt.close()
        if rs != null then rs.close()

private[sqala] def jdbcExec(conn: Connection, sql: String, args: Array[Any]): Int =
    var stmt: PreparedStatement = null
    try
        stmt = conn.prepareStatement(sql)
        for i <- 1 to args.length do
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
        stmt.executeUpdate()
    finally
        if stmt != null then stmt.close()

private[sqala] def jdbcExecReturnKey(conn: Connection, sql: String, args: Array[Any]): List[Long] =
    var stmt: PreparedStatement = null
    val result = ListBuffer[Long]()
    try
        stmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)
        for i <- 1 to args.length do
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
        stmt.executeUpdate()
        val resultSet = stmt.getGeneratedKeys
        while resultSet.next() do
            result += resultSet.getLong(1)
        result.toList
    finally
        if stmt != null then stmt.close()