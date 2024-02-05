package sqala.jdbc

import java.sql.{Connection, PreparedStatement, ResultSet, Statement}
import java.util.Date
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.language.unsafeNulls

private[sqala] def jdbcQuery[T](conn: Connection, sql: String, args: Array[Any])(using decoder: Decoder[T]): List[T] =
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
                case d: Date => stmt.setDate(i, java.sql.Date(d.getTime))
                case _ => stmt.setObject(i, arg)
        rs = stmt.executeQuery()
        while rs.next() do
            result.addOne(decoder.decode(rs, 1))
        result.toList
    catch
        case e: Exception => throw e
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
                case d: Date => stmt.setDate(i, java.sql.Date(d.getTime))
                case _ => stmt.setObject(i, arg)
        stmt.executeUpdate()
    catch
        case e: Exception => throw e
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
                case d: Date => stmt.setDate(i, java.sql.Date(d.getTime))
                case _ => stmt.setObject(i, arg)
        stmt.executeUpdate()
        val resultSet = stmt.getGeneratedKeys
        while resultSet.next() do
            result += resultSet.getLong(1)
        result.toList
    catch
        case e: Exception => throw e
    finally
        if stmt != null then stmt.close()