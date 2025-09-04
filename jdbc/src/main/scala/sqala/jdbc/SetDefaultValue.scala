package sqala.jdbc

import java.sql.PreparedStatement
import java.time.{LocalDate, LocalDateTime}

trait SetDefaultValue[T]:
    def setValue(stmt: PreparedStatement, n: Int): Unit

object SetDefaultValue:
    given setInt: SetDefaultValue[Int] with
        override def setValue(stmt: PreparedStatement, n: Int): Unit =
            stmt.setInt(n, 0)

    given setLong: SetDefaultValue[Long] with
        override def setValue(stmt: PreparedStatement, n: Int): Unit =
            stmt.setLong(n, 0L)

    given setFloat: SetDefaultValue[Float] with
        override def setValue(stmt: PreparedStatement, n: Int): Unit =
            stmt.setFloat(n, 0.0F)

    given setDouble: SetDefaultValue[Double] with
        override def setValue(stmt: PreparedStatement, n: Int): Unit =
            stmt.setDouble(n, 0.0)

    given setDecimal: SetDefaultValue[BigDecimal] with
        override def setValue(stmt: PreparedStatement, n: Int): Unit =
            stmt.setBigDecimal(n, BigDecimal(0).bigDecimal)

    given setBoolean: SetDefaultValue[Boolean] with
        override def setValue(stmt: PreparedStatement, n: Int): Unit =
            stmt.setBoolean(n, false)

    given setString: SetDefaultValue[String] with
        override def setValue(stmt: PreparedStatement, n: Int): Unit =
            stmt.setString(n, "")

    given setLocalDate: SetDefaultValue[LocalDate] with
        override def setValue(stmt: PreparedStatement, n: Int): Unit =
            stmt.setDate(n, java.sql.Date.valueOf("1970-01-01"))

    given setLocalDateTime: SetDefaultValue[LocalDateTime] with
        override def setValue(stmt: PreparedStatement, n: Int): Unit =
            stmt.setDate(n, java.sql.Date.valueOf("1970-01-01"))