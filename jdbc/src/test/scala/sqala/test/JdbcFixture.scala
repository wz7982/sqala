package sqala.test

import sqala.jdbc.*
import sqala.jdbc.dialect.{MysqlJdbcDialect, PostgresqlJdbcDialect}
import sqala.metadata.{CustomField, Json, autoInc, primaryKey}

import java.sql.{Connection, DriverManager, Statement}
import java.time.{LocalDate, LocalDateTime, LocalTime, OffsetDateTime, OffsetTime}
import javax.sql.DataSource
import scala.language.unsafeNulls

case class JUser(@primaryKey id: Int, name: String, age: Int)

case class JAuto(@primaryKey @autoInc id: Int, name: String, memo: Option[String])

case class JKey(@primaryKey src: Int, @primaryKey dst: Int, label: String)

case class JTime(dt: LocalDate, ts: OffsetDateTime, lts: LocalDateTime, tm: OffsetTime, ltm: LocalTime)

case class JScalarPg(
    @primaryKey id: Int,
    s: String,
    os: Option[String],
    b: Boolean,
    i: Int,
    l: Long,
    f: Float,
    d: Double,
    bd: BigDecimal,
    js: Json,
    ojs: Option[Json],
    arr: Array[Int],
    oarr: Option[Array[Int]],
    dt: LocalDate,
    lts: LocalDateTime,
    ltm: LocalTime,
    odt: OffsetDateTime,
    otz: OffsetTime
)

case class JScalarMysql(
    @primaryKey id: Int,
    s: String,
    os: Option[String],
    b: Boolean,
    i: Int,
    l: Long,
    f: Float,
    d: Double,
    bd: BigDecimal,
    js: Json,
    dt: LocalDate,
    lts: LocalDateTime,
    ltm: LocalTime,
    odt: OffsetDateTime
)

enum JStatus:
    case Active, Inactive

object JStatus:
    given CustomField[JStatus, String] with
        def toValue(x: JStatus): String =
            x.toString

        def fromValue(x: String): JStatus =
            JStatus.valueOf(x)

case class JCustom(@primaryKey id: Int, st: JStatus, ost: Option[JStatus])

/**
 * Shared fixtures for the JDBC integration suites. All suites run against
 * a local PostgreSQL and MySQL instance with a `sqala_test` database, and
 * reset only the tables they touch.
 */
object JdbcFixture:
    val pgUrl = "jdbc:postgresql://localhost:5432/sqala_test"
    val pgUser = "postgres"
    val pgPassword = "123456"
    val mysqlUrl = "jdbc:mysql://127.0.0.1:3306/sqala_test"
    val mysqlUser = "root"
    val mysqlPassword = "123456"

    private final class UrlDataSource(url: String, user: String, password: String) extends DataSource:
        def getConnection(): Connection =
            DriverManager.getConnection(url, user, password)

        def getConnection(u: String, p: String): Connection =
            DriverManager.getConnection(url, u, p)

        def getLogWriter: java.io.PrintWriter =
            null

        def setLogWriter(out: java.io.PrintWriter): Unit =
            ()

        def setLoginTimeout(seconds: Int): Unit =
            ()

        def getLoginTimeout: Int =
            0

        def getParentLogger: java.util.logging.Logger =
            throw new java.sql.SQLFeatureNotSupportedException()

        def unwrap[T](iface: Class[T]): T =
            null.asInstanceOf[T]

        def isWrapperFor(iface: Class[?]): Boolean =
            false

    def pgSource: DataSource =
        UrlDataSource(pgUrl, pgUser, pgPassword)

    def pgContext: JdbcContext =
        JdbcContext(pgSource, PostgresqlJdbcDialect, false)

    def pgContextEscape: JdbcContext =
        JdbcContext(pgSource, PostgresqlJdbcDialect, true)

    def mysqlContext: JdbcContext =
        JdbcContext(UrlDataSource(mysqlUrl, mysqlUser, mysqlPassword), MysqlJdbcDialect, false)

    /**
     * Runs raw DDL statements outside the library, so schema setup does
     * not depend on the code under test.
     */
    def runDdl(url: String, user: String, password: String)(stmts: String*): Unit =
        val conn = DriverManager.getConnection(url, user, password)
        try
            val stmt: Statement = conn.createStatement()
            try
                stmts.foreach(s => stmt.execute(s))
            finally
                stmt.close()
        finally
            conn.close()

    def runPgDdl(stmts: String*): Unit =
        runDdl(pgUrl, pgUser, pgPassword)(stmts*)

    def runMysqlDdl(stmts: String*): Unit =
        runDdl(mysqlUrl, mysqlUser, mysqlPassword)(stmts*)

    val userDdl: Seq[String] =
        Seq(
            "drop table if exists j_user",
            "create table j_user(id int primary key, name varchar(64) not null, age int not null)"
        )

    val pgDdl: Seq[String] =
        userDdl ++ Seq(
            "drop table if exists j_auto",
            "create table j_auto(id serial primary key, name varchar(64), memo varchar(64))",
            "drop table if exists j_key",
            "create table j_key(src int, dst int, label varchar(64), primary key(src, dst))",
            "drop table if exists j_time",
            "create table j_time(dt date, ts timestamp with time zone, lts timestamp, tm time with time zone, ltm time)"
        )

    val mysqlDdl: Seq[String] =
        userDdl ++ Seq(
            "drop table if exists j_auto",
            "create table j_auto(id int auto_increment primary key, name varchar(64), memo varchar(64))",
            "drop table if exists j_key",
            "create table j_key(src int, dst int, label varchar(64), primary key(src, dst))",
            "drop table if exists j_time",
            "create table j_time(dt date, ts timestamp(6) null, lts datetime(6), tm time(6), ltm time(6))"
        )

    val pgScalarDdl: Seq[String] =
        Seq(
            "drop table if exists j_scalar_pg",
            """create table j_scalar_pg(id int primary key, s varchar(64), os varchar(64), b boolean, i int, l bigint,
              | f real, d double precision, bd numeric(20,4), js json, ojs json, arr int[], oarr int[],
              | dt date, lts timestamp, ltm time, odt timestamp with time zone, otz time with time zone)""".stripMargin,
            "drop table if exists j_custom",
            "create table j_custom(id int primary key, st varchar(32), ost varchar(32))"
        )

    val mysqlScalarDdl: Seq[String] =
        Seq(
            "drop table if exists j_scalar_mysql",
            """create table j_scalar_mysql(id int primary key, s varchar(64), os varchar(64), b boolean, i int, l bigint,
              | f float, d double precision, bd decimal(20,4), js json,
              | dt date, lts datetime(6), ltm time(6), odt timestamp(6) null)""".stripMargin,
            "drop table if exists j_custom",
            "create table j_custom(id int primary key, st varchar(32), ost varchar(32))"
        )

    /**
     * Compares arrays field-wise, since array equality in munit relies on
     * reference equality.
     */
    def sameRows(actual: List[Array[Int]], expected: List[List[Int]]): Boolean =
        actual.map(_.toList) == expected
