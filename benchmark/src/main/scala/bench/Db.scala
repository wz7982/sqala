package bench

import com.zaxxer.hikari.{HikariConfig, HikariDataSource}

import java.sql.{Connection, Types}
import java.time.LocalDateTime
import java.util.concurrent.ThreadLocalRandom

/**
 * Shared PostgreSQL setup.
 *
 * All frameworks use the same HikariCP pool instance, so the only thing
 * being measured is the framework itself: SQL rendering, statement
 * execution and result-set decoding.
 */
object Db:

    val RowCount: Int = 100_000

    val RangeSize: Int = 10_000

    val JdbcUrl: String = "jdbc:postgresql://localhost:5432/postgres"

    val Username: String = "postgres"

    val Password: String = "123456"

    lazy val dataSource: HikariDataSource =
        val config = new HikariConfig()
        config.setJdbcUrl(JdbcUrl)
        config.setUsername(Username)
        config.setPassword(Password)
        config.setDriverClassName("org.postgresql.Driver")
        config.setPoolName("sqala-benchmark-pool")
        config.setMaximumPoolSize(32)
        config.setMinimumIdle(8)
        config.setConnectionTimeout(10_000)
        new HikariDataSource(config)

    /** A random existing primary key. Thread-safe for multi-threaded JMH runs. */
    def nextPointId(): Long =
        1L + ThreadLocalRandom.current().nextLong(RowCount.toLong)

    /** A random start id for a contiguous range of [[RangeSize]] rows. */
    def nextRangeStart(): Long =
        1L + ThreadLocalRandom.current().nextLong((RowCount - RangeSize + 1).toLong)

    /**
     * Creates the table and inserts [[RowCount]] rows if needed.
     *
     * JMH runs every benchmark in a fresh JVM, so this is called from
     * `@Setup(Trial)` and must be cheap after the first call.
     */
    def ensureSeeded(): Unit =
        val connection = dataSource.getConnection()
        try
            createTable(connection)
            if rowCount(connection) != RowCount then
                truncate(connection)
                insertRows(connection)
        finally
            connection.close()

    private def createTable(connection: Connection): Unit =
        val statement = connection.createStatement()
        try
            statement.execute(
                """CREATE TABLE IF NOT EXISTS bench_user (
                  |    id          BIGINT PRIMARY KEY,
                  |    name        VARCHAR(64)  NOT NULL,
                  |    email       VARCHAR(128) NOT NULL,
                  |    age         INT          NOT NULL,
                  |    active      BOOLEAN      NOT NULL,
                  |    score       NUMERIC(18,2) NOT NULL,
                  |    created_at  TIMESTAMP    NOT NULL,
                  |    bio         TEXT,
                  |    city        VARCHAR(64),
                  |    balance     DOUBLE PRECISION NOT NULL
                  |)""".stripMargin
            )
        finally
            statement.close()

    private def rowCount(connection: Connection): Long =
        val statement = connection.createStatement()
        try
            val resultSet = statement.executeQuery("SELECT COUNT(*) FROM bench_user")
            try
                resultSet.next()
                resultSet.getLong(1)
            finally
                resultSet.close()
        finally
            statement.close()

    private def truncate(connection: Connection): Unit =
        val statement = connection.createStatement()
        try statement.execute("TRUNCATE TABLE bench_user")
        finally statement.close()

    private def insertRows(connection: Connection): Unit =
        val previousAutoCommit = connection.getAutoCommit
        connection.setAutoCommit(false)
        val statement = connection.prepareStatement(
            "INSERT INTO bench_user (id, name, email, age, active, score, created_at, bio, city, balance) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        )
        try
            val baseTime = LocalDateTime.of(2024, 1, 1, 0, 0, 0)
            var i = 1
            while i <= RowCount do
                statement.setLong(1, i.toLong)
                statement.setString(2, s"user_$i")
                statement.setString(3, s"user_$i@example.com")
                statement.setInt(4, 18 + (i % 50))
                statement.setBoolean(5, i % 4 != 0)
                statement.setBigDecimal(6, java.math.BigDecimal.valueOf(i.toLong * 100L, 2))
                statement.setTimestamp(7, java.sql.Timestamp.valueOf(baseTime.plusSeconds(i.toLong)))
                if i % 10 == 0 then statement.setNull(8, Types.VARCHAR)
                else statement.setString(8, s"bio of user $i")
                if i % 20 == 0 then statement.setNull(9, Types.VARCHAR)
                else statement.setString(9, s"city_${i % 100}")
                statement.setDouble(10, i.toDouble * 0.37)
                statement.addBatch()
                if i % 1000 == 0 then statement.executeBatch()
                i += 1
            statement.executeBatch()
            connection.commit()
        catch
            case e: Exception =>
                connection.rollback()
                throw e
        finally
            statement.close()
            connection.setAutoCommit(previousAutoCommit)
