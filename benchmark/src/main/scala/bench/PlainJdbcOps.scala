package bench

import bench.jvm.BenchUser

import javax.sql.DataSource
import java.sql.ResultSet

/**
 * Plain JDBC baseline. A connection is borrowed from the shared pool
 * and closed after each operation, exactly like the other frameworks do.
 */
final class PlainJdbcOps(dataSource: DataSource):

    private val pointSql: String =
        "SELECT id, name, email, age, active, score, created_at, bio, city, balance FROM bench_user WHERE id = ?"

    private val rangeSql: String =
        "SELECT id, name, email, age, active, score, created_at, bio, city, balance FROM bench_user WHERE id >= ? AND id < ?"

    def point(id: Long): BenchUser =
        val connection = dataSource.getConnection()
        try
            val statement = connection.prepareStatement(pointSql)
            try
                statement.setLong(1, id)
                val resultSet = statement.executeQuery()
                try
                    resultSet.next()
                    read(resultSet)
                finally
                    resultSet.close()
            finally
                statement.close()
        finally
            connection.close()

    def range(start: Long, end: Long): java.util.List[BenchUser] =
        val connection = dataSource.getConnection()
        try
            val statement = connection.prepareStatement(rangeSql)
            try
                statement.setLong(1, start)
                statement.setLong(2, end)
                val resultSet = statement.executeQuery()
                try
                    val result = new java.util.ArrayList[BenchUser](Db.RangeSize)
                    while resultSet.next() do result.add(read(resultSet))
                    result
                finally
                    resultSet.close()
            finally
                statement.close()
        finally
            connection.close()

    private def read(resultSet: ResultSet): BenchUser =
        val user = new BenchUser()
        user.setId(resultSet.getLong(1))
        user.setName(resultSet.getString(2))
        user.setEmail(resultSet.getString(3))
        user.setAge(resultSet.getInt(4))
        user.setActive(resultSet.getBoolean(5))
        user.setScore(resultSet.getBigDecimal(6))
        user.setCreatedAt(resultSet.getObject(7, classOf[java.time.LocalDateTime]))
        user.setBio(resultSet.getString(8))
        user.setCity(resultSet.getString(9))
        user.setBalance(resultSet.getDouble(10))
        user
