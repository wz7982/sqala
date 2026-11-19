package bench

import bench.jvm.{BenchUser, BenchUserMapper}
import org.apache.ibatis.mapping.Environment
import org.apache.ibatis.session.{SqlSessionFactory, SqlSessionFactoryBuilder}
import org.apache.ibatis.transaction.jdbc.JdbcTransactionFactory

import javax.sql.DataSource

/**
 * MyBatis with a programmatic configuration backed by the shared
 * HikariCP pool. A `SqlSession` is opened and closed per operation.
 */
final class MyBatisOps(dataSource: DataSource):

    private val mapperClass: Class[BenchUserMapper] = classOf[BenchUserMapper]

    private val sessionFactory: SqlSessionFactory =
        val configuration = new org.apache.ibatis.session.Configuration()
        configuration.setMapUnderscoreToCamelCase(true)
        configuration.setEnvironment(
            new Environment("benchmark", new JdbcTransactionFactory(), dataSource)
        )
        configuration.addMapper(mapperClass)
        new SqlSessionFactoryBuilder().build(configuration)

    def point(id: Long): BenchUser =
        val session = sessionFactory.openSession(true)
        try
            session.getMapper(mapperClass).findById(id)
        finally
            session.close()

    def range(start: Long, end: Long): java.util.List[BenchUser] =
        val session = sessionFactory.openSession(true)
        try
            session.getMapper(mapperClass).findRange(start, end)
        finally
            session.close()
