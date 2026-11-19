package bench

import bench.jvm.BenchUser
import org.hibernate.SessionFactory
import org.hibernate.boot.MetadataSources
import org.hibernate.boot.registry.StandardServiceRegistryBuilder

import javax.sql.DataSource

/**
 * Hibernate ORM using the shared HikariCP pool. A `Session` is opened
 * and closed per operation; the entity is loaded by id or by an HQL
 * range query.
 */
final class HibernateOps(dataSource: DataSource):

    private val sessionFactory: SessionFactory =
        val registry = new StandardServiceRegistryBuilder()
            .applySetting("hibernate.connection.datasource", dataSource)
            .applySetting("hibernate.dialect", "org.hibernate.dialect.PostgreSQLDialect")
            .applySetting("hibernate.show_sql", "false")
            .applySetting("hibernate.hbm2ddl.auto", "none")
            .applySetting("hibernate.cache.use_second_level_cache", "false")
            .build()
        val metadata = new MetadataSources(registry)
            .addAnnotatedClass(classOf[BenchUser])
            .buildMetadata()
        metadata.buildSessionFactory()

    def point(id: Long): BenchUser =
        val session = sessionFactory.openSession()
        try
            session.find(classOf[BenchUser], java.lang.Long.valueOf(id))
        finally
            session.close()

    def range(start: Long, end: Long): java.util.List[BenchUser] =
        val session = sessionFactory.openSession()
        try
            val query = session.createSelectionQuery(
                "select u from BenchUserEntity u where u.id >= :start and u.id < :end",
                classOf[BenchUser]
            )
            query.setParameter("start", java.lang.Long.valueOf(start))
            query.setParameter("end", java.lang.Long.valueOf(end))
            query.getResultList()
        finally
            session.close()
