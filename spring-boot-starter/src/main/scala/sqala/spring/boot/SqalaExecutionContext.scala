package sqala.spring.boot

import org.springframework.jdbc.datasource.DataSourceUtils
import org.springframework.transaction.support.TransactionSynchronizationManager
import sqala.jdbc.{JdbcExecutionContext, JdbcContext, JdbcTransactionContext}

import java.sql.Connection
import javax.sql.DataSource

/**
 * An `JdbcExecutionContext` that participates in Spring-managed JDBC
 * transactions. Connections are obtained and released through
 * `DataSourceUtils`, so sqala operations join a transaction started
 * by Spring's `DataSourceTransactionManager` or `@Transactional`.
 */
final class SqalaExecutionContext(dataSource: DataSource) extends JdbcExecutionContext:
    given executeContext: JdbcExecutionContext = this

    def withConnection[T](context: JdbcContext)(f: Connection => T): T =
        val connection = DataSourceUtils.getConnection(dataSource)
        try
            f(connection)
        finally
            DataSourceUtils.releaseConnection(connection, dataSource)

    override def withTransaction[T](
        context: JdbcContext,
        isolation: Option[Int]
    )(block: JdbcTransactionContext ?=> T): T =
        if TransactionSynchronizationManager.hasResource(dataSource) then
            val connection = DataSourceUtils.getConnection(dataSource)
            try
                block(using JdbcTransactionContext(connection))
            finally
                DataSourceUtils.releaseConnection(connection, dataSource)
        else
            throw new IllegalStateException(
                "SqalaSpringExecutionContext requires an active Spring transaction; " +
                "use @Transactional or a TransactionTemplate."
            )