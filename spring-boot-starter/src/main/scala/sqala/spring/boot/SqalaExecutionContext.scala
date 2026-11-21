package sqala.spring.boot

import org.springframework.dao.DataAccessException
import org.springframework.jdbc.UncategorizedSQLException
import org.springframework.jdbc.datasource.{ConnectionHolder, DataSourceUtils}
import org.springframework.jdbc.support.SQLErrorCodeSQLExceptionTranslator
import org.springframework.transaction.support.TransactionSynchronizationManager
import sqala.jdbc.{JdbcContext, JdbcExecutionContext, JdbcTransactionContext}

import java.lang.reflect.{InvocationTargetException, Proxy}
import java.sql.{Connection, SQLException, Statement}
import javax.sql.DataSource

/**
 * A `JdbcExecutionContext` that participates in Spring-managed JDBC
 * transactions. Connections are obtained and released through
 * `DataSourceUtils`, so sqala operations join a transaction started
 * by Spring's `DataSourceTransactionManager` or `@Transactional`.
 *
 * The context always uses the `DataSource` of the `JdbcContext` passed
 * to it, so a single instance can be shared by multiple data sources.
 */
final class SqalaExecutionContext extends JdbcExecutionContext:
    given executeContext: JdbcExecutionContext = this

    def withConnection[T](context: JdbcContext)(f: Connection => T): T =
        val dataSource = context.dataSource
        val connection = DataSourceUtils.getConnection(dataSource)
        val statementConnection = withStatementTimeout(connection, dataSource)
        try
            f(statementConnection)
        catch
            case e: SQLException =>
                throw translate(dataSource, e)
        finally
            DataSourceUtils.releaseConnection(connection, dataSource)

    override def withTransaction[T](
        context: JdbcContext,
        isolation: Option[Int]
    )(block: JdbcTransactionContext ?=> T): T =
        val dataSource = context.dataSource
        if TransactionSynchronizationManager.hasResource(dataSource) then
            val connection = DataSourceUtils.getConnection(dataSource)
            val statementConnection = withStatementTimeout(connection, dataSource)
            try
                block(using JdbcTransactionContext(statementConnection))
            catch
                case e: SQLException =>
                    throw translate(dataSource, e)
            finally
                DataSourceUtils.releaseConnection(connection, dataSource)
        else
            throw new IllegalStateException(
                "SqalaExecutionContext requires an active Spring transaction; " +
                "use @Transactional or a TransactionTemplate."
            )

    private def withStatementTimeout(connection: Connection, dataSource: DataSource): Connection =
        TransactionSynchronizationManager.getResource(dataSource) match
            case holder: ConnectionHolder if holder.hasTimeout() =>
                Proxy.newProxyInstance(
                    classOf[Connection].getClassLoader,
                    Array(classOf[Connection]),
                    (_, method, args) =>
                        val arguments = if args == null then Array.empty[AnyRef] else args
                        val result =
                            try method.invoke(connection, arguments*)
                            catch
                                case e: InvocationTargetException =>
                                    throw Option(e.getCause).getOrElse(e)
                        result match
                            case statement: Statement if isStatementFactory(method.getName) =>
                                DataSourceUtils.applyTimeout(statement, dataSource, -1)
                                statement
                            case other =>
                                other
                ).asInstanceOf[Connection]
            case _ =>
                connection

    private def isStatementFactory(methodName: String): Boolean =
        methodName == "prepareStatement" ||
        methodName == "prepareCall" ||
        methodName == "createStatement"

    private def translate(dataSource: DataSource, error: SQLException): DataAccessException =
        val translated = new SQLErrorCodeSQLExceptionTranslator(dataSource)
            .translate("sqala", null, error)
        if translated == null then
            new UncategorizedSQLException("sqala", null, error)
        else
            translated
