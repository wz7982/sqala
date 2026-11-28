package sqala.spring.boot

import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.{
    ConditionalOnClass,
    ConditionalOnMissingBean,
    ConditionalOnSingleCandidate
}
import org.springframework.context.annotation.Bean
import org.springframework.jdbc.datasource.{DataSourceTransactionManager, DataSourceUtils}
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.support.TransactionSynchronizationManager
import sqala.jdbc.JdbcExecutionContext

import javax.sql.DataSource

/**
 * Registers Spring-aware JDBC execution and transaction support.
 */
@AutoConfiguration(afterName = Array(
    "org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration",
    "org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration",
    "org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration",
    "org.springframework.boot.jdbc.autoconfigure.DataSourceTransactionManagerAutoConfiguration"
))
@ConditionalOnClass(Array(
    classOf[DataSource],
    classOf[DataSourceUtils],
    classOf[TransactionSynchronizationManager],
    classOf[PlatformTransactionManager]
))
class SqalaTransactionAutoConfiguration:
    @Bean
    @ConditionalOnMissingBean(Array(classOf[JdbcExecutionContext]))
    def sqalaSpringExecutionContext(): SqalaExecutionContext =
        SqalaExecutionContext()

    @Bean
    @ConditionalOnSingleCandidate(classOf[DataSource])
    @ConditionalOnMissingBean(Array(classOf[PlatformTransactionManager]))
    def sqalaTransactionManager(dataSource: DataSource): PlatformTransactionManager =
        DataSourceTransactionManager(dataSource)
