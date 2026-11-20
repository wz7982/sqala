package sqala.spring.boot

import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.{ConditionalOnBean, ConditionalOnClass, ConditionalOnMissingBean}
import org.springframework.context.annotation.Bean
import org.springframework.jdbc.datasource.{DataSourceTransactionManager, DataSourceUtils}
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.support.TransactionSynchronizationManager
import sqala.jdbc.ExecuteContext as JdbcExecuteContext

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
    @ConditionalOnMissingBean(Array(classOf[JdbcExecuteContext]))
    @ConditionalOnBean(Array(classOf[DataSource]))
    def sqalaSpringExecuteContext(dataSource: DataSource): SqalaSpringExecuteContext =
        SqalaSpringExecuteContext(dataSource)

    @Bean
    @ConditionalOnMissingBean(Array(classOf[PlatformTransactionManager]))
    @ConditionalOnBean(Array(classOf[DataSource]))
    def sqalaTransactionManager(dataSource: DataSource): PlatformTransactionManager =
        DataSourceTransactionManager(dataSource)