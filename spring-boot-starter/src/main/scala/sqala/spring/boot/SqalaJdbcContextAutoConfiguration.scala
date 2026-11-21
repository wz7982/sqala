package sqala.spring.boot

import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.{
    ConditionalOnClass,
    ConditionalOnMissingBean,
    ConditionalOnSingleCandidate
}
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import sqala.jdbc.JdbcContext

import javax.sql.DataSource

/**
 * Registers a `JdbcContext` backed by the application's `DataSource`.
 */
@AutoConfiguration(afterName = Array(
    "org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration",
    "org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration"
))
@ConditionalOnClass(Array(classOf[JdbcContext]))
@ConditionalOnSingleCandidate(classOf[DataSource])
@EnableConfigurationProperties(Array(classOf[SqalaJdbcProperties]))
class SqalaJdbcContextAutoConfiguration:
    @Bean
    @ConditionalOnMissingBean(Array(classOf[JdbcContext]))
    def sqalaJdbcContext(
        dataSource: DataSource,
        properties: SqalaJdbcProperties
    ): JdbcContext =
        JdbcContext(
            dataSource = dataSource,
            dialect = properties.jdbcDialect,
            standardEscapeStrings = properties.isStandardEscapeStrings
        )