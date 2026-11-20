package sqala.spring.boot

import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.{ConditionalOnClass, ConditionalOnMissingBean}
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import sqala.jdbc.Logger
import sqala.slf4j.Slf4jLogger

/**
 * Registers a Spring-managed sqala logger backed by SLF4J.
 */
@AutoConfiguration
@ConditionalOnClass(Array(classOf[org.slf4j.Logger]))
@EnableConfigurationProperties(Array(classOf[SqalaLoggingProperties]))
class SqalaLoggingAutoConfiguration:
    @Bean
    @ConditionalOnMissingBean(Array(classOf[SqalaLogging]))
    def sqalaLogging(properties: SqalaLoggingProperties): SqalaLogging =
        val logger =
            if properties.isEnabled then
                Slf4jLogger(
                    properties.getName,
                    properties.isLogArgs,
                    properties.getMaxArgumentLength,
                    properties.logLevel
                )
            else
                Logger.noop
        SqalaLogging(logger)