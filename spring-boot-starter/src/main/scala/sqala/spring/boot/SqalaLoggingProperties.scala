package sqala.spring.boot

import org.springframework.boot.context.properties.ConfigurationProperties
import sqala.jdbc.LogLevel

/**
 * Configuration for sqala logging.
 */
@ConfigurationProperties(prefix = "sqala.logging")
class SqalaLoggingProperties:
    private var enabled: Boolean = true

    private var level: String = "debug"

    private var logArgs: Boolean = true

    private var maxArgumentLength: Int = 200
    
    private var name: String = "sqala"

    def isEnabled: Boolean = enabled

    def setEnabled(value: Boolean): Unit = enabled = value

    def getLevel: String = level

    def setLevel(value: String): Unit = level = value

    def isLogArgs: Boolean = logArgs

    def setLogArgs(value: Boolean): Unit = logArgs = value

    def getMaxArgumentLength: Int = maxArgumentLength

    def setMaxArgumentLength(value: Int): Unit = maxArgumentLength = value

    def getName: String = name

    def setName(value: String): Unit = name = value

    def logLevel: LogLevel =
        level.trim.toLowerCase match
            case "trace" => LogLevel.Trace
            case "debug" => LogLevel.Debug
            case "info" => LogLevel.Info
            case "warn" => LogLevel.Warn
            case "error" => LogLevel.Error
            case _ => LogLevel.Debug