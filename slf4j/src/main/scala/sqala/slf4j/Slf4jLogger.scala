package sqala.slf4j

import org.slf4j.LoggerFactory
import sqala.jdbc.{LogEvent, LogLevel, Logger}

/**
 * Creates sqala loggers backed by SLF4J.
 */
object Slf4jLogger:
    /**
     * Creates a logger using a named SLF4J logger.
     */
    def apply(
        name: String = "sqala",
        logArgs: Boolean = true,
        maxArgumentLength: Int = 200,
        level: LogLevel = LogLevel.Debug
    ): Logger =
        apply(LoggerFactory.getLogger(name), logArgs, maxArgumentLength, level)

    /**
     * Creates a logger using an existing SLF4J logger.
     */
    def apply(
        logger: org.slf4j.Logger,
        logArgs: Boolean,
        maxArgumentLength: Int,
        level: LogLevel
    ): Logger = new Logger:
        override def logLevel: LogLevel = level

        def enabled(level: LogLevel): Boolean =
            level match
                case LogLevel.Trace => logger.isTraceEnabled
                case LogLevel.Debug => logger.isDebugEnabled
                case LogLevel.Info => logger.isInfoEnabled
                case LogLevel.Warn => logger.isWarnEnabled
                case LogLevel.Error => logger.isErrorEnabled

        def log(event: LogEvent): Unit =
            if enabled(event.level) then
                val message = Logger.format(event, logArgs, maxArgumentLength)
                event.error match
                    case Some(error) =>
                        event.level match
                            case LogLevel.Trace => logger.trace(message, error)
                            case LogLevel.Debug => logger.debug(message, error)
                            case LogLevel.Info => logger.info(message, error)
                            case LogLevel.Warn => logger.warn(message, error)
                            case LogLevel.Error => logger.error(message, error)
                    case None =>
                        event.level match
                            case LogLevel.Trace => logger.trace(message)
                            case LogLevel.Debug => logger.debug(message)
                            case LogLevel.Info => logger.info(message)
                            case LogLevel.Warn => logger.warn(message)
                            case LogLevel.Error => logger.error(message)

/**
 * Default SLF4J logger, imported with `import sqala.slf4j.given`.
 */
given slf4jLogger: Logger = Slf4jLogger()