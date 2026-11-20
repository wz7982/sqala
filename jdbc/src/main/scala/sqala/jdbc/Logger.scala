package sqala.jdbc

/**
 * Log levels used by sqala's logging abstraction.
 */
enum LogLevel:
    case Trace, Debug, Info, Warn, Error

/**
 * The kind of JDBC operation that produced a log event.
 */
enum LogOperation:
    case Query, Update, Cursor

/**
 * A structured logging event emitted by the JDBC execution layer.
 *
 * @param level       the event level
 * @param operation   the kind of JDBC operation
 * @param statement   the SQL statement
 * @param args        the bound parameters
 * @param elapsedNanos elapsed execution time in nanoseconds
 * @param rowCount    affected/fetched row count, when available
 * @param error       the failure cause, when the operation failed
 */
case class LogEvent(
    level: LogLevel,
    operation: LogOperation,
    statement: String,
    args: Array[Any],
    elapsedNanos: Long,
    rowCount: Option[Int] = None,
    error: Option[Throwable] = None
)

/**
 * A logging sink for sqala JDBC operations.
 */
trait Logger:
    /**
     * The level used for successful JDBC operations. Adapters can
     * override it to emit statements at a custom level.
     */
    def logLevel: LogLevel = LogLevel.Debug

    /**
     * Returns whether the given level is enabled.
     */
    def enabled(level: LogLevel): Boolean

    /**
     * Records one event.
     */
    def log(event: LogEvent): Unit

object Logger:
    /**
     * A logger that discards every event.
     */
    val noop: Logger = new Logger:
        def enabled(level: LogLevel): Boolean = false

        def log(event: LogEvent): Unit = ()

    /**
     * Default logger used when the application does not provide one.
     */
    given default: Logger = noop

    /**
     * Creates a console logger.
     *
     * @param level            level used for successful operations
     * @param logArgs          whether bound parameters are logged
     * @param maxArgumentLength maximum rendered argument length
     */
    def console(
        level: LogLevel = LogLevel.Debug,
        logArgs: Boolean = true,
        maxArgumentLength: Int = 200
    ): Logger = new Logger:
        override def logLevel: LogLevel = level

        def enabled(eventLevel: LogLevel): Boolean =
            eventLevel.ordinal >= level.ordinal

        def log(event: LogEvent): Unit =
            if enabled(event.level) then
                println(format(event, logArgs, maxArgumentLength))
                event.error.foreach(_.printStackTrace())

    private[sqala] def format(event: LogEvent, logArgs: Boolean, maxArgumentLength: Int): String =
        val durationMillis = event.elapsedNanos / 1_000_000.0
        val rowInfo = event.rowCount.fold("")(count => s", rows=$count")
        val head = f"[${event.level}] ${event.operation} $durationMillis%.2fms$rowInfo"
        val argsInfo =
            if logArgs && event.args.nonEmpty then
                val rendered = event.args.map(renderArg(_, maxArgumentLength)).mkString("[", ", ", "]")
                s"\nargs=$rendered"
            else
                ""
        s"$head\n${event.statement}$argsInfo"

    private def renderArg(value: Any, maxLength: Int): String =
        val text = String.valueOf(value)
        if text.length > maxLength then s"${text.take(maxLength)}..." else text

/**
 * A logger backed by `java.util.logging`.
 */
object JdkLogger:
    /**
     * Creates a logger backed by `java.util.logging`. The default
     * level is `Info`, matching the JDK logging defaults.
     */
    def apply(
        name: String = "sqala",
        level: LogLevel = LogLevel.Info,
        logArgs: Boolean = true,
        maxArgumentLength: Int = 200
    ): Logger = new Logger:
        private val logger = java.util.logging.Logger.getLogger(name)

        override def logLevel: LogLevel = level

        def enabled(eventLevel: LogLevel): Boolean =
            eventLevel.ordinal >= level.ordinal && logger.isLoggable(toJavaLevel(eventLevel))

        def log(event: LogEvent): Unit =
            if enabled(event.level) then
                val message = Logger.format(event, logArgs, maxArgumentLength)
                event.error match
                    case Some(error) => logger.log(toJavaLevel(event.level), message, error)
                    case None => logger.log(toJavaLevel(event.level), message)

    given default: Logger = apply()

    private def toJavaLevel(level: LogLevel): java.util.logging.Level =
        level match
            case LogLevel.Trace | LogLevel.Debug => java.util.logging.Level.FINE
            case LogLevel.Info => java.util.logging.Level.INFO
            case LogLevel.Warn => java.util.logging.Level.WARNING
            case LogLevel.Error => java.util.logging.Level.SEVERE