package sqala.jdbc

/**
 * A logging function that receives a formatted SQL string
 * before execution. Typically provided implicitly via
 * a `String => Unit` callback.
 *
 * {{{
 * given Logger = Logger(sql => println(sql))
 * }}}
 */
opaque type Logger = String => Unit

object Logger:
    def apply(logger: String => Unit): Logger = logger

    extension (logger: Logger)
        /**
         * Logs the SQL statement and optional parameter array.
         */
        def printLog(sql: String, args: Array[Any]): Unit =
            logger(s"Execute SQL: \n${sql}")
            if args.nonEmpty then
                val parameterString = args.map(_.toString).mkString("[", ", ", "]")
                logger(s"Args: ${parameterString}")