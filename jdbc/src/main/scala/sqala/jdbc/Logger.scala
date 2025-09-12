package sqala.jdbc

opaque type Logger = String => Unit

object Logger:
    def apply(logger: String => Unit): Logger = logger

    extension (logger: Logger)
        def apply(sql: String, args: Array[Any]): Unit =
            logger(s"Execute SQL: \n${sql}")
            val parameterString = args.map(_.toString).mkString("[", ", ", "]")
            logger(s"Args: ${parameterString}")