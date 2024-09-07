package sqala.jdbc

opaque type Logger = String => Unit

object Logger:
    def apply(logger: String => Unit): Logger = logger

    extension (logger: Logger)
        def apply(sql: String, args: Array[Any]): Unit =
            logger(s"execute sql: \n${sql}\n")
            val parameterString = args.map(_.toString).mkString("[", ", ", "]")
            logger(s"parameters: \n${parameterString}")