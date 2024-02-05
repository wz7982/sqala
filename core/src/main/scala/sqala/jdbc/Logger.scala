package sqala.jdbc

opaque type Logger = String => Unit

object Logger:
    def apply(logger: String => Unit): Logger = logger

    extension (logger: Logger)
        def apply(sql: String): Unit =
            logger(s"execute sql: \n${sql}")