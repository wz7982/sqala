package sqala.printer

trait Dialect:
    def printer(standardEscapeStrings: Boolean): SqlPrinter

object MysqlDialect extends Dialect:
    def printer(standardEscapeStrings: Boolean): SqlPrinter =
        new MysqlPrinter(standardEscapeStrings)

object PostgresqlDialect extends Dialect:
    def printer(standardEscapeStrings: Boolean): SqlPrinter =
        new PostgresqlPrinter(standardEscapeStrings)

object OracleDialect extends Dialect:
    def printer(standardEscapeStrings: Boolean): SqlPrinter =
        new OraclePrinter(standardEscapeStrings)

object H2Dialect extends Dialect:
    def printer(standardEscapeStrings: Boolean): SqlPrinter =
        new H2Printer(standardEscapeStrings)

object SqlserverDialect extends Dialect:
    def printer(standardEscapeStrings: Boolean): SqlPrinter =
        new SqlserverPrinter(standardEscapeStrings)