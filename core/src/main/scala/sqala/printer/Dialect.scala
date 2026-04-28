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

object SqlserverDialect extends Dialect:
    def printer(standardEscapeStrings: Boolean): SqlPrinter =
        new SqlserverPrinter(standardEscapeStrings)

object SqliteDialect extends Dialect:
    def printer(standardEscapeStrings: Boolean): SqlPrinter =
        new SqlitePrinter(standardEscapeStrings)

object H2Dialect extends Dialect:
    def printer(standardEscapeStrings: Boolean): SqlPrinter =
        new H2Printer(standardEscapeStrings)