package sqala.printer

trait Dialect:
    def printer(standardEscapeStrings: Boolean): SqlPrinter

object MysqlDialect extends Dialect:
    def printer(standardEscapeStrings: Boolean): SqlPrinter =
        new MysqlPrinter(standardEscapeStrings)

type MysqlDialect = MysqlDialect.type

object PostgresqlDialect extends Dialect:
    def printer(standardEscapeStrings: Boolean): SqlPrinter =
        new PostgresqlPrinter(standardEscapeStrings)

type PostgresqlDialect = PostgresqlDialect.type

object OracleDialect extends Dialect:
    def printer(standardEscapeStrings: Boolean): SqlPrinter =
        new OraclePrinter(standardEscapeStrings)

type OracleDialect = OracleDialect.type

object H2Dialect extends Dialect:
    def printer(standardEscapeStrings: Boolean): SqlPrinter =
        new H2Printer(standardEscapeStrings)

type H2Dialect = H2Dialect.type