package sqala.printer

trait Dialect:
    def printer(enableJdbcPrepare: Boolean): SqlPrinter

object MysqlDialect extends Dialect:
    def printer(enableJdbcPrepare: Boolean): SqlPrinter =
        new MysqlPrinter(enableJdbcPrepare)

type MysqlDialect = MysqlDialect.type

object PostgresqlDialect extends Dialect:
    def printer(enableJdbcPrepare: Boolean): SqlPrinter =
        new PostgresqlPrinter(enableJdbcPrepare)

type PostgresqlDialect = PostgresqlDialect.type

object OracleDialect extends Dialect:
    def printer(enableJdbcPrepare: Boolean): SqlPrinter =
        new OraclePrinter(enableJdbcPrepare)

type OracleDialect = OracleDialect.type

object H2Dialect extends Dialect:
    def printer(enableJdbcPrepare: Boolean): SqlPrinter =
        new H2Printer(enableJdbcPrepare)

type H2Dialect = H2Dialect.type