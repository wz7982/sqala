package sqala.printer

trait Dialect:
    def printer(enableJdbcPrepare: Boolean): SqlPrinter

object MysqlDialect extends Dialect:
    def printer(enableJdbcPrepare: Boolean): SqlPrinter =
        new MysqlPrinter(enableJdbcPrepare)

object PostgresqlDialect extends Dialect:
    def printer(enableJdbcPrepare: Boolean): SqlPrinter =
        new PostgresqlPrinter(enableJdbcPrepare)

object SqliteDialect extends Dialect:
    def printer(enableJdbcPrepare: Boolean): SqlPrinter =
        new SqlitePrinter(enableJdbcPrepare)

object OracleDialect extends Dialect:
    def printer(enableJdbcPrepare: Boolean): SqlPrinter =
        new OraclePrinter(enableJdbcPrepare)

object MssqlDialect extends Dialect:
    def printer(enableJdbcPrepare: Boolean): SqlPrinter =
        new MssqlPrinter(enableJdbcPrepare)

object DB2Dialect extends Dialect:
    def printer(enableJdbcPrepare: Boolean): SqlPrinter =
        new DB2Printer(enableJdbcPrepare)

object H2Dialect extends Dialect:
    def printer(enableJdbcPrepare: Boolean): SqlPrinter =
        new H2Printer(enableJdbcPrepare)