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

object SqliteDialect extends Dialect:
    def printer(enableJdbcPrepare: Boolean): SqlPrinter =
        new SqlitePrinter(enableJdbcPrepare)

type SqliteDialect = SqliteDialect.type

object OracleDialect extends Dialect:
    def printer(enableJdbcPrepare: Boolean): SqlPrinter =
        new OraclePrinter(enableJdbcPrepare)

type OracleDialect = OracleDialect.type

object MssqlDialect extends Dialect:
    def printer(enableJdbcPrepare: Boolean): SqlPrinter =
        new MssqlPrinter(enableJdbcPrepare)

type MssqlDialect = MssqlDialect.type

object Db2Dialect extends Dialect:
    def printer(enableJdbcPrepare: Boolean): SqlPrinter =
        new Db2Printer(enableJdbcPrepare)

type Db2Dialect = Db2Dialect.type

object H2Dialect extends Dialect:
    def printer(enableJdbcPrepare: Boolean): SqlPrinter =
        new H2Printer(enableJdbcPrepare)

type H2Dialect = H2Dialect.type