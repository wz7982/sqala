package sqala.printer

trait Dialect:
    def printer: SqlPrinter

object MysqlDialect extends Dialect:
    override def printer: SqlPrinter =
        new MysqlPrinter

object PostgresqlDialect extends Dialect:
    override def printer: SqlPrinter =
        new PostgresqlPrinter

object SqliteDialect extends Dialect:
    override def printer: SqlPrinter =
        new SqlitePrinter

object OracleDialect extends Dialect:
    override def printer: SqlPrinter =
        new OraclePrinter

object MssqlDialect extends Dialect:
    override def printer: SqlPrinter =
        new MssqlPrinter

object DB2Dialect extends Dialect:
    override def printer: SqlPrinter =
        new DB2Printer

object H2Dialect extends Dialect:
    override def printer: SqlPrinter =
        new H2Printer