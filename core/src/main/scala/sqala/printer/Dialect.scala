package sqala.printer

trait Dialect:
    def printer(prepare: Boolean, indent: Int = 4): SqlPrinter

object MysqlDialect extends Dialect:
    override def printer(prepare: Boolean, indent: Int = 4): SqlPrinter =
        new MysqlPrinter(prepare, indent)

object PostgresqlDialect extends Dialect:
    override def printer(prepare: Boolean, indent: Int = 4): SqlPrinter =
        new PostgresqlPrinter(prepare, indent)

object SqliteDialect extends Dialect:
    override def printer(prepare: Boolean, indent: Int = 4): SqlPrinter =
        new SqlitePrinter(prepare, indent)

object OracleDialect extends Dialect:
    override def printer(prepare: Boolean, indent: Int = 4): SqlPrinter =
        new OraclePrinter(prepare, indent)

object MssqlDialect extends Dialect:
    override def printer(prepare: Boolean, indent: Int = 4): SqlPrinter =
        new MssqlPrinter(prepare, indent)

object DB2Dialect extends Dialect:
    override def printer(prepare: Boolean, indent: Int = 4): SqlPrinter =
        new DB2Printer(prepare, indent)