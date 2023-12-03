package sqala.jdbc

import sqala.printer.*

trait Dialect:
    def printer: SqlPrinter

object MySqlDialect extends Dialect:
    override def printer: SqlPrinter =
        new MySqlPrinter(true)

object PostgreSqlDialect extends Dialect:
    override def printer: SqlPrinter =
        new PostgreSqlPrinter(true)

object SqliteDialect extends Dialect:
    override def printer: SqlPrinter =
        new SqlitePrinter(true)

object OracleDialect extends Dialect:
    override def printer: SqlPrinter =
        new OraclePrinter(true)

object SqlServerDialect extends Dialect:
    override def printer: SqlPrinter =
        new SqlServerPrinter(true)

object Db2Dialect extends Dialect:
    override def printer: SqlPrinter =
        new DB2Printer(true)