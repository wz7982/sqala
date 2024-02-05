package sqala.jdbc

import sqala.printer.*

trait Dialect:
    def printer(prepare: Boolean): SqlPrinter

object MysqlDialect extends Dialect:
    override def printer(prepare: Boolean): SqlPrinter =
        new MysqlPrinter(prepare)

object PostgresqlDialect extends Dialect:
    override def printer(prepare: Boolean): SqlPrinter =
        new PostgresqlPrinter(prepare)

object SqliteDialect extends Dialect:
    override def printer(prepare: Boolean): SqlPrinter =
        new SqlitePrinter(prepare)

object OracleDialect extends Dialect:
    override def printer(prepare: Boolean): SqlPrinter =
        new OraclePrinter(prepare)

object MssqlDialect extends Dialect:
    override def printer(prepare: Boolean): SqlPrinter =
        new MssqlPrinter(prepare)

object DB2Dialect extends Dialect:
    override def printer(prepare: Boolean): SqlPrinter =
        new DB2Printer(prepare)