package sqala.jdbc.dialect

import sqala.dialect.Dialect
import sqala.jdbc.printer.*

/**
 * A JDBC database dialect.
 */
trait JdbcDialect extends Dialect:
    def printer(standardEscapeStrings: Boolean): JdbcSqlPrinter

/**
 * MySQL dialect.
 */
object MysqlJdbcDialect extends JdbcDialect:
    def printer(standardEscapeStrings: Boolean): JdbcSqlPrinter =
        new MysqlJdbcPrinter(standardEscapeStrings)

/**
 * PostgreSQL dialect.
 */
object PostgresqlJdbcDialect extends JdbcDialect:
    def printer(standardEscapeStrings: Boolean): JdbcSqlPrinter =
        new PostgresqlJdbcPrinter(standardEscapeStrings)

/**
 * Oracle dialect.
 */
object OracleJdbcDialect extends JdbcDialect:
    def printer(standardEscapeStrings: Boolean): JdbcSqlPrinter =
        new OracleJdbcPrinter(standardEscapeStrings)