package sqala.jdbc.dialect

import sqala.jdbc.printer.*
import sqala.metadata.Dialect

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