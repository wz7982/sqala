package sqala.test

import sqala.printer.{PostgresqlPrinter, StandardSqlPrinter}

/** Standard SQL output, with no dialect overrides. Used by the AST-level suites. */
def createSql(f: StandardSqlPrinter => Unit): String =
    val printer = new StandardSqlPrinter:
        val standardEscapeStrings = true
    f(printer)
    printer.sql

/** PostgreSQL dialect output. Used by the dialect suites. */
def createPgSql(f: StandardSqlPrinter => Unit): String =
    val printer = new PostgresqlPrinter(true)
    f(printer)
    printer.sql