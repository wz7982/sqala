package sqala.static.statement.dml

import sqala.ast.statement.SqlStatement
import sqala.printer.Dialect
import sqala.util.statementToString

class Dml(val ast: SqlStatement):
    def sql(dialect: Dialect, prepare: Boolean = true): (String, Array[Any]) =
        statementToString(ast, dialect, prepare)