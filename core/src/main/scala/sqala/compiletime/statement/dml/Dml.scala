package sqala.compiletime.statement.dml

import sqala.ast.statement.SqlStatement
import sqala.compiletime.statement.ToSql
import sqala.jdbc.Dialect

trait Dml extends ToSql:
    def ast: SqlStatement

    override def sql(dialect: Dialect): (String, Array[Any]) =
        val printer = dialect.printer
        printer.printStatement(ast)
        printer.sql -> printer.args.toArray