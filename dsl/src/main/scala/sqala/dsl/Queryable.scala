package sqala.dsl

import sqala.ast.statement.SqlQuery
import sqala.printer.Dialect
import sqala.util.queryToString

trait Queryable[T]:
    val ast: SqlQuery

    def sql(dialect: Dialect, prepare: Boolean = true): (String, Array[Any]) =
        queryToString(ast, dialect, prepare)