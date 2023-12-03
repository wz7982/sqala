package sqala.compiletime.statement

import sqala.jdbc.Dialect

trait ToSql:
    def sql(dialect: Dialect): (String, Array[Any])