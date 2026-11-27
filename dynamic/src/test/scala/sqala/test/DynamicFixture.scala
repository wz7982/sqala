package sqala.test

import sqala.dialect.{H2Dialect, MysqlDialect, OracleDialect, PostgresqlDialect, SqliteDialect, SqlserverDialect}
import sqala.dynamic.dsl.*

object DynamicFixture:
    def lines(xs: String*): String =
        xs.mkString("\n")

    def pg(q: Query): String =
        q.sql(PostgresqlDialect, true)

    def pgLegacy(q: Query): String =
        q.sql(PostgresqlDialect, false)

    def mysql(q: Query): String =
        q.sql(MysqlDialect, false)

    def mysqlStandard(q: Query): String =
        q.sql(MysqlDialect, true)

    def oracle(q: Query): String =
        q.sql(OracleDialect, true)

    def sqlserver(q: Query): String =
        q.sql(SqlserverDialect, true)

    def sqlite(q: Query): String =
        q.sql(SqliteDialect, true)

    def h2(q: Query): String =
        q.sql(H2Dialect, true)
