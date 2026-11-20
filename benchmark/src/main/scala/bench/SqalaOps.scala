package bench

import sqala.jdbc.{JdbcContext, Logger}
import sqala.jdbc.dialect.PostgresqlJdbcDialect
import sqala.static.dsl.*

import javax.sql.DataSource

/**
 * sqala using the shared HikariCP pool. The point lookup uses the
 * primary-key API; the range query is built with the typed DSL.
 */
final class SqalaOps(dataSource: DataSource):

    private given Logger = Logger.noop

    private val db = JdbcContext(dataSource, PostgresqlJdbcDialect, false)

    def point(id: Long): Option[BenchRow] =
        db.findByPrimaryKey[BenchRow](id)

    def range(start: Long, end: Long): List[BenchRow] =
        db.fetch(from(BenchRow).filter(u => u.id >= start && u.id < end))