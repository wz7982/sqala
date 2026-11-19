package bench

import slick.jdbc.GetResult
import slick.jdbc.PostgresProfile.api.*

import javax.sql.DataSource
import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
 * Slick using plain SQL queries and the shared HikariCP pool.
 * The result is decoded with a hand-written `GetResult`.
 */
final class SlickOps(dataSource: DataSource):

    private val database = Database.forDataSource(dataSource, Some(16))

    private given GetResult[BenchRow] = GetResult { result =>
        BenchRow(
            result.nextLong(),
            result.nextString(),
            result.nextString(),
            result.nextInt(),
            result.nextBoolean(),
            result.nextBigDecimal(),
            result.nextTimestamp().toLocalDateTime,
            result.nextStringOption(),
            result.nextStringOption(),
            result.nextDouble()
        )
    }

    def point(id: Long): BenchRow =
        val rows: Vector[BenchRow] = Await.result(
            database.run(
                sql"SELECT id, name, email, age, active, score, created_at, bio, city, balance FROM bench_user WHERE id = $id"
                    .as[BenchRow]
            ),
            Duration.Inf
        )
        rows.head

    def range(start: Long, end: Long): Seq[BenchRow] =
        val rows: Vector[BenchRow] = Await.result(
            database.run(
                sql"SELECT id, name, email, age, active, score, created_at, bio, city, balance FROM bench_user WHERE id >= $start AND id < $end"
                    .as[BenchRow]
            ),
            Duration.Inf
        )
        rows
