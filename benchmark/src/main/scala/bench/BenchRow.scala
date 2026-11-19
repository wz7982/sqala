package bench

import sqala.metadata.{primaryKey, table}

import java.time.LocalDateTime

/**
 * Row type for the Scala based frameworks (sqala and Slick).
 *
 * The default name conversion maps `BenchRow` to `bench_row`, so the
 * table name is overridden explicitly.
 */
@table("bench_user")
case class BenchRow(
    @primaryKey id: Long,
    name: String,
    email: String,
    age: Int,
    active: Boolean,
    score: BigDecimal,
    createdAt: LocalDateTime,
    bio: Option[String],
    city: Option[String],
    balance: Double
)
