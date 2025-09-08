package sqala.metadata

import java.time.*

trait Number[T]

object Number:
    given int: Number[Int]()

    given long: Number[Long]()

    given float: Number[Float]()

    given double: Number[Double]()

    given decimal: Number[BigDecimal]()

    given option[T: Number]: Number[Option[T]]()

trait DateTime[T]

object DateTime:
    given localDate: DateTime[LocalDate]()

    given localDateTime: DateTime[LocalDateTime]()

    given offsetDateTime: DateTime[OffsetDateTime]()

    given option[T: DateTime]: DateTime[Option[T]]()

opaque type Json = String

object Json:
    def apply(value: String): Json = value

opaque type Interval = Long

opaque type Vector = String

object Vector:
    def apply(value: String): Vector = value