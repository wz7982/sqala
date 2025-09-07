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

    given some[T: Number]: Number[Some[T]]()

    given none: Number[None.type]()

trait DateTime[T]

object DateTime:
    given localDate: DateTime[LocalDate]()

    given localDateTime: DateTime[LocalDateTime]()

    given localTime: DateTime[LocalTime]()

    given offsetDateTime: DateTime[OffsetDateTime]()

    given offsetTime: DateTime[LocalTime]()

    given option[T: DateTime]: DateTime[Option[T]]()

    given some[T: DateTime]: DateTime[Some[T]]()

    given none: DateTime[None.type]()

opaque type Json = String

object Json:
    def apply(value: String): Json = value

opaque type Interval = Long

opaque type Vector = String

object Vector:
    def apply(value: String): Vector = value