package sqala.dsl

import java.time.{LocalDate, LocalDateTime}
import java.util.Date

trait Number[T]

object Number:
    given intNumber: Number[Int]()

    given longNumber: Number[Long]()

    given floatNumber: Number[Float]()

    given doubleNumber: Number[Double]()

    given decimalNumber: Number[BigDecimal]()

    given optionNumber[T: Number]: Number[Option[T]]()

trait DateTime[T]

object DateTime:
    given dateDateTime: DateTime[Date]()

    given localDateDateTime: DateTime[LocalDate]()

    given localDateTimeDateTime: DateTime[LocalDateTime]()

    given optionDateTime[T: DateTime]: DateTime[Option[T]]()