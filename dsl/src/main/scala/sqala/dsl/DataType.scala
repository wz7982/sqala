package sqala.dsl

import java.time.{LocalDate, LocalDateTime}
import java.util.Date
import scala.annotation.implicitNotFound

@implicitNotFound("Expr must be a numeric type")
trait Number[T]

object Number:
    given intNumber: Number[Int]()

    given longNumber: Number[Long]()

    given floatNumber: Number[Float]()

    given doubleNumber: Number[Double]()

    given decimalNumber: Number[BigDecimal]()

    given optionNumber[T: Number]: Number[Option[T]]()

@implicitNotFound("Expr must be a time type")
trait DateTime[T]

object DateTime:
    given dateDateTime: DateTime[Date]()

    given localDateDateTime: DateTime[LocalDate]()

    given localDateTimeDateTime: DateTime[LocalDateTime]()

    given optionDateTime[T: DateTime]: DateTime[Option[T]]()

opaque type Json = String

object Json:
    def apply(value: String): Json = value

    extension (x: Json)
        def toString: String = x