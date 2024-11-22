package sqala.dsl

import java.time.{LocalDate, LocalDateTime}
import java.util.Date
import scala.annotation.implicitNotFound

@implicitNotFound("Expr must be a numeric type.")
trait Numeric[-T]

object Numeric:
    given intNumeric: Numeric[Int]()

    given longNumeric: Numeric[Long]()

    given floatNumeric: Numeric[Float]()

    given doubleNumeric: Numeric[Double]()

    given decimalNumeric: Numeric[BigDecimal]()

    given optionNumeric[T: Numeric]: Numeric[Option[T]]()

@implicitNotFound("Expr must be a time type.")
trait DateTime[-T]

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