package sqala.dsl

trait Number[T]

object Number:
    given intNumber: Number[Int]()

    given longNumber: Number[Long]()

    given floatNumber: Number[Float]()

    given doubleNumber: Number[Double]()

    given decimalNumber: Number[BigDecimal]()

    given optionNumber[T: Number]: Number[Option[T]]()