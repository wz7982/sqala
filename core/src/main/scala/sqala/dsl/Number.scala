package sqala.dsl

trait Number[T]

object Number:
    given intNumber: Number[Int] with {}

    given longNumber: Number[Long] with {}

    given floatNumber: Number[Float] with {}

    given doubleNumber: Number[Double] with {}

    given decimalNumber: Number[BigDecimal] with {}

    given optionNumber[T: Number]: Number[Option[T]] with {}