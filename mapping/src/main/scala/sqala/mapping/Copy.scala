package sqala.mapping

import java.time.{LocalDate, LocalDateTime}
import java.util.Date

trait Copy[T]:
    def copy(x: T): T

object Copy:
    given inyCopy: Copy[Int] with
        override def copy(x: Int): Int = x

    given longCopy: Copy[Long] with
        override def copy(x: Long): Long = x

    given floatCopy: Copy[Float] with
        override def copy(x: Float): Float = x

    given doubleCopy: Copy[Double] with
        override def copy(x: Double): Double = x

    given decimalCopy: Copy[BigDecimal] with
        override def copy(x: BigDecimal): BigDecimal = x

    given stringCopy: Copy[String] with
        override def copy(x: String): String = x

    given booleanCopy: Copy[Boolean] with
        override def copy(x: Boolean): Boolean = x

    given dateCopy: Copy[Date] with
        override def copy(x: Date): Date = new Date(x.getTime)

    given localDateCopy: Copy[LocalDate] with
        override def copy(x: LocalDate): LocalDate = LocalDate.from(x)

    given localDateTimeCopy: Copy[LocalDateTime] with
        override def copy(x: LocalDateTime): LocalDateTime = LocalDateTime.from(x)

    given optionCopy[T](using c: Copy[T]): Copy[Option[T]] with
        override def copy(x: Option[T]): Option[T] = x.map(i => c.copy(i))

    given listCopy[T](using c: Copy[T]): Copy[List[T]] with
        override def copy(x: List[T]): List[T] = x.map(i => c.copy(i))