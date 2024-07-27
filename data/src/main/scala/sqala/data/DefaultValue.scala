package sqala.data

import scala.compiletime.{erasedValue, summonInline}
import scala.deriving.Mirror
import java.time.{LocalDate, LocalDateTime}
import java.util.Date

trait DefaultValue[T]:
    def defaultValue: T

object DefaultValue:
    inline def defaultValues[T <: Tuple]: List[Any] =
        inline erasedValue[T] match
            case _: EmptyTuple => Nil
            case _: (t *: ts) => summonInline[DefaultValue[t]].defaultValue :: defaultValues[ts]

    given intDefaultValue: DefaultValue[Int] with
        override def defaultValue: Int = 0

    given longDefaultValue: DefaultValue[Long] with
        override def defaultValue: Long = 0L

    given floatDefaultValue: DefaultValue[Float] with
        override def defaultValue: Float = 0F

    given doubleDefaultValue: DefaultValue[Double] with
        override def defaultValue: Double = 0D

    given decimalDefaultValue: DefaultValue[BigDecimal] with
        override def defaultValue: BigDecimal = BigDecimal("0")

    given stringDefaultValue: DefaultValue[String] with
        override def defaultValue: String = ""

    given booleanDefaultValue: DefaultValue[Boolean] with
        override def defaultValue: Boolean = false

    given dateDefaultValue: DefaultValue[Date] with
        override def defaultValue: Date = new Date

    given localDateDefaultValue: DefaultValue[LocalDate] with
        override def defaultValue: LocalDate = LocalDate.now()

    given localDateTimeDefaultValue: DefaultValue[LocalDateTime] with
        override def defaultValue: LocalDateTime = LocalDateTime.now()

    given optionDefaultValue[T: DefaultValue]: DefaultValue[Option[T]] with
        override def defaultValue: Option[T] = None

    given listDefaultValue[T: DefaultValue]: DefaultValue[List[T]] with
        override def defaultValue: List[T] = Nil

    private def newDefaultValueProduct[T](values: List[Any], m: Mirror.ProductOf[T]): DefaultValue[T] =
        new DefaultValue[T]:
            override def defaultValue: T =
                m.fromProduct(Tuple.fromArray(values.toArray))

    private def newDefaultValueSum[T](values: List[Any]): DefaultValue[T] =
        new DefaultValue[T]:
            override def defaultValue: T =
                values.head.asInstanceOf[T]

    inline given derived[T](using m: Mirror.Of[T]): DefaultValue[T] =
        val values = defaultValues[m.MirroredElemTypes]

        inline m match
            case p: Mirror.ProductOf[T] => newDefaultValueProduct[T](values, p)
            case s: Mirror.SumOf[T] => newDefaultValueSum[T](values)