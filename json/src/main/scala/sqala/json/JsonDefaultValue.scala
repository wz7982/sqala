package sqala.json

import scala.compiletime.{erasedValue, summonInline}

trait JsonDefaultValue[T]:
    def defaultValue: T

object JsonDefaultValue: 
    // TODO summon
    inline def defaultValues[T]: List[Any] =
        inline erasedValue[T] match
            case _: EmptyTuple => Nil
            case _: (t *: ts) => summonInline[JsonDefaultValue[t]].defaultValue :: defaultValues[ts]

    given intDefaultValue: JsonDefaultValue[Int] with
        override def defaultValue: Int = 0

    given longDefaultValue: JsonDefaultValue[Long] with
        override def defaultValue: Long = 0L

    given floatDefaultValue: JsonDefaultValue[Float] with
        override def defaultValue: Float = 0F

    given doubleDefaultValue: JsonDefaultValue[Double] with
        override def defaultValue: Double = 0D

    given decimalDefaultValue: JsonDefaultValue[BigDecimal] with
        override def defaultValue: BigDecimal = BigDecimal("0")

    given stringDefaultValue: JsonDefaultValue[String] with
        override def defaultValue: String = ""

    given booleanDefaultValue: JsonDefaultValue[Boolean] with
        override def defaultValue: Boolean = false

    given optionDefaultValue[T: JsonDefaultValue]: JsonDefaultValue[Option[T]] with
        override def defaultValue: Option[T] = None

    given listDefaultValue[T: JsonDefaultValue]: JsonDefaultValue[List[T]] with
        override def defaultValue: List[T] = Nil