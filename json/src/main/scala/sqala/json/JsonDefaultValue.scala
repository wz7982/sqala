package sqala.json

import scala.compiletime.{erasedValue, summonInline}
import scala.deriving.Mirror
import java.time.{LocalDate, LocalDateTime}
import java.util.Date

trait JsonDefaultValue[T]:
    def defaultValue: T

object JsonDefaultValue: 
    inline def defaultValues[T <: Tuple]: List[Any] =
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

    given dateDefaultValue: JsonDefaultValue[Date] with
        override def defaultValue: Date = new Date

    given localDateDefaultValue: JsonDefaultValue[LocalDate] with
        override def defaultValue: LocalDate = LocalDate.now()

    given localDateTimeDefaultValue: JsonDefaultValue[LocalDateTime] with
        override def defaultValue: LocalDateTime = LocalDateTime.now()

    given optionDefaultValue[T: JsonDefaultValue]: JsonDefaultValue[Option[T]] with
        override def defaultValue: Option[T] = None

    given listDefaultValue[T: JsonDefaultValue]: JsonDefaultValue[List[T]] with
        override def defaultValue: List[T] = Nil

    given tupleDefaultValue[H, T <: Tuple](using dh: JsonDefaultValue[H], dt: JsonDefaultValue[T]): JsonDefaultValue[H *: T] with
        override def defaultValue: H *: T = dh.defaultValue *: dt.defaultValue

    given emptyTupleDefaultValue: JsonDefaultValue[EmptyTuple] with
        override def defaultValue: EmptyTuple = EmptyTuple

    given namedTupleDefaultValue[N <: Tuple, V <: Tuple](using d: JsonDefaultValue[V]): JsonDefaultValue[NamedTuple.NamedTuple[N, V]] with
        override def defaultValue: NamedTuple.NamedTuple[N, V] = NamedTuple(d.defaultValue)

    private def newDefaultValueProduct[T](values: List[Any], m: Mirror.ProductOf[T]): JsonDefaultValue[T] =
        new JsonDefaultValue[T]:
            override def defaultValue: T = 
                m.fromProduct(Tuple.fromArray(values.toArray))
    
    private def newDefaultValueSum[T](values: List[Any]): JsonDefaultValue[T] =
        new JsonDefaultValue[T]:
            override def defaultValue: T = 
                values.head.asInstanceOf[T]

    inline given derived[T](using m: Mirror.Of[T]): JsonDefaultValue[T] =
        val values = defaultValues[m.MirroredElemTypes]

        inline m match
            case p: Mirror.ProductOf[T] => newDefaultValueProduct[T](values, p)
            case s: Mirror.SumOf[T] => newDefaultValueSum[T](values)