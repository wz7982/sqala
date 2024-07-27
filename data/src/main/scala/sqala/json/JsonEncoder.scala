package sqala.json

import sqala.util.fetchNames

import scala.compiletime.{constValue, erasedValue, summonInline}
import scala.deriving.Mirror
import java.time.{LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter
import java.text.SimpleDateFormat
import java.util.Date

trait JsonEncoder[T]:
    def encode(x: T)(using JsonDateFormat): JsonNode

object JsonEncoder:
    inline def summonInstances[T <: Tuple]: List[JsonEncoder[?]] =
        inline erasedValue[T] match
            case _: EmptyTuple => Nil
            case _: (t *: ts) => summonInline[JsonEncoder[t]] :: summonInstances[ts]

    given intEncoder: JsonEncoder[Int] with
        override def encode(x: Int)(using JsonDateFormat): JsonNode = JsonNode.Num(x)

    given longEncoder: JsonEncoder[Long] with
        override def encode(x: Long)(using JsonDateFormat): JsonNode = JsonNode.Num(x)

    given floatEncoder: JsonEncoder[Float] with
        override def encode(x: Float)(using JsonDateFormat): JsonNode = JsonNode.Num(x)

    given doubleEncoder: JsonEncoder[Double] with
        override def encode(x: Double)(using JsonDateFormat): JsonNode = JsonNode.Num(x)

    given decimalEncoder: JsonEncoder[BigDecimal] with
        override def encode(x: BigDecimal)(using JsonDateFormat): JsonNode = JsonNode.Num(x)

    given stringEncoder: JsonEncoder[String] with
        override def encode(x: String)(using JsonDateFormat): JsonNode = JsonNode.Str(x)

    given booleanEncoder: JsonEncoder[Boolean] with
        override def encode(x: Boolean)(using JsonDateFormat): JsonNode = JsonNode.Bool(x)

    given dateEncoder: JsonEncoder[Date] with
        override def encode(x: Date)(using dataFormat: JsonDateFormat): JsonNode =
            val formatter = new SimpleDateFormat(dataFormat.format)
            JsonNode.Str(formatter.format(x))

    given localDateEncoder: JsonEncoder[LocalDate] with
        override def encode(x: LocalDate)(using dateFormat: JsonDateFormat): JsonNode =
            val formatter = DateTimeFormatter.ofPattern(dateFormat.format)
            JsonNode.Str(x.format(formatter))

    given localDateTimeEncoder: JsonEncoder[LocalDateTime] with
        override def encode(x: LocalDateTime)(using dateFormat: JsonDateFormat): JsonNode =
            val formatter = DateTimeFormatter.ofPattern(dateFormat.format)
            JsonNode.Str(x.format(formatter))

    given optionEncoder[T](using e: JsonEncoder[T]): JsonEncoder[Option[T]] with
        override def encode(x: Option[T])(using JsonDateFormat): JsonNode = x match
            case None => JsonNode.Null
            case Some(value) => e.encode(value)

    given listEncoder[T](using e: JsonEncoder[T]): JsonEncoder[List[T]] with
        override def encode(x: List[T])(using JsonDateFormat): JsonNode = JsonNode.Array(x.map(e.encode))

    private def newEncoderNamedTuple[N <: Tuple, V <: Tuple](names: List[String], instances: List[JsonEncoder[?]]): JsonEncoder[NamedTuple.NamedTuple[N, V]] =
        new JsonEncoder[NamedTuple.NamedTuple[N, V]]:
            override def encode(x: NamedTuple.NamedTuple[N, V])(using JsonDateFormat): JsonNode =
                val data = x.toTuple.productIterator.toList
                val nodes = data.zip(instances).map: (v, i) =>
                    i.asInstanceOf[JsonEncoder[Any]].encode(v)
                JsonNode.Object(names.zip(nodes).toMap)

    inline given namedTupleEncoder[N <: Tuple, V <: Tuple]: JsonEncoder[NamedTuple.NamedTuple[N, V]] =
        val names = fetchNames[N]
        val instances = summonInstances[V]
        newEncoderNamedTuple(names, instances)
    
    private def newEncoderProduct[T](names: List[String], metaData: JsonMetaData, instances: List[JsonEncoder[?]]): JsonEncoder[T] =
        val aliasNameMap = metaData.fieldNames
            .zip(metaData.aliasNames)
            .toMap
            .map((k, v) => k -> v.headOption.getOrElse(k))

        val ignoreMap = metaData.fieldNames
            .zip(metaData.ignore)
            .toMap

        new JsonEncoder[T]:
            override def encode(x: T)(using JsonDateFormat): JsonNode =
                val data = names
                    .zip(instances)
                    .zip(x.asInstanceOf[Product].productIterator)
                    .map(i => (i._1._1, i._1._2, i._2))
                    .filter((n, _, _) => !ignoreMap(n))
                    .map: (n, i, v) =>
                        val alias = aliasNameMap(n)
                        alias -> i.asInstanceOf[JsonEncoder[Any]].encode(v)
                JsonNode.Object(data.toMap)

    private def newEncoderSum[T](s: Mirror.SumOf[T], names: List[String], instances: List[JsonEncoder[?]]): JsonEncoder[T] =
        new JsonEncoder[T]:
            override def encode(x: T)(using JsonDateFormat): JsonNode =
                val ord = s.ordinal(x)
                x match
                    case p: Product if p.productArity == 0 => JsonNode.Str(names(ord))
                    case _ => 
                        val instance = instances(ord)
                        val node = instance.asInstanceOf[JsonEncoder[Any]].encode(x)
                        JsonNode.Object(Map(names(ord) -> node))
    
    inline given derived[T](using m: Mirror.Of[T]): JsonEncoder[T] =
        val names = fetchNames[m.MirroredElemLabels]
        val instances = summonInstances[m.MirroredElemTypes]

        inline m match
            case _: Mirror.ProductOf[T] =>
                val metaData = jsonMetaDataMacro[T]
                newEncoderProduct[T](names, metaData, instances)
            case s: Mirror.SumOf[T] =>
                newEncoderSum[T](s, names, instances)