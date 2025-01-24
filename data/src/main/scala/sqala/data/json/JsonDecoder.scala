package sqala.data.json

import sqala.data.DefaultValue
import sqala.data.util.fetchNames

import scala.compiletime.{constValue, erasedValue, summonInline}
import scala.deriving.Mirror
import java.time.{LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter

trait JsonDecoder[T]:
    def decode(node: JsonNode)(using JsonDateFormat): T

object JsonDecoder:
    inline def summonInstances[T <: Tuple]: List[JsonDecoder[?]] =
        inline erasedValue[T] match
            case _: EmptyTuple => Nil
            case _: (t *: ts) => summonInline[JsonDecoder[t]] :: summonInstances[ts]

    given intDecoder: JsonDecoder[Int] with
        override def decode(node: JsonNode)(using JsonDateFormat): Int =
            node match
                case JsonNode.Num(number) => number.intValue
                case _ => throw new JsonDecodeException

    given longDecoder: JsonDecoder[Long] with
        override def decode(node: JsonNode)(using JsonDateFormat): Long =
            node match
                case JsonNode.Num(number) => number.longValue
                case _ => throw new JsonDecodeException

    given floatDecoder: JsonDecoder[Float] with
        override def decode(node: JsonNode)(using JsonDateFormat): Float =
            node match
                case JsonNode.Num(number) => number.floatValue
                case _ => throw new JsonDecodeException

    given doubleDecoder: JsonDecoder[Double] with
        override def decode(node: JsonNode)(using JsonDateFormat): Double =
            node match
                case JsonNode.Num(number) => number.doubleValue
                case _ => throw new JsonDecodeException

    given decimalDecoder: JsonDecoder[BigDecimal] with
        override def decode(node: JsonNode)(using JsonDateFormat): BigDecimal =
            node match
                case JsonNode.Num(number) => BigDecimal(number.toString)
                case _ => throw new JsonDecodeException

    given stringDecoder: JsonDecoder[String] with
        override def decode(node: JsonNode)(using JsonDateFormat): String =
            node match
                case JsonNode.Str(string) => string
                case _ => throw new JsonDecodeException

    given booleanDecoder: JsonDecoder[Boolean] with
        override def decode(node: JsonNode)(using JsonDateFormat): Boolean =
            node match
                case JsonNode.Bool(boolean) => boolean
                case _ => throw new JsonDecodeException

    given localDateDecoder: JsonDecoder[LocalDate] with
        override def decode(node: JsonNode)(using dateFormat: JsonDateFormat): LocalDate =
            node match
                case JsonNode.Str(string) =>
                    val formatter = DateTimeFormatter.ofPattern(dateFormat.format)
                    LocalDateTime.parse(string, formatter).toLocalDate()
                case _ => throw new JsonDecodeException

    given localDateTimeDecoder: JsonDecoder[LocalDateTime] with
        override def decode(node: JsonNode)(using dateFormat: JsonDateFormat): LocalDateTime =
            node match
                case JsonNode.Str(string) =>
                    val formatter = DateTimeFormatter.ofPattern(dateFormat.format)
                    LocalDateTime.parse(string, formatter)
                case _ => throw new JsonDecodeException

    given optionDecoder[T](using d: JsonDecoder[T]): JsonDecoder[Option[T]] with
        override def decode(node: JsonNode)(using JsonDateFormat): Option[T] =
            node match
                case JsonNode.Null => None
                case n => Some(d.decode(n))

    given listDecoder[T](using d: JsonDecoder[T]): JsonDecoder[List[T]] with
        override def decode(node: JsonNode)(using JsonDateFormat): List[T] =
            node match
                case JsonNode.Array(items) => items.map(i => d.decode(i))
                case _ => throw new JsonDecodeException

    private def newDecoderProduct[T](names: List[String], instances: List[JsonDecoder[?]], typedDefaultValues: Map[String, Any], metaData: JsonMetaData)(using m: Mirror.ProductOf[T]): JsonDecoder[T] =
        val info = names.zip(instances)

        val aliasNameMap = metaData.fieldNames
            .zip(metaData.aliasNames)
            .toMap
            .map((k, v) => k -> v.headOption.getOrElse(k))

        val defaultValueMap = metaData.fieldNames
            .zip(metaData.defaultValues)
            .toMap

        val ignoreMap = metaData.fieldNames
            .zip(metaData.ignore)
            .toMap

        new JsonDecoder[T]:
            override def decode(node: JsonNode)(using JsonDateFormat): T =
                node match
                    case JsonNode.Object(items) =>
                        val data = info.map: (name, instance) =>
                            val alias = aliasNameMap(name)
                            (items.contains(alias), ignoreMap(name)) match
                                case (true, _) =>
                                    try
                                        instance.decode(items(alias))
                                    catch
                                        case e: JsonDecodeException =>
                                            throw new JsonDecodeException(s"The JSON cannot be mapped to field '$name'.")
                                case (false, true) if defaultValueMap(name) ne None => defaultValueMap(name).get
                                case (false, true) => typedDefaultValues(name)
                                case (false, false) =>
                                    throw new JsonDecodeException(s"The JSON does not contain a value for field '$name', consider adding an annotation @jsonIgnore or checking the JSON.")
                        m.fromProduct(Tuple.fromArray(data.toArray))
                    case _ => throw new JsonDecodeException

    private def newDecoderSum[T](names: List[String], instances: List[JsonDecoder[?]], metaData: List[JsonMetaData]): JsonDecoder[T] =
        new JsonDecoder[T]:
            override def decode(node: JsonNode)(using JsonDateFormat): T =
                node match
                    case JsonNode.Str(string) =>
                        if !names.contains(string) then
                            throw new JsonDecodeException
                        val index = names.indexOf(string)
                        if metaData(index).fieldNames.isEmpty then
                            instances(index).asInstanceOf[JsonDecoder[T]].decode(JsonNode.Object(Map()))
                        else throw new JsonDecodeException
                    case JsonNode.Object(items) =>
                        val (name, node) = items.head
                        if !names.contains(name) then
                            throw new JsonDecodeException
                        val index = names.indexOf(name)
                        instances(index).asInstanceOf[JsonDecoder[T]].decode(node)
                    case _ => throw new JsonDecodeException

    inline given derived[T](using m: Mirror.Of[T]): JsonDecoder[T] =
        val names = fetchNames[m.MirroredElemLabels]
        val instances = summonInstances[m.MirroredElemTypes]
        val metaData = fetchMetaData[m.MirroredElemTypes]

        inline m match
            case p: Mirror.ProductOf[T] =>
                val defaultValues = names
                    .zip(DefaultValue.defaultValues[m.MirroredElemTypes])
                    .toMap
                val metaData = jsonMetaDataMacro[T]
                newDecoderProduct[T](names, instances, defaultValues, metaData)(using p)
            case _: Mirror.SumOf[T] => newDecoderSum[T](names, instances, metaData)