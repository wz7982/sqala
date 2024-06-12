package sqala.json

import sqala.util.fetchNames

import scala.compiletime.{constValue, erasedValue, summonInline}
import scala.deriving.Mirror
import scala.language.experimental.saferExceptions

trait JsonDecoder[T]:
    def decode(node: JsonNode): T throws JsonDecodeException

object JsonDecoder:
    inline def summonInstances[T <: Tuple]: List[JsonDecoder[?]] =
        inline erasedValue[T] match
            case _: EmptyTuple => Nil
            case _: (t *: ts) => summonInline[JsonDecoder[t]] :: summonInstances[ts]

    given intDecoder: JsonDecoder[Int] with
        override def decode(node: JsonNode): Int throws JsonDecodeException =
            node match
                case JsonNode.NumberLiteral(number) => number.intValue
                case _ => throw new JsonDecodeException

    given longDecoder: JsonDecoder[Long] with
        override def decode(node: JsonNode): Long throws JsonDecodeException =
            node match
                case JsonNode.NumberLiteral(number) => number.longValue
                case _ => throw new JsonDecodeException

    given floatDecoder: JsonDecoder[Float] with
        override def decode(node: JsonNode): Float throws JsonDecodeException = 
            node match
                case JsonNode.NumberLiteral(number) => number.floatValue
                case _ => throw new JsonDecodeException

    given doubleDecoder: JsonDecoder[Double] with
        override def decode(node: JsonNode): Double throws JsonDecodeException =
            node match
                case JsonNode.NumberLiteral(number) => number.doubleValue
                case _ => throw new JsonDecodeException

    given decimalDecoder: JsonDecoder[BigDecimal] with
        override def decode(node: JsonNode): BigDecimal throws JsonDecodeException =
            node match
                case JsonNode.NumberLiteral(number) => BigDecimal(number.toString)
                case _ => throw new JsonDecodeException

    given stringDecoder: JsonDecoder[String] with
        override def decode(node: JsonNode): String throws JsonDecodeException =
            node match
                case JsonNode.StringLiteral(string) => string
                case _ => throw new JsonDecodeException

    given booleanDecoder: JsonDecoder[Boolean] with
        override def decode(node: JsonNode): Boolean throws JsonDecodeException =
            node match
                case JsonNode.BooleanLiteral(boolean) => boolean
                case _ => throw new JsonDecodeException

    given optionDecoder[T](using d: JsonDecoder[T]): JsonDecoder[Option[T]] with
        override def decode(node: JsonNode): Option[T] throws JsonDecodeException =
            node match
                case JsonNode.NullLiteral => None
                case n => Some(d.decode(n))

    given listDecoder[T](using d: JsonDecoder[T]): JsonDecoder[List[T]] with
        override def decode(node: JsonNode): List[T] throws JsonDecodeException = 
            node match
                case JsonNode.Vector(items) => items.map(i => d.decode(i))
                case _ => throw new JsonDecodeException

    private def newDecoder[T <: Product](names: List[String], instances: List[JsonDecoder[?]], typedDefaultValues: Map[String, Any], metaData: JsonMetaData)(using m: Mirror.ProductOf[T]): JsonDecoder[T] =
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
            override def decode(node: JsonNode): T throws JsonDecodeException =
                node match
                    case JsonNode.Object(items) => 
                        val data = info.map: (name, instance) =>
                            val alias = aliasNameMap(name)
                            (items.contains(alias), ignoreMap(name)) match
                                case (true, _) => instance.decode(items(alias))
                                case (false, true) if defaultValueMap(name) ne None => defaultValueMap(name).get
                                case (false, true) => typedDefaultValues(name)
                                case (false, false) => throw new JsonDecodeException(s"The JSON does not contain a value for field '$name', consider adding an annotation @jsonIgnore or checking the JSON.")
                        m.fromProduct(Tuple.fromArray(data.toArray))
                    case _ => throw new JsonDecodeException

    inline given derived[T <: Product](using m: Mirror.ProductOf[T]): JsonDecoder[T] =
        val names = fetchNames[m.MirroredElemLabels]
        val instances = summonInstances[m.MirroredElemTypes]
        val defaultValues = names.zip(JsonDefaultValue.defaultValues[m.MirroredElemTypes]).toMap
        val metaData = jsonMetaDataMacro[T]
        newDecoder[T](names, instances, defaultValues, metaData)