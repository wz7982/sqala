package sqala.json

import scala.deriving.Mirror
import scala.language.experimental.saferExceptions

trait JsonObject[T] extends JsonEncoder[T] with JsonDecoder[T] with JsonDefaultValue[T]

object JsonObject:
    private def newInstance[T](encoder: JsonEncoder[T], decoder: JsonDecoder[T], value: JsonDefaultValue[T]): JsonObject[T] =
        new JsonObject[T]:
            override def encode(value: T)(using JsonDateFormat): JsonNode = encoder.encode(value)

            override def decode(node: JsonNode)(using JsonDateFormat): T throws JsonDecodeException = decoder.decode(node)

            override def defaultValue: T = value.defaultValue

    inline given derived[T](using m: Mirror.Of[T]): JsonObject[T] =
        val encoder = JsonEncoder.derived[T]
        val decoder = JsonDecoder.derived[T]
        val value = JsonDefaultValue.derived[T]
        newInstance[T](encoder, decoder, value)

trait JsonStateEnum[T] extends JsonEncoder[T] with JsonDecoder[T] with JsonDefaultValue[T]:
    def toJson(x: T): JsonNode

    def fromJson(node: JsonNode): T

    override def encode(x: T)(using JsonDateFormat): JsonNode = toJson(x)

    override def decode(node: JsonNode)(using JsonDateFormat): T throws JsonDecodeException = fromJson(node)

case class JsonDateFormat(format: String)

extension [T: JsonEncoder](x: T)(using dateFormat: JsonDateFormat = JsonDateFormat("yyyy-MM-dd HH:mm:ss"))
    def toJson: String = summon[JsonEncoder[T]].encode(x).toString

def fromJson[T: JsonDecoder](jsonString: String)(using dateFormat: JsonDateFormat = JsonDateFormat("yyyy-MM-dd HH:mm:ss")): T throws JsonDecodeException =
    val parser = new JsonParser
    val jsonNode = parser.parse(jsonString)
    summon[JsonDecoder[T]].decode(jsonNode)