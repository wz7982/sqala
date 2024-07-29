package sqala.json

import scala.language.experimental.saferExceptions

case class JsonDateFormat(format: String)

extension [T: JsonEncoder](x: T)(using dateFormat: JsonDateFormat = JsonDateFormat("yyyy-MM-dd HH:mm:ss"))
    def toJson: String = summon[JsonEncoder[T]].encode(x)

def fromJson[T: JsonDecoder](jsonString: String)(using dateFormat: JsonDateFormat = JsonDateFormat("yyyy-MM-dd HH:mm:ss")): T throws JsonDecodeException =
    val parser = new JsonParser
    val jsonNode = parser.parse(jsonString)
    summon[JsonDecoder[T]].decode(jsonNode)

def parseJson(jsonString: String): JsonNode throws JsonDecodeException =
    val parser = new JsonParser
    parser.parse(jsonString)