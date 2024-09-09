package sqala.data.json

case class JsonDateFormat(format: String)

extension [T: JsonEncoder](x: T)(using dateFormat: JsonDateFormat = JsonDateFormat("yyyy-MM-dd HH:mm:ss"))
    def toJson: String = summon[JsonEncoder[T]].encode(x).toString

def fromJson[T: JsonDecoder](jsonString: String)(using dateFormat: JsonDateFormat = JsonDateFormat("yyyy-MM-dd HH:mm:ss")): T =
    val parser = new JsonParser
    val jsonNode = parser.parse(jsonString)
    summon[JsonDecoder[T]].decode(jsonNode)

def parseJson(jsonString: String): JsonNode =
    val parser = new JsonParser
    parser.parse(jsonString)