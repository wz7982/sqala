package sqala.data.json

case class JsonDateFormat(format: String)

extension [T: JsonEncoder](x: T)(using 
    dateFormat: JsonDateFormat = JsonDateFormat("yyyy-MM-dd HH:mm:ss")
)
    def toJson: String = summon[JsonEncoder[T]].encode(x).toString

def fromJson[T: JsonDecoder](jsonString: String)(using 
    dateFormat: JsonDateFormat = JsonDateFormat("yyyy-MM-dd HH:mm:ss")
): T =
    val jsonNode = JsonParser.parse(jsonString)
    summon[JsonDecoder[T]].decode(jsonNode)

def parseJson(jsonString: String): JsonNode =
    JsonParser.parse(jsonString)