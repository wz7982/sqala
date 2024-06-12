package sqala.json

import scala.language.experimental.saferExceptions

trait JosnProcessor[T] extends JsonEncoder[T] with JsonDecoder[T]

extension [T: JsonEncoder](x: T)
    def toJson: String = summon[JsonEncoder[T]].encode(x).toString

def fromJson[T: JsonDecoder](jsonString: String): T throws JsonDecodeException =
    val parser = new JsonParser
    val jsonNode = parser.parse(jsonString)
    summon[JsonDecoder[T]].decode(jsonNode)