package sqala.data.json

import scala.util.parsing.combinator.JavaTokenParsers

object JsonParser extends JavaTokenParsers:
    def value: Parser[JsonNode] =
        number |
        string |
        boolean |
        nil |
        obj |
        arr

    def number: Parser[JsonNode] =
        decimalNumber ^^ (i => JsonNode.Num(BigDecimal(i)))

    def string: Parser[JsonNode] =
        stringLiteral ^^ (i => JsonNode.Str(i.drop(1).dropRight(1)))

    def boolean: Parser[JsonNode] =
        "true" ^^ (_ => JsonNode.Bool(true)) |
        "false" ^^ (_ => JsonNode.Bool(false))

    def nil: Parser[JsonNode] =
        "null" ^^ (_ => JsonNode.Null)

    def attr: Parser[(String, JsonNode)] =
        string ~ ":" ~ value ^^ {
            case k ~ _ ~ v => k.asInstanceOf[JsonNode.Str].string -> v
        }

    def obj: Parser[JsonNode] =
        "{" ~> repsep(attr, ",") <~ "}" ^^ (items => JsonNode.Object(items.toMap))

    def arr: Parser[JsonNode] =
        "[" ~> repsep(value, ",") <~ "]" ^^ (items => JsonNode.Array(items))

    def parse(text: String): JsonNode = 
        parseAll(obj | arr, text) match
            case Success(result, _) => result
            case e => throw JsonDecodeException(e.toString)