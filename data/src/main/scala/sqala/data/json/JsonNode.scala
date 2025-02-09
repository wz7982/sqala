package sqala.data.json

enum JsonNode:
    case Num(number: Number)
    case Str(string: String)
    case Bool(boolean: Boolean)
    case Null
    case Object(items: Map[String, JsonNode])
    case Array(items: List[JsonNode])

    def show: String =
        this match
            case Num(number) => number.toString
            case Str(string) => "\"" + string + "\""
            case Bool(boolean) => boolean.toString
            case Null => "null"
            case Object(items) => items.map((k, v) => s"\"$k\": ${v.show}").mkString("{", ", ", "}")
            case Array(items) => items.map(_.show).mkString("[",", ", "]")