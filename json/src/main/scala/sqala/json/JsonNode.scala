package sqala.json

enum JsonNode:
    case Num(number: Number)
    case Str(string: String)
    case Bool(boolean: Boolean)
    case Null
    case Object(items: Map[String, JsonNode])
    case Array(items: List[JsonNode])

    override def toString: String =
        this match
            case Num(number) => number.toString
            case Str(string) => "\"" + string + "\""
            case Bool(boolean) => boolean.toString
            case Null => "null"
            case Object(items) => items.map((k, v) => s"\"$k\": $v").mkString("{", ", ", "}")
            case Array(items) => items.mkString("[",", ", "]")