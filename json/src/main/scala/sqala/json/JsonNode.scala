package sqala.json

enum JsonNode:
    case NumberLiteral(number: Number)
    case StringLiteral(string: String)
    case BooleanLiteral(boolean: Boolean)
    case NullLiteral
    case Object(items: Map[String, JsonNode])
    case Vector(items: List[JsonNode])

    override def toString: String =
        this match
            case NumberLiteral(number) => number.toString
            case StringLiteral(string) => "\"" + string + "\""
            case BooleanLiteral(boolean) => boolean.toString
            case NullLiteral => "null"
            case Object(items) => items.map((k, v) => s"\"$k\": $v").mkString("{", ", ", "}")
            case Vector(items) => items.mkString("[",", ", "]")