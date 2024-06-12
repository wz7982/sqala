package sqala.json

enum JsonNode:
    case NumberLiteral(number: Number)
    case StringLiteral(string: String)
    case BooleanLiteral(boolean: Boolean)
    case NullLiteral
    case Object(items: Map[String, JsonNode])
    case Vector(items: List[JsonNode])