package sqala.ast.expr

enum SqlTimeUnit(val unit: String):
    case Year extends SqlTimeUnit("YEAR")
    case Month extends SqlTimeUnit("MONTH")
    case Week extends SqlTimeUnit("WEEK")
    case Day extends SqlTimeUnit("DAY")
    case Hour extends SqlTimeUnit("HOUR")
    case Minute extends SqlTimeUnit("MINUTE")
    case Second extends SqlTimeUnit("SECOND")

enum SqlTimeLiteralUnit(val unit: String):
    case Timestamp extends SqlTimeLiteralUnit("TIMESTAMP")
    case Date extends SqlTimeLiteralUnit("DATE")
    case Time extends SqlTimeLiteralUnit("TIME")