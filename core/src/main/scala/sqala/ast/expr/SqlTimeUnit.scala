package sqala.ast.expr

enum SqlTimeUnit(val unit: String):
    case Year extends SqlTimeUnit("YEAR")
    case Month extends SqlTimeUnit("MONTH")
    case Week extends SqlTimeUnit("WEEK")
    case Day extends SqlTimeUnit("DAY")
    case Hour extends SqlTimeUnit("HOUR")
    case Minute extends SqlTimeUnit("MINUTE")
    case Second extends SqlTimeUnit("SECOND")