package sqala.ast.expr

enum SqlTimeUnit:
    case Year
    case Month
    case Day
    case Hour
    case Minute
    case Second
    case Custom(unit: String)

enum SqlIntervalField:
    case To(start: SqlTimeUnit, end: SqlTimeUnit)
    case Single(unit: SqlTimeUnit)

enum SqlTimeLiteralUnit:
    case Timestamp
    case Date
    case Time