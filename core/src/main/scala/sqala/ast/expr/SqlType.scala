package sqala.ast.expr

enum SqlType:
    case Varchar(maxLength: Option[Int])
    case Int
    case Long
    case Float
    case Double
    case Decimal(precision: Option[(Int, Int)])
    case Date
    case Timestamp(mode: Option[SqlTimeZoneMode])
    case Time(mode: Option[SqlTimeZoneMode])
    case Json
    case Boolean
    case Vector
    case Array(`type`: SqlType)
    case Custom(`type`: String)

enum SqlTimeZoneMode(val mode: String):
    case With extends SqlTimeZoneMode("WITH TIME ZONE")
    case Without extends SqlTimeZoneMode("WITHOUT TIME ZONE")