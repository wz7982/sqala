package sqala.ast.expr

enum SqlCastType:
    case Varchar
    case Int4
    case Int8
    case Float4
    case Float8
    case DateTime
    case Json
    case Custom(castType: String)