package sqala.ast.order

enum SqlOrderByOption(val order: String):
    case Asc extends SqlOrderByOption("ASC")
    case Desc extends SqlOrderByOption("DESC")