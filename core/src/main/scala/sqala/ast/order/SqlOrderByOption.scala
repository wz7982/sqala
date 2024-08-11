package sqala.ast.order

enum SqlOrderByOption(val order: String):
    case Asc extends SqlOrderByOption("ASC")
    case Desc extends SqlOrderByOption("DESC")

enum SqlOrderByNullsOption(val order: String):
    case First extends SqlOrderByNullsOption("NULLS FIRST")
    case Last extends SqlOrderByNullsOption("NULLS LAST")
