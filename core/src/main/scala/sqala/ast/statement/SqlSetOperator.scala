package sqala.ast.statement

enum SqlSetOperator(val operator: String):
    case Union extends SqlSetOperator("UNION")
    case UnionAll extends SqlSetOperator("UNION ALL")
    case Except extends SqlSetOperator("EXCEPT")
    case ExceptAll extends SqlSetOperator("EXCEPT ALL")
    case Intersect extends SqlSetOperator("INTERSECT")
    case IntersectAll extends SqlSetOperator("INTERSECT ALL")