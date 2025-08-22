package sqala.ast.expr

enum SqlWindowFrameBound(val bound: String):
    case CurrentRow extends SqlWindowFrameBound("CURRENT ROW")
    case UnboundedPreceding extends SqlWindowFrameBound("UNBOUNDED PRECEDING")
    case Preceding(n: Int) extends SqlWindowFrameBound(s"$n PRECEDING")
    case UnboundedFollowing extends SqlWindowFrameBound("UNBOUNDED FOLLOWING")
    case Following(n: Int) extends SqlWindowFrameBound(s"$n FOLLOWING")

enum SqlWindowFrameUnit(val unit: String):
    case Rows extends SqlWindowFrameUnit("ROWS")
    case Range extends SqlWindowFrameUnit("RANGE")
    case Groups extends SqlWindowFrameUnit("GROUPS")

enum SqlWindowFrame:
    case Start(unit: SqlWindowFrameUnit, start: SqlWindowFrameBound)
    case Between(unit: SqlWindowFrameUnit, start: SqlWindowFrameBound, end: SqlWindowFrameBound)