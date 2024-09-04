package sqala.ast.expr

enum SqlWindowFrameOption(val showString: String):
    case CurrentRow extends SqlWindowFrameOption("CURRENT ROW")
    case UnboundedPreceding extends SqlWindowFrameOption("UNBOUNDED PRECEDING")
    case Preceding(n: Int) extends SqlWindowFrameOption(s"$n PRECEDING")
    case UnboundedFollowing extends SqlWindowFrameOption("UNBOUNDED FOLLOWING")
    case Following(n: Int) extends SqlWindowFrameOption(s"$n FOLLOWING")

enum SqlWindowFrame:
    case Rows(start: SqlWindowFrameOption, end: SqlWindowFrameOption)
    case Range(start: SqlWindowFrameOption, end: SqlWindowFrameOption)
    case Groups(start: SqlWindowFrameOption, end: SqlWindowFrameOption)