package sqala.ast.statement

enum SqlSelectParam(val param: String):
    case All extends SqlSelectParam("ALL")
    case Distinct extends SqlSelectParam("DISTINCT")
    case Custom(p: String) extends SqlSelectParam(p)