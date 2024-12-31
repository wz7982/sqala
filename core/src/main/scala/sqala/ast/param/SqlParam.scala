package sqala.ast.param

enum SqlParam(val param: String):
    case All extends SqlParam("ALL")
    case Distinct extends SqlParam("DISTINCT")
    case Custom(p: String) extends SqlParam(p)