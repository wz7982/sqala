package sqala.ast.param

enum SqlParam(val param: String):
    case All extends SqlParam("ALL")
    case Distinct extends SqlParam("DISTINCT")
    case Custom(override val param: String) extends SqlParam(param)