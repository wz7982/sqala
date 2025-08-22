package sqala.ast.quantifier

enum SqlQuantifier(val quantifier: String):
    case All extends SqlQuantifier("ALL")
    case Distinct extends SqlQuantifier("DISTINCT")
    case Custom(override val quantifier: String) extends SqlQuantifier(quantifier)