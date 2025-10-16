package sqala.ast.quantifier

enum SqlQuantifier:
    case All
    case Distinct
    case Custom(quantifier: String)