package sqala.ast.expr

enum SqlSubqueryQuantifier:
    case Any
    case All
    case Exists