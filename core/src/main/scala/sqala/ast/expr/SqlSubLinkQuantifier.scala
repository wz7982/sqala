package sqala.ast.expr

enum SqlSubLinkQuantifier(val quantifier: String):
    case Some extends SqlSubLinkQuantifier("SOME")
    case Any extends SqlSubLinkQuantifier("ANY")
    case All extends SqlSubLinkQuantifier("ALL")
    case Exists extends SqlSubLinkQuantifier("EXISTS")