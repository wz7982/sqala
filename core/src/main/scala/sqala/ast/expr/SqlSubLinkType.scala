package sqala.ast.expr

enum SqlSubLinkType(val linkType: String):
    case Some extends SqlSubLinkType("SOME")
    case Any extends SqlSubLinkType("ANY")
    case All extends SqlSubLinkType("ALL")
    case Exists extends SqlSubLinkType("EXISTS")
    case NotExists extends SqlSubLinkType("NOT EXISTS")