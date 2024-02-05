package sqala.ast.expr

enum SqlSubQueryPredicate(val predicate: String):
    case Some extends SqlSubQueryPredicate("SOME")
    case Any extends SqlSubQueryPredicate("ANY")
    case All extends SqlSubQueryPredicate("ALL")
    case Exists extends SqlSubQueryPredicate("EXISTS")
    case NotExists extends SqlSubQueryPredicate("NOT EXISTS")