package sqala.ast.quantifier

import sqala.ast.expr.SqlExpr

enum SqlQuantifier:
    case All
    case Distinct
    case Custom(words: List[String], exprs: List[SqlExpr])