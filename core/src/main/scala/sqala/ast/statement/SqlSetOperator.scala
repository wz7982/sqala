package sqala.ast.statement

import sqala.ast.quantifier.SqlQuantifier

enum SqlSetOperator(val operator: String, val quantifier: Option[SqlQuantifier]):
    case Union(override val quantifier: Option[SqlQuantifier]) extends SqlSetOperator("UNION", quantifier)
    case Except(override val quantifier: Option[SqlQuantifier]) extends SqlSetOperator("EXCEPT", quantifier)
    case Intersect(override val quantifier: Option[SqlQuantifier]) extends SqlSetOperator("INTERSECT", quantifier)