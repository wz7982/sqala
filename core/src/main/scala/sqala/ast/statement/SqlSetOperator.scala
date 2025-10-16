package sqala.ast.statement

import sqala.ast.quantifier.SqlQuantifier

enum SqlSetOperator(val quantifier: Option[SqlQuantifier]):
    case Union(override val quantifier: Option[SqlQuantifier]) extends SqlSetOperator(quantifier)
    case Except(override val quantifier: Option[SqlQuantifier]) extends SqlSetOperator(quantifier)
    case Intersect(override val quantifier: Option[SqlQuantifier]) extends SqlSetOperator(quantifier)