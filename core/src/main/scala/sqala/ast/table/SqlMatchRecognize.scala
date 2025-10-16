package sqala.ast.table

import sqala.ast.expr.SqlExpr
import sqala.ast.order.SqlOrderingItem

case class SqlMatchRecognize(
    partitionBy: List[SqlExpr],
    orderBy: List[SqlOrderingItem],
    measures: List[SqlRecognizeMeasureItem],
    rowsPerMatch: Option[SqlRecognizePatternRowsPerMatchMode],
    rowPattern: SqlRowPattern,
    alias: Option[SqlTableAlias]
)

case class SqlRecognizeMeasureItem(expr: SqlExpr, alias: String)

enum SqlRecognizePatternRowsPerMatchMode:
    case OneRow
    case AllRows(emptyMatchMode: Option[SqlRecognizePatternEmptyMatchMode])

enum SqlRecognizePatternEmptyMatchMode:
    case ShowEmptyMatches
    case OmitEmptyMatches
    case WithUnmatchedRows

case class SqlRowPattern(
    afterMatch: Option[SqlRowPatternSkipMode],
    strategy: Option[SqlRowPatternStrategy],
    pattern: SqlRowPatternTerm,
    subset: List[SqlRowPatternSubsetItem],
    define: List[SqlRowPatternDefineItem]
)

enum SqlRowPatternSkipMode:
    case SkipToNextRow
    case SkipPastLastRow
    case SkipToFirst(name: String)
    case SkipToLast(name: String)
    case SkipTo(name: String)

enum SqlRowPatternStrategy:
    case Initial
    case Seek

enum SqlRowPatternQuantifier:
    case Asterisk(withQuestion: Boolean)
    case Plus(withQuestion: Boolean)
    case Question(withQuestion: Boolean)
    case Between(start: Option[SqlExpr], end: Option[SqlExpr], withQuestion: Boolean)
    case Quantity(quantity: SqlExpr)

enum SqlRowPatternTerm(val quantifier: Option[SqlRowPatternQuantifier]):
    case Pattern(
        name: String, 
        override val quantifier: Option[SqlRowPatternQuantifier]
    ) extends SqlRowPatternTerm(quantifier)
    case Circumflex(
        override val quantifier: Option[SqlRowPatternQuantifier]
    ) extends SqlRowPatternTerm(quantifier)
    case Dollar(
        override val quantifier: Option[SqlRowPatternQuantifier]
    ) extends SqlRowPatternTerm(quantifier)
    case Exclusion(
        term: SqlRowPatternTerm, 
        override val quantifier: Option[SqlRowPatternQuantifier]
    ) extends SqlRowPatternTerm(quantifier)
    case Permute(
        terms: List[SqlRowPatternTerm], 
        override val quantifier: Option[SqlRowPatternQuantifier]
    ) extends SqlRowPatternTerm(quantifier)
    case Then(
        left: SqlRowPatternTerm, 
        right: SqlRowPatternTerm, 
        override val quantifier: Option[SqlRowPatternQuantifier]
    ) extends SqlRowPatternTerm(quantifier)
    case Or(
        left: SqlRowPatternTerm, 
        right: SqlRowPatternTerm, 
        override val quantifier: Option[SqlRowPatternQuantifier]
    ) extends SqlRowPatternTerm(quantifier)

case class SqlRowPatternDefineItem(name: String, expr: SqlExpr)

case class SqlRowPatternSubsetItem(name: String, patternNames: List[String])