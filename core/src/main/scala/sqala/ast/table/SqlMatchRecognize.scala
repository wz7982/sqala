package sqala.ast.table

import sqala.ast.expr.SqlExpr
import sqala.ast.order.SqlOrderingItem
import sqala.util.NonEmptyList

/**
 * A `MATCH_RECOGNIZE` clause for row pattern recognition.
 *
 * Renders as
 * `MATCH_RECOGNIZE(
 *   [PARTITION BY expr [, ...]]
 *   [ORDER BY ordering_item [, ...]]
 *   MEASURES expr AS "alias" [, ...]
 *   [ONE ROW PER MATCH|ALL ROWS PER MATCH [SHOW EMPTY MATCHES|OMIT EMPTY MATCHES|WITH UNMATCHED ROWS]]
 *   [AFTER MATCH SKIP TO NEXT ROW|PAST LAST ROW|TO LAST "name"|TO "name"]
 *   [INITIAL|SEEK]
 *   PATTERN (term)
 *   [SUBSET "name" = (pattern [, ...]), [, ...]]
 *   DEFINE "name" AS expr [, ...]
 * ) [[AS] "alias" [("column_alias" [, ...])]]`.
 */
case class SqlMatchRecognize(
    partitionBy: List[SqlExpr],
    orderBy: List[SqlOrderingItem],
    measures: List[SqlRecognizeMeasureItem],
    rowsMode: Option[SqlRecognizePatternRowsMode],
    rowPattern: SqlRowPattern,
    alias: Option[SqlTableAlias]
)

/**
 * A `MEASURES` item in `MATCH_RECOGNIZE`.
 *
 * Renders as `expr AS "alias"`.
 */
case class SqlRecognizeMeasureItem(expr: SqlExpr, alias: String)

/**
 * Rows-per-match mode for `MATCH_RECOGNIZE`.
 */
enum SqlRecognizePatternRowsMode:
    /**
     * One row per match.
     *
     * Renders as `ONE ROW PER MATCH`.
     */
    case OneRow

    /**
     * All rows per match, optionally with an empty match mode.
     *
     * Renders as `ALL ROWS PER MATCH [SHOW EMPTY MATCHES|OMIT EMPTY MATCHES|WITH UNMATCHED ROWS]`.
     */
    case AllRows(emptyMatchMode: Option[SqlRecognizePatternEmptyMatchMode])

/**
 * Empty match handling mode for `ALL ROWS PER MATCH`.
 */
enum SqlRecognizePatternEmptyMatchMode:
    /**
     * Show empty matches.
     *
     * Renders as `SHOW EMPTY MATCHES`.
     */
    case ShowEmptyMatches

    /**
     * Omit empty matches.
     *
     * Renders as `OMIT EMPTY MATCHES`.
     */
    case OmitEmptyMatches

    /**
     * Show unmatched rows.
     *
     * Renders as `WITH UNMATCHED ROWS`.
     */
    case WithUnmatchedRows

/**
 * A row pattern definition within `MATCH_RECOGNIZE`.
 *
 * Renders as
 * `[AFTER MATCH SKIP TO NEXT ROW|PAST LAST ROW|TO LAST "name"|TO "name"]
 *   [INITIAL|SEEK]
 *   PATTERN (term)
 *   [SUBSET "name" = (pattern [, ...]) [, ...]]
 *   [DEFINE "name" AS expr [, ...]]`.
 */
case class SqlRowPattern(
    afterMatchMode: Option[SqlRowPatternSkipMode],
    strategy: Option[SqlRowPatternStrategy],
    pattern: SqlRowPatternTerm,
    subset: List[SqlRowPatternSubsetItem],
    define: NonEmptyList[SqlRowPatternDefineItem]
)

/**
 * Skip mode for `AFTER MATCH`.
 */
enum SqlRowPatternSkipMode:
    /**
     * Skip to the next row.
     *
     * Renders as `AFTER MATCH SKIP TO NEXT ROW`.
     */
    case ToNextRow

    /**
     * Skip past the last row of the match.
     *
     * Renders as `AFTER MATCH SKIP PAST LAST ROW`.
     */
    case PastLastRow

    /**
     * Skip to the first occurrence of the named pattern variable.
     *
     * Renders as `AFTER MATCH SKIP TO FIRST name`.
     */
    case ToFirst(name: String)

    /**
     * Skip to the last occurrence of the named pattern variable.
     *
     * Renders as `AFTER MATCH SKIP TO LAST "name"`.
     */
    case ToLast(name: String)

    /**
     * Skip to the named pattern variable.
     *
     * Renders as `AFTER MATCH SKIP TO "name"`.
     */
    case To(name: String)

/**
 * Matching strategy for `MATCH_RECOGNIZE`.
 */
enum SqlRowPatternStrategy:
    /**
     * Initial strategy.
     *
     * Renders as `INITIAL`.
     */
    case Initial

    /**
     * Seek strategy.
     *
     * Renders as `SEEK`.
     */
    case Seek

/**
 * Quantifier for row pattern terms.
 */
enum SqlRowPatternQuantifier:
    /**
     * Zero or more (reluctant when `withQuestion` is true).
     *
     * Renders as `*|*?`.
     */
    case Asterisk(withQuestion: Boolean)

    /**
     * One or more (reluctant when `withQuestion` is true).
     *
     * Renders as `+|+?`.
     */
    case Plus(withQuestion: Boolean)

    /**
     * Zero or one (reluctant when `withQuestion` is true).
     *
     * Renders as `?|??`.
     */
    case Question(withQuestion: Boolean)

    /**
     * Between `start` and `end` repetitions (reluctant when `withQuestion` is true).
     *
     * Renders as `{[expr], [expr]}|{[expr], [expr]}?`.
     */
    case Between(start: Option[SqlExpr], end: Option[SqlExpr], withQuestion: Boolean)

    /**
     * Exact quantity of repetitions.
     *
     * Renders as `{expr}`.
     */
    case Quantity(quantity: SqlExpr)

/**
 * A row pattern term, optionally quantified.
 */
enum SqlRowPatternTerm(val quantifier: Option[SqlRowPatternQuantifier]):
    /**
     * A named pattern variable.
     *
     * Renders as `"name" [quantifier]`.
     */
    case Pattern(
        name: String,
        override val quantifier: Option[SqlRowPatternQuantifier]
    ) extends SqlRowPatternTerm(quantifier)

    /**
     * Start anchor.
     *
     * Renders as `^ [quantifier]`.
     */
    case Circumflex(
        override val quantifier: Option[SqlRowPatternQuantifier]
    ) extends SqlRowPatternTerm(quantifier)

    /**
     * End anchor.
     *
     * Renders as `$ [quantifier]`.
     */
    case Dollar(
        override val quantifier: Option[SqlRowPatternQuantifier]
    ) extends SqlRowPatternTerm(quantifier)

    /**
     * Exclusion of a pattern term.
     *
     * Renders as `{- term -} [quantifier]`.
     */
    case Exclusion(
        term: SqlRowPatternTerm,
        override val quantifier: Option[SqlRowPatternQuantifier]
    ) extends SqlRowPatternTerm(quantifier)

    /**
     * Permutation of pattern terms.
     *
     * Renders as `PERMUTE(term [, ...]) [quantifier]`.
     */
    case Permute(
        terms: NonEmptyList[SqlRowPatternTerm],
        override val quantifier: Option[SqlRowPatternQuantifier]
    ) extends SqlRowPatternTerm(quantifier)

    /**
     * Concatenation (sequence) of two pattern terms.
     *
     * Renders as `term term [quantifier]`.
     */
    case Then(
        left: SqlRowPatternTerm,
        right: SqlRowPatternTerm,
        override val quantifier: Option[SqlRowPatternQuantifier]
    ) extends SqlRowPatternTerm(quantifier)

    /**
     * Alternation of two pattern terms.
     *
     * Renders as `term | term [quantifier]`.
     */
    case Or(
        left: SqlRowPatternTerm,
        right: SqlRowPatternTerm,
        override val quantifier: Option[SqlRowPatternQuantifier]
    ) extends SqlRowPatternTerm(quantifier)

/**
 * A `DEFINE` item in `MATCH_RECOGNIZE`.
 *
 * Renders as `"name" AS expr`.
 */
case class SqlRowPatternDefineItem(name: String, expr: SqlExpr)

/**
 * A `SUBSET` item in `MATCH_RECOGNIZE`.
 *
 * Renders as `"name" = (pattern [, ...])`.
 */
case class SqlRowPatternSubsetItem(name: String, patternNames: NonEmptyList[String])