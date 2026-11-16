package sqala.ir

import sqala.ast.expr.SqlType
import sqala.ast.table.*
import sqala.util.NonEmptyList

/**
 * The intermediate representation of a table source in a `FROM` clause.
 *
 * Covers plain identifiers, derived subqueries, joins, CTE references,
 * function tables, JSON tables, graph tables, `MATCH_RECOGNIZE`
 * sources, and pattern variables.
 */
enum IrTable:
    case Ident(name: String, columnAliases: List[String])
    case Subquery(withLateral: Boolean, query: IrQuery, columnAliases: List[String])
    case Join(left: IrTable, joinType: SqlJoinType, right: IrTable, on: Option[IrExpr])
    case Cte(index: Int, columnAliases: List[String])
    case Func(
        withLateral: Boolean,
        name: String,
        args: List[IrExpr],
        withOrdinality: Boolean,
        columnAliases: List[String]
    )
    case Json(
        withLateral: Boolean,
        expr: IrExpr,
        path: IrExpr,
        pathAlias: Option[String],
        columns: NonEmptyList[IrJsonColumn],
        columnAliases: List[String]
    )
    case Graph(
        withLateral: Boolean,
        name: String,
        matchMode: Option[SqlGraphMatchMode],
        patterns: NonEmptyList[IrGraphPattern],
        where: Option[IrExpr],
        rowsMode: Option[SqlGraphRowsMode],
        columns: NonEmptyList[IrSelectItem],
        exportMode: Option[SqlGraphExportMode],
        columnAliases: List[String]
    )
    case MatchRecognize(
        source: IrTable,
        partitionBy: List[IrExpr],
        orderBy: List[IrOrderingItem],
        measures: NonEmptyList[IrSelectItem],
        rowsMode: Option[SqlRecognizePatternRowsMode],
        rowPattern: IrRowPattern,
        columnAliases: List[String]
    )
    case PatternVariable(name: String)

/**
 * A column definition of a JSON table: ordinality, value extraction,
 * existence check, or nested column group.
 */
enum IrJsonColumn:
    case Ordinality(name: String)
    case Column(
        name: String,
        `type`: SqlType,
        path: Option[IrExpr]
    )
    case Exists(
        name: String,
        `type`: SqlType,
        path: Option[IrExpr]
    )
    case Nested(
        path: IrExpr,
        pathAlias: Option[String],
        columns: NonEmptyList[IrJsonColumn]
    )

/**
 * A named graph pattern used by a graph table.
 */
case class IrGraphPattern(
    name: Option[String],
    term: IrGraphPatternTerm
)

/**
 * A graph pattern term: a quantified sub-term, vertex, edge, or a
 * conjunction/disjunction/alternation of sub-terms.
 */
enum IrGraphPatternTerm:
    case Quantified(
        term: IrGraphPatternTerm,
        quantifier: IrGraphQuantifier
    )
    case Vertex(
        table: IrTable,
        label: Option[SqlGraphLabel],
        where: Option[IrExpr]
    )
    case Edge(
        leftSymbol: SqlGraphSymbol,
        table: IrTable,
        label: Option[SqlGraphLabel],
        where: Option[IrExpr],
        rightSymbol: SqlGraphSymbol
    )
    case And(
        left: IrGraphPatternTerm,
        right: IrGraphPatternTerm
    )
    case Or(
        left: IrGraphPatternTerm,
        right: IrGraphPatternTerm
    )
    case Alternation(
        left: IrGraphPatternTerm,
        right: IrGraphPatternTerm
    )

/**
 * A quantifier applied to a graph pattern term (`*`, `?`, `+`, a range,
 * or an explicit quantity).
 */
enum IrGraphQuantifier:
    case Asterisk
    case Question
    case Plus
    case Between(start: Option[IrExpr], end: Option[IrExpr])
    case Quantity(quantity: IrExpr)

/**
 * The row pattern specification of a `MATCH_RECOGNIZE` clause.
 */
case class IrRowPattern(
    afterMatchMode: Option[SqlRowPatternSkipMode],
    strategy: Option[SqlRowPatternStrategy],
    pattern: IrRowPatternTerm,
    subset: List[SqlRowPatternSubsetItem],
    define: NonEmptyList[IrRowPatternDefineItem]
)

/**
 * A `DEFINE` item binding a pattern variable name to a condition.
 */
case class IrRowPatternDefineItem(name: String, expr: IrExpr)

/**
 * A quantifier applied to a row pattern term (`*`, `+`, `?`, a range, or
 * an explicit quantity), optionally marked reluctant (`?` suffix).
 */
enum IrRowPatternQuantifier:
    case Asterisk(withQuestion: Boolean)
    case Plus(withQuestion: Boolean)
    case Question(withQuestion: Boolean)
    case Between(start: Option[IrExpr], end: Option[IrExpr], withQuestion: Boolean)
    case Quantity(quantity: IrExpr)

/**
 * A term of a `MATCH_RECOGNIZE` row pattern: a pattern variable, the
 * `^`/`$` anchors, an exclusion, a permutation, a concatenation (`then`),
 * or an alternation (`or`), each with an optional quantifier.
 */
enum IrRowPatternTerm(val quantifier: Option[IrRowPatternQuantifier]):
    case Pattern(
        name: String,
        override val quantifier: Option[IrRowPatternQuantifier]
    ) extends IrRowPatternTerm(quantifier)
    case Circumflex(
        override val quantifier: Option[IrRowPatternQuantifier]
    ) extends IrRowPatternTerm(quantifier)
    case Dollar(
        override val quantifier: Option[IrRowPatternQuantifier]
    ) extends IrRowPatternTerm(quantifier)
    case Exclusion(
        term: IrRowPatternTerm,
        override val quantifier: Option[IrRowPatternQuantifier]
    ) extends IrRowPatternTerm(quantifier)
    case Permute(
        terms: NonEmptyList[IrRowPatternTerm],
        override val quantifier: Option[IrRowPatternQuantifier]
    ) extends IrRowPatternTerm(quantifier)
    case Then(
        left: IrRowPatternTerm,
        right: IrRowPatternTerm,
        override val quantifier: Option[IrRowPatternQuantifier]
    ) extends IrRowPatternTerm(quantifier)
    case Or(
        left: IrRowPatternTerm,
        right: IrRowPatternTerm,
        override val quantifier: Option[IrRowPatternQuantifier]
    ) extends IrRowPatternTerm(quantifier)