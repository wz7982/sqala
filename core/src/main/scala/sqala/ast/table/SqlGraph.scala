package sqala.ast.table

import sqala.ast.expr.SqlExpr

/**
 * Rows mode for `GRAPH_TABLE`.
 */
enum SqlGraphRowsMode:
    /**
     * One row per match.
     *
     * Renders as `ONE ROW PER MATCH`.
     */
    case Match

    /**
     * One row per vertex.
     *
     * Renders as `ONE ROW PER VERTEX ("name") [IN ("path" [, ...])]`.
     */
    case Vertex(
        name: String,
        inPaths: List[String]
    )

    /**
     * One row per step traversing an edge.
     *
     * Renders as `ONE ROW PER STEP (vertex1, edge, vertex2) [IN ("path" [, ...])]`.
     */
    case Step(
        vertex1: String,
        edge: String,
        vertex2: String,
        inPaths: List[String]
    )

/**
 * Export mode for `GRAPH_TABLE`.
 */
enum SqlGraphExportMode:
    /**
     * Export all singletons except the given patterns.
     *
     * Renders as `EXPORT ALL SINGLETONS EXCEPT (pattern [, ...])`.
     */
    case AllSingletons(exceptPatterns: List[String])

    /**
     * Export only the given singletons.
     *
     * Renders as `EXPORT SINGLETONS (pattern [, ...])`.
     */
    case Singletons(patterns: List[String])

    /**
     * Do not export singletons.
     *
     * Renders as `EXPORT NO SINGLETONS`.
     */
    case NoSingletons

/**
 * Match mode for `GRAPH_TABLE`.
 */
enum SqlGraphMatchMode:
    /**
     * Repeatable matching.
     *
     * Renders as `REPEATABLE ELEMENT|ELEMENT BINDINGS|ELEMENTS`.
     */
    case Repeatable(mode: SqlGraphRepeatableMode)

    /**
     * Different matching.
     *
     * Renders as `DIFFERENT EDGE|EDGE BINDINGS|EDGES`.
     */
    case Different(mode: SqlGraphDifferentMode)

/**
 * Repeatable mode for graph matching.
 */
enum SqlGraphRepeatableMode:
    /**
     * Repeatable element.
     *
     * Renders as `ELEMENT`.
     */
    case Element

    /**
     * Repeatable element bindings.
     *
     * Renders as `ELEMENT BINDINGS`.
     */
    case ElementBindings

    /**
     * Repeatable elements.
     *
     * Renders as `ELEMENTS`.
     */
    case Elements

/**
 * Different mode for graph matching.
 */
enum SqlGraphDifferentMode:
    /**
     * Different edge.
     *
     * Renders as `EDGE`.
     */
    case Edge

    /**
     * Different edge bindings.
     *
     * Renders as `EDGE BINDINGS`.
     */
    case EdgeBindings

    /**
     * Different edges.
     *
     * Renders as `EDGES`.
     */
    case Edges

/**
 * A graph pattern within a `GRAPH_TABLE` expression.
 *
 * Renders as `["name" = ] term`.
 */
case class SqlGraphPattern(
    name: Option[String],
    term: SqlGraphPatternTerm
)

/**
 * A graph pattern term describing vertices, edges, and their relationships.
 */
enum SqlGraphPatternTerm:
    /**
     * A quantified pattern term.
     *
     * Renders as `term quantifier`.
     */
    case Quantified(
        term: SqlGraphPatternTerm,
        quantifier: SqlGraphQuantifier
    )

    /**
     * A vertex pattern term.
     *
     * Renders as `(["name"] [IS "label"] [WHERE expr])`.
     */
    case Vertex(
        name: Option[String],
        label: Option[SqlGraphLabel],
        where: Option[SqlExpr]
    )

    /**
     * An edge pattern term.
     *
     * Renders as `symbol["name" [IS "label"] [WHERE expr]]symbol`.
     */
    case Edge(
        leftSymbol: SqlGraphSymbol,
        name: Option[String],
        label: Option[SqlGraphLabel],
        where: Option[SqlExpr],
        rightSymbol: SqlGraphSymbol
    )

    /**
     * Conjunction of two pattern terms.
     *
     * Renders as `term term` separated by a space.
     */
    case And(
        left: SqlGraphPatternTerm,
        right: SqlGraphPatternTerm
    )

    /**
     * Disjunction of two pattern terms.
     *
     * Renders as `term | term`.
     */
    case Or(
        left: SqlGraphPatternTerm,
        right: SqlGraphPatternTerm
    )

    /**
     * Alternation of two pattern terms.
     *
     * Renders as `term |+| term`.
     */
    case Alternation(
        left: SqlGraphPatternTerm,
        right: SqlGraphPatternTerm
    )

/**
 * A graph label used in vertex and edge patterns.
 *
 * Preceded by `IS` in the pattern term rendering.
 */
enum SqlGraphLabel:
    /**
     * A named label.
     *
     * Renders as `"name"`.
     */
    case Label(name: String)

    /**
     * Wildcard label (any label).
     *
     * Renders as `%`.
     */
    case Percent

    /**
     * Negated label.
     *
     * Renders as `!(label)`.
     */
    case Not(label: SqlGraphLabel)

    /**
     * Conjunction of two labels.
     *
     * Renders as `label & label`.
     */
    case And(left: SqlGraphLabel, right: SqlGraphLabel)

    /**
     * Disjunction of two labels.
     *
     * Renders as `label | label`.
     */
    case Or(left: SqlGraphLabel, right: SqlGraphLabel)

/**
 * A quantifier for graph pattern terms.
 */
enum SqlGraphQuantifier:
    /**
     * Zero or more repetitions.
     *
     * Renders as `*`.
     */
    case Asterisk

    /**
     * Zero or one repetition (optional).
     *
     * Renders as `?`.
     */
    case Question

    /**
     * One or more repetitions.
     *
     * Renders as `+`.
     */
    case Plus

    /**
     * Between `start` and `end` repetitions.
     *
     * Renders as `{[expr], [expr]}`.
     */
    case Between(start: Option[SqlExpr], end: Option[SqlExpr])

    /**
     * Exact quantity of repetitions.
     *
     * Renders as `{expr}`.
     */
    case Quantity(quantity: SqlExpr)

/**
 * Edge symbols for graph pattern direction.
 */
enum SqlGraphSymbol:
    /**
     * Undirected dash.
     *
     * Renders as `-`.
     */
    case Dash

    /**
     * Undirected tilde.
     *
     * Renders as `~`.
     */
    case Tilde

    /**
     * Left-directed arrow.
     *
     * Renders as `<-`.
     */
    case LeftArrow

    /**
     * Right-directed arrow.
     *
     * Renders as `->`.
     */
    case RightArrow

    /**
     * Left-directed tilde arrow.
     *
     * Renders as `<~`.
     */
    case LeftTildeArrow

    /**
     * Right-directed tilde arrow.
     *
     * Renders as `~>`.
     */
    case RightTildeArrow