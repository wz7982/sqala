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
     *
     * @param name the vertex pattern variable.
     * @param inPaths the path variables leading into the vertex.
     */
    case Vertex(
        name: String,
        inPaths: List[String]
    )

    /**
     * One row per step traversing an edge.
     *
     * Renders as `ONE ROW PER STEP (vertex1, edge, vertex2) [IN ("path" [, ...])]`.
     *
     * @param vertex1 the source vertex.
     * @param edge the traversed edge.
     * @param vertex2 the destination vertex.
     * @param inPaths the path variables leading into the step.
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
     *
     * @param exceptPatterns the pattern names to exclude.
     */
    case AllSingletons(exceptPatterns: List[String])

    /**
     * Export only the given singletons.
     *
     * Renders as `EXPORT SINGLETONS (pattern [, ...])`.
     *
     * @param patterns the pattern names to export.
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
     *
     * @param mode the repeatable mode.
     */
    case Repeatable(mode: SqlGraphRepeatableMode)

    /**
     * Different matching.
     *
     * Renders as `DIFFERENT EDGE|EDGE BINDINGS|EDGES`.
     *
     * @param mode the different mode.
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
 *
 * @param name optional pattern name.
 * @param term the pattern term.
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
     *
     * @param term the inner pattern term.
     * @param quantifier the quantifier applied to the term.
     */
    case Quantified(
        term: SqlGraphPatternTerm,
        quantifier: SqlGraphQuantifier
    )

    /**
     * A vertex pattern term.
     *
     * Renders as `(["name"] [IS "label"] [WHERE expr])`.
     *
     * @param name optional vertex variable name.
     * @param label optional vertex label.
     * @param where optional filter condition.
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
     *
     * @param leftSymbol the left edge symbol.
     * @param name optional edge variable name.
     * @param label optional edge label.
     * @param where optional filter condition.
     * @param rightSymbol the right edge symbol.
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
     *
     * @param left the left term.
     * @param right the right term.
     */
    case And(
        left: SqlGraphPatternTerm,
        right: SqlGraphPatternTerm
    )

    /**
     * Disjunction of two pattern terms.
     *
     * Renders as `term | term`.
     *
     * @param left the left term.
     * @param right the right term.
     */
    case Or(
        left: SqlGraphPatternTerm,
        right: SqlGraphPatternTerm
    )

    /**
     * Alternation of two pattern terms.
     *
     * Renders as `term |+| term`.
     *
     * @param left the left term.
     * @param right the right term.
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
     *
     * @param name the label name.
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
     *
     * @param label the label to negate.
     */
    case Not(label: SqlGraphLabel)

    /**
     * Conjunction of two labels.
     *
     * Renders as `label & label`.
     *
     * @param left the left label.
     * @param right the right label.
     */
    case And(left: SqlGraphLabel, right: SqlGraphLabel)

    /**
     * Disjunction of two labels.
     *
     * Renders as `label | label`.
     *
     * @param left the left label.
     * @param right the right label.
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
     *
     * @param start optional minimum count.
     * @param end optional maximum count.
     */
    case Between(start: Option[SqlExpr], end: Option[SqlExpr])

    /**
     * Exact quantity of repetitions.
     *
     * Renders as `{expr}`.
     *
     * @param quantity the exact count expression.
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