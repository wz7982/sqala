package sqala.ast.table

import sqala.ast.expr.SqlExpr

enum SqlGraphRowsMode:
    case Match
    case Vertex(
        name: String, 
        inPaths: List[String]
    )
    case Step(
        vertex1: String, 
        edge: String, 
        vertex2: String,
        inPaths: List[String]
    )

enum SqlGraphExportMode:
    case AllSingletons(exceptPatterns: List[String])
    case Singletons(patterns: List[String])
    case NoSingletons

enum SqlGraphMatchMode:
    case Repeatable(mode: SqlGraphRepeatableMode)
    case Different(mode: SqlGraphDifferentMode)

enum SqlGraphRepeatableMode:
    case Element
    case ElementBindings
    case Elements

enum SqlGraphDifferentMode:
    case Edge
    case EdgeBindings
    case Edges

case class SqlGraphPattern(
    name: Option[String],
    term: SqlGraphPatternTerm
)

enum SqlGraphPatternTerm:
    case Quantified(
        term: SqlGraphPatternTerm,
        quantifier: SqlGraphQuantifier
    )
    case Vertex(
        name: Option[String],
        label: Option[SqlGraphLabel],
        where: Option[SqlExpr]
    )
    case Edge(
        leftSymbol: SqlGraphSymbol,
        name: Option[String],
        label: Option[SqlGraphLabel],
        where: Option[SqlExpr],
        rightSymbol: SqlGraphSymbol
    )
    case And(
        left: SqlGraphPatternTerm, 
        right: SqlGraphPatternTerm
    )
    case Or(
        left: SqlGraphPatternTerm, 
        right: SqlGraphPatternTerm
    )
    case Alternation(
        left: SqlGraphPatternTerm, 
        right: SqlGraphPatternTerm
    )

enum SqlGraphLabel:
    case Label(name: String)
    case Percent
    case Not(label: SqlGraphLabel)
    case And(left: SqlGraphLabel, right: SqlGraphLabel)
    case Or(left: SqlGraphLabel, right: SqlGraphLabel)

enum SqlGraphQuantifier:
    case Asterisk
    case Question
    case Plus
    case Between(start: Option[SqlExpr], end: Option[SqlExpr])
    case Quantity(quantity: SqlExpr)

enum SqlGraphSymbol:
    case Dash
    case Tilde
    case LeftArrow
    case RightArrow
    case LeftTildeArrow
    case RightTildeArrow