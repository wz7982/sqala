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

enum SqlGraphColumn:
    case Asterisk(tableName: Option[String])
    case Expr(expr: SqlExpr, alias: Option[String])

enum SqlGraphExportMode:
    case AllSingletons(exceptPatterns: List[String])
    case Singletons(patterns: List[String])
    case NoSingletons

enum SqlGraphMatchMode:
    case Repeatable(mode: SqlGraphRepeatableMode)
    case Different(mode: SqlGraphDifferentMode)

enum SqlGraphRepeatableMode(val mode: String):
    case Element extends SqlGraphRepeatableMode("ELEMENT")
    case ElementBindings extends SqlGraphRepeatableMode("ELEMENT BINDINGS")
    case Elements extends SqlGraphRepeatableMode("ELEMENTS")

enum SqlGraphDifferentMode(val mode: String):
    case Edge extends SqlGraphDifferentMode("EDGE")
    case EdgeBindings extends SqlGraphDifferentMode("EDGE BINDINGS")
    case Edges extends SqlGraphDifferentMode("EDGES")

enum SqlGraphPatternTerm(val quantifier: Option[SqlGraphQuantifier]):
    case Pattern(
        name: Option[String],
        term: SqlGraphPatternTerm,
        where: Option[SqlExpr],
        override val quantifier: Option[SqlGraphQuantifier]
    ) extends SqlGraphPatternTerm(quantifier)
    case Vertex(
        name: Option[String],
        label: Option[SqlGraphLabelTerm],
        where: Option[SqlExpr],
        override val quantifier: Option[SqlGraphQuantifier]
    ) extends SqlGraphPatternTerm(quantifier)
    case Edge(
        leftSymbol: SqlGraphSymbol,
        name: Option[String],
        label: Option[SqlGraphLabelTerm],
        where: Option[SqlExpr],
        rightSymbol: SqlGraphSymbol,
        override val quantifier: Option[SqlGraphQuantifier]
    ) extends SqlGraphPatternTerm(quantifier)
    case And(
        left: SqlGraphPatternTerm, 
        right: SqlGraphPatternTerm,
        override val quantifier: Option[SqlGraphQuantifier]
    ) extends SqlGraphPatternTerm(quantifier)
    case Or(
        left: SqlGraphPatternTerm, 
        right: SqlGraphPatternTerm,
        override val quantifier: Option[SqlGraphQuantifier]
    ) extends SqlGraphPatternTerm(quantifier)
    case Alternation(
        left: SqlGraphPatternTerm, 
        right: SqlGraphPatternTerm,
        override val quantifier: Option[SqlGraphQuantifier]
    ) extends SqlGraphPatternTerm(quantifier)

enum SqlGraphLabelTerm:
    case Label(name: String)
    case Percent
    case Not(term: SqlGraphLabelTerm)
    case And(left: SqlGraphLabelTerm, right: SqlGraphLabelTerm)
    case Or(left: SqlGraphLabelTerm, right: SqlGraphLabelTerm)

enum SqlGraphQuantifier:
    case Asterisk
    case Question
    case Plus
    case Between(start: Option[SqlExpr], end: Option[SqlExpr])
    case Quantity(quantity: SqlExpr)

enum SqlGraphSymbol(val symbol: String):
    case Dash extends SqlGraphSymbol("-")
    case Tilde extends SqlGraphSymbol("~")
    case LeftArrow extends SqlGraphSymbol("<-")
    case RightArrow extends SqlGraphSymbol("->")
    case LeftTildeArrow extends SqlGraphSymbol("<~")
    case RightTildeArrow extends SqlGraphSymbol("~>")