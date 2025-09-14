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

case class SqlGraphPattern(
    declaration: Option[String],
    prefix: Option[SqlGraphPatternPrefix],
    expr: SqlGraphPatternExpr
)

// TODO
class SqlGraphPatternPrefix

// TODO
class SqlGraphPatternExpr