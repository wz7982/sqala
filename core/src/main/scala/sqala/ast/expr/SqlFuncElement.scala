package sqala.ast.expr

enum SqlVectorDistanceMode:
    case Euclidean
    case Cosine
    case Dot
    case Manhattan

enum SqlTrimMode(val mode: String):
    case Both extends SqlTrimMode("BOTH")
    case Leading extends SqlTrimMode("LEADING")
    case Trailing extends SqlTrimMode("TRAILING")

enum SqlJsonUniqueness(val uniqueness: String):
    case With extends SqlJsonUniqueness("WITH UNIQUE KEYS")
    case Without extends SqlJsonUniqueness("WITHOUT UNIQUE KEYS")

case class SqlJsonPassing(expr: SqlExpr, alias: String)

enum SqlJsonNodeType(val `type`: String):
    case Value extends SqlJsonNodeType("VALUE")
    case Object extends SqlJsonNodeType("OBJECT")
    case Array extends SqlJsonNodeType("ARRAY")
    case Scalar extends SqlJsonNodeType("SCALAR")

enum SqlJsonEncoding(val encoding: String):
    case Utf8 extends SqlJsonEncoding("UTF8")
    case Utf16 extends SqlJsonEncoding("UTF16")
    case Utf32 extends SqlJsonEncoding("UTF32")

enum SqlJsonNullConstructor(val constructor: String):
    case Null extends SqlJsonNullConstructor("NULL ON NULL")
    case Absent extends SqlJsonNullConstructor("ABSENT ON NULL")

enum SqlJsonQueryWrapperBehavior:
    case With(mode: Option[SqlJsonQueryWrapperBehaviorMode], array: Boolean)
    case Without(array: Boolean)

enum SqlJsonQueryWrapperBehaviorMode(val mode: String):
    case Conditional extends SqlJsonQueryWrapperBehaviorMode("CONDITIONAL")
    case Unconditional extends SqlJsonQueryWrapperBehaviorMode("UNCONDITIONAL")

case class SqlJsonQueryQuotesBehavior(mode: SqlJsonQueryQuotesBehaviorMode, onScalarString: Boolean)

enum SqlJsonQueryQuotesBehaviorMode(val mode: String):
    case Keep extends SqlJsonQueryQuotesBehaviorMode("KEEP")
    case Omit extends SqlJsonQueryQuotesBehaviorMode("OMIT")

enum SqlJsonQueryEmptyBehavior:
    case Error
    case Null
    case EmptyObject
    case EmptyArray
    case Default(expr: SqlExpr)

enum SqlJsonQueryErrorBehavior:
    case Error
    case Null
    case EmptyObject
    case EmptyArray
    case Default(expr: SqlExpr)

enum SqlJsonValueEmptyBehavior:
    case Error
    case Null
    case Default(expr: SqlExpr)

enum SqlJsonValueErrorBehavior:
    case Error
    case Null
    case Default(expr: SqlExpr)

enum SqlJsonExistsErrorBehavior(val mode: String):
    case Error extends SqlJsonExistsErrorBehavior("ERROR")
    case True extends SqlJsonExistsErrorBehavior("TRUE")
    case False extends SqlJsonExistsErrorBehavior("FALSE")
    case Unknown extends SqlJsonExistsErrorBehavior("UNKNOWN")

case class SqlJsonInput(format: Option[SqlJsonEncoding])
    
case class SqlJsonOutput(`type`: SqlType, format: Option[Option[SqlJsonEncoding]])

enum SqlJsonTableErrorBehavior:
    case Error
    case Empty
    case EmptyArray

enum SqlJsonTableColumn:
    case Ordinality(name: String)
    case Column(
        name: String, 
        `type`: SqlType,
        format: Option[Option[SqlJsonEncoding]],
        path: Option[SqlExpr],
        wrapper: Option[SqlJsonQueryWrapperBehavior],
        quotes: Option[SqlJsonQueryQuotesBehavior],
        onEmpty: Option[SqlJsonQueryEmptyBehavior],
        onError: Option[SqlJsonQueryErrorBehavior]
    )
    case Exists(
        name: String,
        `type`: SqlType,
        path: Option[SqlExpr],
        onError: Option[SqlJsonExistsErrorBehavior]
    )
    case Nested(
        path: SqlExpr,
        pathAlias: Option[String],
        columns: List[SqlJsonTableColumn]
    )

case class SqlListAggOnOverflow(
    mode: SqlListAggOnOverflowMode,
    countMode: SqlListAggCountMode
)

enum SqlListAggOnOverflowMode:
    case Error
    case Truncate(expr: SqlExpr)

enum SqlListAggCountMode(val mode: String):
    case With extends SqlListAggCountMode("WITH COUNT")
    case Without extends SqlListAggCountMode("WITHOUT COUNT")

enum SqlWindowNullsMode(val mode: String):
    case Respect extends SqlWindowNullsMode("RESPECT NULLS")
    case Ignore extends SqlWindowNullsMode("IGNORE NULLS")

enum SqlNthValueFromMode(val mode: String):
    case First extends SqlNthValueFromMode("FROM FIRST")
    case Last extends SqlNthValueFromMode("FROM LAST")

enum SqlMatchPhase(val phase: String):
    case Final extends SqlMatchPhase("FINAL")
    case Running extends SqlMatchPhase("RUNNING")