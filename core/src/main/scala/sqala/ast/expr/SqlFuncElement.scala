package sqala.ast.expr

enum SqlVectorDistanceMode:
    case Euclidean
    case Cosine
    case Dot
    case Manhattan

case class SqlTrim(mode: Option[SqlTrimMode], value: Option[SqlExpr])

enum SqlTrimMode:
    case Both
    case Leading
    case Trailing

enum SqlJsonUniqueness:
    case With
    case Without

case class SqlJsonPassing(expr: SqlExpr, alias: String)

enum SqlJsonNodeType:
    case Value
    case Object
    case Array
    case Scalar

enum SqlJsonEncoding:
    case Utf8
    case Utf16
    case Utf32
    case Custom(encoding: String)

enum SqlJsonNullConstructor:
    case Null
    case Absent

enum SqlJsonQueryWrapperBehavior:
    case With(mode: Option[SqlJsonQueryWrapperBehaviorMode], array: Boolean)
    case Without(array: Boolean)

enum SqlJsonQueryWrapperBehaviorMode:
    case Conditional
    case Unconditional

case class SqlJsonQueryQuotesBehavior(mode: SqlJsonQueryQuotesBehaviorMode, onScalarString: Boolean)

enum SqlJsonQueryQuotesBehaviorMode:
    case Keep
    case Omit

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

enum SqlJsonExistsErrorBehavior:
    case Error
    case True
    case False
    case Unknown

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

case class SqlJsonObjectItem(key: SqlExpr, value: SqlExpr)

case class SqlJsonArrayItem(value: SqlExpr, input: Option[SqlJsonInput])

enum SqlListAggOnOverflow:
    case Error
    case Truncate(expr: SqlExpr, countMode: SqlListAggCountMode)

enum SqlListAggCountMode:
    case With
    case Without

enum SqlWindowNullsMode:
    case Respect
    case Ignore

enum SqlNthValueFromMode:
    case First
    case Last

enum SqlMatchPhase:
    case Final
    case Running