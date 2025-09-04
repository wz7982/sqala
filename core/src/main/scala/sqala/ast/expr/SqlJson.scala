package sqala.ast.expr

enum SqlUniqueness(val uniqueness: String):
    case WithUnique extends SqlUniqueness("WITH UNIQUE KEYS")
    case WithoutUnique extends SqlUniqueness("WITHOUT UNIQUE KEYS")

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
    case Without(array: Boolean)
    case With(mode: Option[SqlJsonQueryWrapperBehaviorMode], array: Boolean)

enum SqlJsonQueryWrapperBehaviorMode(val mode: String):
    case Conditional extends SqlJsonQueryWrapperBehaviorMode("CONDITIONAL")
    case Unconditional extends SqlJsonQueryWrapperBehaviorMode("UNCONDITIONAL")

case class SqlJsonQueryQuotesBehavior(mode: SqlJsonQueryQuotesBehaviorMode, onScalarString: Boolean)

enum SqlJsonQueryQuotesBehaviorMode(val mode: String):
    case Keep extends SqlJsonQueryQuotesBehaviorMode("KEEP")
    case Omit extends SqlJsonQueryQuotesBehaviorMode("OMIT")

enum SqlJsonQueryEmptyOrErrorBehavior:
    case Error
    case Null
    case EmptyObject
    case EmptyArray
    case Default(expr: SqlExpr)

enum SqlJsonValueEmptyOrErrorBehavior:
    case Error
    case Null
    case Default(expr: SqlExpr)

enum SqlJsonExistsErrorBehavior(val mode: String):
    case Error extends SqlJsonExistsErrorBehavior("ERROR")
    case True extends SqlJsonExistsErrorBehavior("TRUE")
    case False extends SqlJsonExistsErrorBehavior("FALSE")
    case Unknown extends SqlJsonExistsErrorBehavior("UNKNOWN")
    
case class SqlJsonReturning(`type`: SqlType, format: Option[Option[SqlJsonEncoding]])