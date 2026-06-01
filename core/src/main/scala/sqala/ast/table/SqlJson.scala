package sqala.ast.table

import sqala.ast.expr.*

enum SqlJsonErrorBehavior:
    case Error
    case Empty
    case EmptyArray

enum SqlJsonColumn:
    case Ordinality(name: String)
    case Column(
        name: String,
        `type`: SqlType,
        format: Option[SqlJsonOutputFormat],
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
        columns: List[SqlJsonColumn]
    )