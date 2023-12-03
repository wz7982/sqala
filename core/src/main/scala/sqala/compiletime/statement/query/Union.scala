package sqala.compiletime.statement.query

import sqala.ast.statement.{SqlQuery, SqlUnionType}
import sqala.compiletime.Column

class Union[T <: Tuple, AliasNames <: Tuple](
    private[sqala] val left: Query[?, ?],
    private[sqala] val unionType: SqlUnionType,
    private[sqala] val right: Query[?, ?]
) extends Query[T, AliasNames]:
    def ast: SqlQuery = SqlQuery.Union(left.ast, unionType, right.ast)

    private[sqala] def cols: List[Column[?, ?, ?]] = left.cols