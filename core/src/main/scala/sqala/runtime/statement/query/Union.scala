package sqala.runtime.statement.query

import sqala.ast.statement.{SqlUnionType, SqlQuery}

class Union(
    private[sqala] val left: Query,
    private[sqala] val unionType: SqlUnionType,
    private[sqala] val right: Query
) extends Query:
    override def ast: SqlQuery = SqlQuery.Union(left.ast, unionType, right.ast)