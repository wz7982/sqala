package sqala.static.dsl.table

import sqala.ast.table.{SqlTable, SqlTableAlias}
import sqala.static.dsl.Index
import sqala.static.dsl.statement.query.Query

import scala.NamedTuple.NamedTuple
import scala.compiletime.constValue
import sqala.ast.statement.SqlQuery

case class SubQueryTable[N <: Tuple, V <: Tuple](
    private[sqala] val __aliasName__ : Option[String],
    private[sqala] val __items__ : V,
    private[sqala] val __sqlTable__ : SqlTable.SubQuery
) extends Selectable:
    type Fields = NamedTuple[N, V]

    inline def selectDynamic(name: String): Any =
        val index = constValue[Index[N, name.type, 0]]
        __items__.toList(index)

object SubQueryTable:
    def apply[N <: Tuple, V <: Tuple](query: SqlQuery, lateral: Boolean, alias: Option[String])(using 
        p: AsTableParam[V]
    ): SubQueryTable[N, V] =
        new SubQueryTable(
            alias, 
            p.asTableParam(alias, 1),
            SqlTable.SubQuery(
                query,
                lateral,
                alias.map(SqlTableAlias(_, Nil)),
                None
            )
        )

    def apply[N <: Tuple, V <: Tuple](query: Query[?], lateral: Boolean, alias: Option[String])(using 
        p: AsTableParam[V]
    ): SubQueryTable[N, V] =
        apply(query.tree, lateral, alias)