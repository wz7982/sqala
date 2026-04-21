package sqala.static.dsl.table

import sqala.ast.statement.SqlQuery
import sqala.ast.table.{SqlTable, SqlTableAlias}
import sqala.static.dsl.{Index, ToTuple}
import sqala.static.dsl.statement.query.Query

import scala.NamedTuple.NamedTuple
import scala.compiletime.constValue

case class SubQueryTable[N <: Tuple, V <: Tuple, F <: InFrom](
    private[sqala] val __aliasName__ : Option[String],
    private[sqala] val __items__ : V,
    private[sqala] val __sqlTable__ : SqlTable.SubQuery
) extends Selectable with AnyTable:
    type Fields = NamedTuple[N, V]

    inline def selectDynamic(name: String): Any =
        val index = constValue[Index[N, name.type, 0]]
        __items__.toList(index)

object SubQueryTable:
    def apply[N <: Tuple, V <: Tuple](query: SqlQuery, lateral: Boolean, alias: Option[String])(using
        p: AsTableParam[V],
        t: ToTuple[p.R]
    ): SubQueryTable[N, t.R, CanInFrom] =
        new SubQueryTable(
            alias,
            t.toTuple(p.asTableParam(alias, 1)),
            SqlTable.SubQuery(
                lateral,
                query,
                alias.map(SqlTableAlias(_, Nil)),
                None
            )
        )

    def apply[N <: Tuple, V <: Tuple](query: Query[?, ?], lateral: Boolean, alias: Option[String])(using
        p: AsTableParam[V],
        t: ToTuple[p.R]
    ): SubQueryTable[N, t.R, CanInFrom] =
        apply(query.tree, lateral, alias)