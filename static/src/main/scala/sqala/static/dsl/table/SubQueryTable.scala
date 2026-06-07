package sqala.static.dsl.table

import sqala.ast.statement.SqlQuery
import sqala.ast.table.{SqlTable, SqlTableAlias}
import sqala.static.dsl.{Index, ToTuple}
import sqala.static.dsl.statement.query.Query

import scala.NamedTuple.NamedTuple
import scala.compiletime.constValue

/**
 * A table reference produced by `from` when a query is passed as a
 * subquery source, with typed column access via `selectDynamic`.
 */
final case class SubqueryTable[N <: Tuple, V <: Tuple, L <: Int](
    private[sqala] val __aliasName__ : Option[String],
    private[sqala] val __items__ : V,
    private[sqala] val __sqlTable__ : SqlTable.Subquery
) extends Selectable with AnyTable:
    /**
     * The structural type declaring available columns as a named tuple.
     * Required by `Selectable`.
     */
    type Fields = NamedTuple[N, V]

    /**
     * Runtime column accessor. Required by `Selectable`.
     */
    inline def selectDynamic(name: String): Any =
        val index = constValue[Index[N, name.type, 0]]
        __items__.toList(index)

object SubqueryTable:
    def apply[N <: Tuple, V <: Tuple, L <: Int](query: SqlQuery, lateral: Boolean, alias: Option[String])(using
        p: AsTableParam[V, L],
        t: ToTuple[p.R]
    ): SubqueryTable[N, t.R, L] =
        new SubqueryTable(
            alias,
            t.toTuple(p.asTableParam(alias, 1)),
            SqlTable.Subquery(
                lateral,
                query,
                alias.map(SqlTableAlias(_, Nil)),
                None
            )
        )

    def apply[N <: Tuple, V <: Tuple, L <: Int](query: Query[?, ?, ?, ?], lateral: Boolean, alias: Option[String])(using
        p: AsTableParam[V, L],
        t: ToTuple[p.R]
    ): SubqueryTable[N, t.R, L] =
        apply(query.tree, lateral, alias)