package sqala.static.dsl.table

import sqala.ast.table.{SqlTable, SqlTableAlias}
import sqala.static.dsl.Index
import sqala.static.metadata.tableCte

import scala.NamedTuple.NamedTuple
import scala.compiletime.constValue

case class RecursiveTable[N <: Tuple, V <: Tuple](
    private[sqala] val __aliasName__ : Option[String],
    private[sqala] val __items__ : V,
    private[sqala] val __sqlTable__ : SqlTable.Ident
) extends Selectable:
    type Fields = NamedTuple[N, V]

    inline def selectDynamic(name: String): Any =
        val index = constValue[Index[N, name.type, 0]]
        __items__.toList(index)

object RecursiveTable:
    def apply[N <: Tuple, V <: Tuple](alias: Option[String])(using 
        p: AsTableParam[V]
    ): RecursiveTable[N, V] =
        new RecursiveTable(
            alias, 
            p.asTableParam(alias, 1),
            SqlTable.Ident(
                tableCte,
                None,
                alias.map(SqlTableAlias(_, Nil)),
                None,
                None
            )
        )