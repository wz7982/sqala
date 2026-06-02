package sqala.static.dsl.table

import sqala.ast.table.{SqlTable, SqlTableAlias}
import sqala.static.dsl.{tableCte, Index, ToTuple}

import scala.NamedTuple.NamedTuple
import scala.compiletime.constValue

final case class RecursiveTable[N <: Tuple, V <: Tuple, L <: Int](
    private[sqala] val __aliasName__ : Option[String],
    private[sqala] val __items__ : V,
    private[sqala] val __sqlTable__ : SqlTable.Ident
) extends Selectable with AnyTable:
    type Fields = NamedTuple[N, V]

    inline def selectDynamic(name: String): Any =
        val index = constValue[Index[N, name.type, 0]]
        __items__.toList(index)

object RecursiveTable:
    def apply[N <: Tuple, V <: Tuple, H <: Int](alias: Option[String])(using
        p: AsTableParam[V, H],
        t: ToTuple[p.R]
    ): RecursiveTable[N, t.R, H] =
        new RecursiveTable(
            alias,
            t.toTuple(p.asTableParam(alias, 1)),
            SqlTable.Ident(
                tableCte,
                alias.map(SqlTableAlias(_, Nil)),
                None,
                None,
                None
            )
        )