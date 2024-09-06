package sqala.dsl.statement.query

import sqala.dsl.{ColumnKind, Expr, Index}

import scala.NamedTuple.*
import scala.compiletime.constValue

class NamedQuery[N <: Tuple, V <: Tuple](
    private[sqala] val __query__ : Query[NamedTuple[N, V], ?], 
    private[sqala] val __alias__ : String
) extends Selectable:
    type Fields = NamedTuple[N, V]

    inline def selectDynamic(using s: SelectItem[NamedTuple[N, V]])(name: String): Expr[?, ColumnKind] = 
        val items = s.selectItems(__query__.queryItems, 0)
        val index = constValue[Index[N, name.type, 0]]
        val item = items(index)
        Expr.Column(__alias__, item.alias.get)