package sqala.dsl.statement.query

import sqala.dsl.{ColumnKind, Expr, Index}

import scala.NamedTuple.*
import scala.compiletime.constValue

class SubQuery[N <: Tuple, V <: Tuple](
    private[sqala] val __alias__ : String,
    private[sqala] val __columnSize__ : Int
)(using QueryContext) extends Selectable:
    type Fields = NamedTuple[N, V]

    inline def selectDynamic(name: String): Expr[?, ColumnKind] = 
        val index = constValue[Index[N, name.type, 0]]
        Expr.Column(__alias__, s"c${index}")