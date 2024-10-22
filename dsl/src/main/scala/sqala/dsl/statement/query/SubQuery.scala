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

    inline def * : Fields =
        val columns = (0 until __columnSize__).toArray.map: i =>
            Expr.Column(__alias__, s"c${i}")
        val columnTuple = Tuple.fromArray(columns.toArray)
        NamedTuple(columnTuple).asInstanceOf[Fields]