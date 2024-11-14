package sqala.dsl.statement.query

import sqala.dsl.Expr

import scala.NamedTuple.NamedTuple
import scala.compiletime.constValueTuple

class SortGroup[N <: Tuple, V <: Tuple](
    private[sqala] val __names__ : List[String],
    private[sqala] val __values__ : List[Expr[?]]
) extends Selectable:
    type Fields = NamedTuple[N, V]

    def selectDynamic(name: String): Expr[?] =
        __names__.zip(__values__).find(_._1 == name).map(_._2).get

object SortGroup:
    inline def apply[N <: Tuple, V <: Tuple](values: List[Expr[?]]): SortGroup[N, V] =
        val names = constValueTuple[N].toList.map(_.asInstanceOf[String])
        new SortGroup(names, values)