package sqala.static.dsl.table

import sqala.ast.expr.SqlExpr
import sqala.ast.table.SqlTable
import sqala.static.dsl.*

import scala.NamedTuple.{DropNames, From, NamedTuple, Names}

final case class FromFunc[T, K[_ <: Int] <: ExprKind, OKS <: Tuple, CL <: Int](
    private[sqala] val __aliasName__ : Option[String],
    private[sqala] val __fieldNames__ : List[String],
    private[sqala] val __columnNames__ : List[String],
    private[sqala] val __sqlTable__ : SqlTable.Func
) extends AnyTable

final case class FuncTable[T, K[_ <: Int] <: ExprKind, L <: Int](
    private[sqala] val __aliasName__ : Option[String],
    private[sqala] val __fieldNames__ : List[String],
    private[sqala] val __columnNames__ : List[String],
    private[sqala] val __sqlTable__ : SqlTable.Func
) extends Selectable with AnyTable:
    type Fields =
        NamedTuple[
            Names[From[Unwrap[T, Option]]],
            Tuple.Map[DropNames[From[Unwrap[T, Option]]], [x] =>> MapField[x, T, K, L]]
        ]

    def selectDynamic(name: String): Any =
        val index = __fieldNames__.indexWhere(f => f == name)
        Expr(SqlExpr.Column(__aliasName__, __columnNames__(index)))