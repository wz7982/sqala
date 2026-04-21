package sqala.static.dsl.table

import sqala.ast.expr.SqlExpr
import sqala.ast.table.SqlTable
import sqala.static.dsl.*

import scala.NamedTuple.{DropNames, From, NamedTuple, Names}

case class FuncTable[T, K <: ExprKind, F <: InFrom](
    private[sqala] val __aliasName__ : Option[String],
    private[sqala] val __fieldNames__ : List[String],
    private[sqala] val __columnNames__ : List[String],
    private[sqala] val __sqlTable__ : SqlTable.Func
) extends Selectable with AnyTable:
    type Fields =
        NamedTuple[
            Names[From[Unwrap[T, Option]]],
            Tuple.Map[DropNames[From[Unwrap[T, Option]]], [x] =>> MapField[x, T, K]]
        ]

    def selectDynamic(name: String): Any =
        val index = __fieldNames__.indexWhere(f => f == name)
        Expr(SqlExpr.Column(__aliasName__, __columnNames__(index)))