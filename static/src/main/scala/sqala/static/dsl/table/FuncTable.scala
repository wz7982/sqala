package sqala.static.dsl.table

import sqala.ast.expr.SqlExpr
import sqala.ast.table.SqlTable
import sqala.static.dsl.{Expr, MapField, Unwrap}

import scala.NamedTuple.{DropNames, From, NamedTuple, Names}

case class FuncTable[T](
    private[sqala] val __aliasName__ : Option[String],
    private[sqala] val __fieldNames__ : List[String],
    private[sqala] val __columnNames__ : List[String],
    private[sqala] val __sqlTable__ : SqlTable.Func
) extends Selectable:
    type Fields =
        NamedTuple[
            Names[From[Unwrap[T, Option]]],
            Tuple.Map[DropNames[From[Unwrap[T, Option]]], [x] =>> MapField[x, T]]
        ]

    def selectDynamic(name: String): Expr[?] =
        val index = __fieldNames__.indexWhere(f => f == name)
        Expr(SqlExpr.Column(__aliasName__, __columnNames__(index)))