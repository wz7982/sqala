package sqala.static.dsl.table

import sqala.ast.expr.SqlExpr
import sqala.ast.table.SqlTable
import sqala.static.dsl.*
import sqala.static.metadata.TableMetaData

import scala.NamedTuple.{DropNames, From, NamedTuple, Names}

case class Table[T, K <: ExprKind, F <: InFrom](
    private[sqala] val __aliasName__ : Option[String],
    private[sqala] val __metaData__ : TableMetaData,
    private[sqala] val __sqlTable__ : SqlTable.Ident
) extends Selectable with AnyTable:
    type Fields =
        NamedTuple[
            Names[From[Unwrap[T, Option]]],
            Tuple.Map[DropNames[From[Unwrap[T, Option]]], [x] =>> MapField[x, T, K]]
        ]

    def selectDynamic(name: String): Any =
        val index = __metaData__.fieldNames.indexWhere(f => f == name)
        Expr(SqlExpr.Column(__aliasName__, __metaData__.columnNames(index)))