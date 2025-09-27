package sqala.static.dsl.table

import sqala.ast.expr.SqlExpr
import sqala.ast.table.SqlTable
import sqala.static.dsl.{Expr, MapField, Unwrap}
import sqala.static.metadata.TableMetaData

import scala.NamedTuple.{DropNames, From, NamedTuple, Names}

case class Table[T](
    private[sqala] val __aliasName__ : Option[String],
    private[sqala] val __metaData__ : TableMetaData,
    private[sqala] val __sqlTable__ : SqlTable.Standard
) extends Selectable:
    type Fields =
        NamedTuple[
            Names[From[Unwrap[T, Option]]],
            Tuple.Map[DropNames[From[Unwrap[T, Option]]], [x] =>> MapField[x, T]]
        ]

    def selectDynamic(name: String): Expr[?] =
        val index = __metaData__.fieldNames.indexWhere(f => f == name)
        Expr(SqlExpr.Column(__aliasName__, __metaData__.columnNames(index)))

case class UngroupedTable[T](
    private[sqala] val __aliasName__ : Option[String],
    private[sqala] val __metaData__ : TableMetaData,
    private[sqala] val __sqlTable__ : SqlTable.Standard
) extends Selectable:
    type Fields =
        NamedTuple[
            Names[From[Unwrap[T, Option]]],
            Tuple.Map[DropNames[From[Unwrap[T, Option]]], [x] =>> MapField[x, T]]
        ]

    def selectDynamic(name: String): Expr[?] =
        val index = __metaData__.fieldNames.indexWhere(f => f == name)
        Expr(SqlExpr.Column(__aliasName__, __metaData__.columnNames(index)))