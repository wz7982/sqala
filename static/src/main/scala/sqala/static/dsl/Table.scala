package sqala.static.dsl

import sqala.ast.expr.SqlExpr
import sqala.metadata.TableMetaData

import scala.NamedTuple.{DropNames, From, NamedTuple, Names}

case class Table[T](
    private[sqala] val __tableName__ : String,
    private[sqala] val __aliasName__ : String,
    private[sqala] val __metaData__ : TableMetaData
) extends Selectable:
    type Fields =
        NamedTuple[
            Names[From[Unwrap[T, Option]]],
            Tuple.Map[DropNames[From[Unwrap[T, Option]]], [x] =>> MapField[x, T]]
        ]

    def selectDynamic(name: String): Expr[?] =
        val index = __metaData__.fieldNames.indexWhere(f => f == name)
        Expr(SqlExpr.Column(Some(__aliasName__), __metaData__.columnNames(index)))