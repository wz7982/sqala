package sqala.static.dsl.table

import sqala.ast.expr.SqlExpr
import sqala.ast.table.SqlTable
import sqala.static.dsl.*
import sqala.static.metadata.TableMetaData

import scala.NamedTuple.{DropNames, NamedTuple, Names}
import scala.compiletime.{constValue, constValueTuple}

case class ExcludedTable[N <: Tuple, V <: Tuple, F <: InFrom](
    private[sqala] val __aliasName__ : Option[String],
    private[sqala] val __items__ : Any,
    private[sqala] val __sqlTable__ : SqlTable.Ident
) extends Selectable with AnyTable:
    type Fields = NamedTuple[N, V]

    inline def selectDynamic(name: String): Any =
        val index = constValue[Index[N, name.type, 0]]
        __items__.asInstanceOf[V].toList(index)

object ExcludedTable:
    inline def apply[T, EN <: Tuple](
        table: Table[T, Column, CanNotInFrom]
    ): ExcludedTable[ExcludeName[EN, Names[table.Fields]], ExcludeValue[EN, Names[table.Fields], DropNames[table.Fields]], CanInFrom] =
        val names = constValueTuple[EN].toList.map(_.asInstanceOf[String])
        val items =
            table.__metaData__.fieldNames.zip(table.__metaData__.columnNames).filter: (f, _) =>
                !names.contains(f)
            .map: (_, c) =>
                Expr(SqlExpr.Column(table.__aliasName__, c))
        val tuple = Tuple.fromArray(items.toArray).asInstanceOf[ExcludeValue[EN, Names[table.Fields], DropNames[table.Fields]]]
        new ExcludedTable(
            table.__aliasName__,
            tuple,
            table.__sqlTable__
        )