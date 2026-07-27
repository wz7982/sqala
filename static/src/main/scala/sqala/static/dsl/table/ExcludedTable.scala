package sqala.static.dsl.table

import sqala.ast.expr.SqlExpr
import sqala.ast.table.{SqlTable, SqlTableAlias}
import sqala.metadata.TableMetaData
import sqala.static.dsl.*

import scala.NamedTuple.{DropNames, Names}
import scala.compiletime.constValueTuple

/**
 * A table reference with certain columns excluded, used by the `exclude`
 * method to omit specific fields from query projection.
 */
final case class FromExcluded[N <: Tuple, V <: Tuple, CL <: Int](
    private[sqala] val __aliasName__ : String,
    private[sqala] val __items__ : V,
    private[sqala] val __sqlTable__ : SqlTable.Ident
) extends AnyTable

object FromExcluded:
    inline def apply[T, EN <: Tuple, CL <: Int](
        table: Table[T, Column, CL]
    ): FromExcluded[ExcludeName[EN, Names[table.Fields]], ExcludeValue[EN, Names[table.Fields], DropNames[table.Fields]], CL] =
        val names = constValueTuple[EN].toList.map(_.asInstanceOf[String])
        val items =
            table.__metaData__.fieldNames.zip(table.__metaData__.columnNames).filter: (f, _) =>
                !names.contains(f)
            .map: (_, c) =>
                Expr(SqlExpr.Column(Some(table.__aliasName__), c))
        val tuple =
            Tuple.fromArray(items.toArray)
        val sqlTable: SqlTable.Ident =
            SqlTable.Ident(
                table.__metaData__.tableName,
                Some(SqlTableAlias(table.__aliasName__, Nil)),
                None,
                None,
                None
            )
        FromExcluded(
            table.__aliasName__,
            tuple.asInstanceOf[ExcludeValue[EN, Names[table.Fields], DropNames[table.Fields]]],
            sqlTable
        )