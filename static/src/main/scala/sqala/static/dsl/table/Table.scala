package sqala.static.dsl.table

import sqala.ast.expr.SqlExpr
import sqala.ast.table.SqlTable
import sqala.metadata.TableMetaData
import sqala.static.dsl.*

import scala.NamedTuple.{DropNames, From, NamedTuple, Names}

/**
 * A table reference produced by `from` when an entity companion object
 * is passed, mapping entity class fields to typed expressions via
 * `Fields`.
 */
final case class Table[T, K[_ <: Int] <: ExprKind, L <: Int](
    private[sqala] val __aliasName__ : Option[String],
    private[sqala] val __metaData__ : TableMetaData,
    private[sqala] val __sqlTable__ : SqlTable.Ident
) extends Selectable with AnyTable:
    /**
     * The structural type declaring available columns as a named tuple.
     * Required by `Selectable`.
     */
    type Fields =
        NamedTuple[
            Names[From[Unwrap[T, Option]]],
            Tuple.Map[DropNames[From[Unwrap[T, Option]]], [x] =>> MapField[x, T, K, L]]
        ]

    /**
     * Runtime column accessor. Required by `Selectable`.
     */
    def selectDynamic(name: String): Any =
        val index = __metaData__.fieldNames.indexWhere(f => f == name)
        Expr(SqlExpr.Column(__aliasName__, __metaData__.columnNames(index)))