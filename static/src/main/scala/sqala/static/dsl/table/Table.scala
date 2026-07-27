package sqala.static.dsl.table

import sqala.ast.expr.SqlExpr
import sqala.metadata.TableMetaData
import sqala.static.dsl.*

import scala.NamedTuple.{DropNames, From, NamedTuple, Names}
import scala.compiletime.constValue

/**
 * A table reference produced by `from` when an entity companion object
 * is passed, mapping entity class fields to typed expressions via
 * `Fields`.
 */
final case class Table[T, K[_ <: Int] <: ExprKind, L <: Int](
    private[sqala] val __aliasName__ : String,
    private[sqala] val __metaData__ : TableMetaData
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
        Expr(SqlExpr.Column(Some(__aliasName__), __metaData__.columnNames(index)))

/**
  * A table reference produced by `from` when a `MappedTable` is passed,
  * mapping entity class fields to typed expressions via `Fields`.
  */
final case class MappedTable[N <: Tuple, V <: Tuple, L <: Int](
    private[sqala] val __aliasName__ : String,
    private[sqala] val __items__ : V
) extends Selectable with AnyTable:
    /**
     * The structural type declaring available columns as a named tuple.
     * Required by `Selectable`.
     */
    type Fields = NamedTuple[N, V]

    /**
     * Runtime column accessor. Required by `Selectable`.
     */
    inline def selectDynamic(name: String): Any =
        val index = constValue[Index[N, name.type, 0]]
        __items__.toList(index)

object MappedTable:
    def apply[N <: Tuple, V <: Tuple, L <: Int](alias: String)(using
        p: AsTableParam[V, L],
        t: ToTuple[p.R]
    ): MappedTable[N, t.R, L] =
        new MappedTable(
            alias,
            t.toTuple(p.asTableParam(alias, 1))
        )