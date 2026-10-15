package sqala.static.dsl.table

import sqala.ir.IrExpr
import sqala.ir.IrTable

import sqala.metadata.TableMetaData
import sqala.static.dsl.*

import scala.NamedTuple.{DropNames, From, NamedTuple, Names}
import scala.compiletime.constValueTuple

/**
 * A table reference produced by `from` when an entity companion object
 * is passed, mapping entity class fields to typed expressions via
 * `Fields`.
 */
final case class Table[T, K[_ <: Int] <: ExprKind, L <: Int](
    private[sqala] val __table__ : IrTable,
    private[sqala] val __metaData__ : TableMetaData
) extends Selectable with AnyTable:
    /**
     * The structural type declaring available columns as a named tuple.
     * Required by `Selectable`.
     */
    type Fields =
        NamedTuple[
            Names[From[Unwrap[T, Option]]],
            Tuple.Map[DropNames[From[Unwrap[T, Option]]], [x] =>> MapField[x, T, L]]
        ]

    /**
     * Runtime column accessor. Required by `Selectable`.
     */
    def selectDynamic(name: String): Any =
        val index = __metaData__.fields.toList.indexOf(name)
        ColumnExpr(IrExpr.Column(__table__, __metaData__.columns.toList(index)))

/**
  * A table reference produced by `from` when a `MappedTable` is passed,
  * mapping entity class fields to typed expressions via `Fields`.
  */
final case class MappedTable[N <: Tuple, V <: Tuple, L <: Int](
    private[sqala] val __table__ : IrTable,
    private[sqala] val __items__ : V,
    private[sqala] val __fields__ : List[String]
) extends Selectable with AnyTable:
    /**
     * The structural type declaring available columns as a named tuple.
     * Required by `Selectable`.
     */
    type Fields = NamedTuple[N, V]

    /**
     * Runtime column accessor. Required by `Selectable`.
     */
    def selectDynamic(name: String): Any =
        val index = __fields__.toList.indexOf(name)
        __items__.toList(index)

object MappedTable:
    inline def apply[N <: Tuple, V <: Tuple, L <: Int](table: IrTable)(using
        p: AsTableParam[V, L],
        t: ToTuple[p.R]
    ): MappedTable[N, t.R, L] =
        val fields = constValueTuple[N].toArray.toList.map(_.toString)
        new MappedTable(
            table,
            t.toTuple(p.asTableParam(table, 1)),
            fields
        )