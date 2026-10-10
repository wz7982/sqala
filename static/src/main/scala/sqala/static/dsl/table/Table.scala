package sqala.static.dsl.table

import sqala.ir.IrExpr
import sqala.ir.IrTable

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
    inline def selectDynamic(name: String): Any =
        val index = constValue[Index[Names[From[Unwrap[T, Option]]], name.type, 0]]
        ColumnExpr(IrExpr.Column(__table__, __metaData__.columns.toList(index)))