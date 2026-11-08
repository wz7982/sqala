package sqala.static.ext.dsl.table

import sqala.static.dsl.ExprKind
import sqala.metadata.TableMetaData
import sqala.static.dsl.table.AnyTable
import sqala.ir.IrTable
import sqala.static.dsl.table.AsTable
import sqala.static.dsl.Column
import sqala.static.dsl.table.Table
import sqala.static.dsl.table.AsLateralTable
import scala.compiletime.ops.int.-
import sqala.static.dsl.ExcludeCurrentLevelColumn

/**
 * A table function reference, representing a table-valued function
 * such as `unnest`.
 */
final case class FromFunc[T, K[_ <: Int] <: ExprKind, OKS <: Tuple, CL <: Int](
    private[sqala] val __table__ : IrTable.Func,
    private[sqala] val __metaData__ : TableMetaData
) extends AnyTable

object FromFunc:
    given asTable[T, K[_ <: Int] <: ExprKind, TOKS <: Tuple, CL <: Int]: AsTable.Aux[FromFunc[T, Column, TOKS, CL], CL, Table[T, Column, CL], TOKS] =
        new AsTable[FromFunc[T, Column, TOKS, CL], CL]:
            type R = Table[T, Column, CL]

            type OKS = TOKS

            def asTable(x: FromFunc[T, Column, TOKS, CL]): (R, IrTable) =
                (Table(x.__table__, x.__metaData__), x.__table__)

    given asLateralTable[T, K[_ <: Int] <: ExprKind, TOKS <: Tuple, CL <: Int](using
        e: ExcludeCurrentLevelColumn[TOKS, CL - 1]
    ): AsLateralTable.Aux[FromFunc[T, Column, TOKS, CL], CL, Table[T, Column, CL - 1], e.R] =
        new AsLateralTable[FromFunc[T, Column, TOKS, CL], CL]:
            type R = Table[T, Column, CL - 1]

            type OKS = e.R

            def asTable(x: FromFunc[T, Column, TOKS, CL]): (R, IrTable) =
                val table: IrTable.Func = x.__table__.copy(withLateral = true)
                (Table(table, x.__metaData__), table)