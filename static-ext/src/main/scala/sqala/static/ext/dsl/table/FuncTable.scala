package sqala.static.ext.dsl.table

import sqala.ir.IrTable
import sqala.metadata.TableMetaData
import sqala.static.dsl.{Column, ExcludeCurrentLevelColumn, ExprKind}
import sqala.static.dsl.table.{AnyTable, AsLateralTable, AsTable, Table}

import scala.compiletime.ops.int.-

/**
 * A table function reference, representing a table-valued function
 * such as `unnest`.
 */
final class FromFunc[T, K[_ <: Int] <: ExprKind, OKS <: Tuple, CL <: Int](
    private[sqala] val table: IrTable.Func,
    private[sqala] val metaData: TableMetaData
) extends AnyTable

object FromFunc:
    given asTable[T, K[_ <: Int] <: ExprKind, TOKS <: Tuple, CL <: Int]: AsTable.Aux[FromFunc[T, Column, TOKS, CL], CL, Table[T, Column, CL], TOKS] =
        new AsTable[FromFunc[T, Column, TOKS, CL], CL]:
            type R = Table[T, Column, CL]

            type OKS = TOKS

            def asTable(x: FromFunc[T, Column, TOKS, CL]): (R, IrTable) =
                (Table(x.table, x.metaData), x.table)

    given asLateralTable[T, K[_ <: Int] <: ExprKind, TOKS <: Tuple, CL <: Int](using
        e: ExcludeCurrentLevelColumn[TOKS, CL - 1]
    ): AsLateralTable.Aux[FromFunc[T, Column, TOKS, CL], CL, Table[T, Column, CL - 1], e.R] =
        new AsLateralTable[FromFunc[T, Column, TOKS, CL], CL]:
            type R = Table[T, Column, CL - 1]

            type OKS = e.R

            def asTable(x: FromFunc[T, Column, TOKS, CL]): (R, IrTable) =
                val table: IrTable.Func = x.table.copy(withLateral = true)
                (Table(table, x.metaData), table)