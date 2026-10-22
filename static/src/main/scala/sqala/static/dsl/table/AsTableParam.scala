package sqala.static.dsl.table

import sqala.ast.expr.SqlExpr
import sqala.metadata.{TableMacro, TableMetaData}
import sqala.static.dsl.*
import sqala.ir.IrExpr
import sqala.ir.IrTable

/**
 * Generates column expressions (as `c1`, `c2`, ...) for subquery
 * and table function result items. `CL` is the current query context
 * level.
 */
trait AsTableParam[T, CL <: Int]:
    /**
     * The result type.
     */
    type R

    /**
     * The number of columns consumed by this item.
     */
    def offset: Int

    /**
     * Produces the column expression at the given cursor position.
     */
    def asTableParam(table: IrTable, cursor: Int): R

object AsTableParam:
    type Aux[T, CL <: Int, O] = AsTableParam[T, CL]:
        type R = O

    given expr[T, EK <: ExprKind, CL <: Int]: Aux[Expr[T, EK], CL, ColumnExpr[T, CL]] =
        new AsTableParam[Expr[T, EK], CL]:
            type R = ColumnExpr[T, CL]

            def offset: Int =
                1

            def asTableParam(table: IrTable, cursor: Int): ColumnExpr[T, CL] =
                ColumnExpr(IrExpr.Column(table, s"c$cursor"))

    inline given table[T, L <: Int, CL <: Int]: Aux[Table[T, Column, L], CL, Table[T, Column, CL]] =
        val metaData: TableMetaData = TableMacro.tableMetaData[Unwrap[T, Option]]
        createTableInstance[T, L, CL](metaData)

    private def createTableInstance[T, L <: Int, CL <: Int](metaData: TableMetaData): Aux[Table[T, Column, L], CL, Table[T, Column, CL]] =
        new AsTableParam[Table[T, Column, L], CL]:
            type R = Table[T, Column, CL]

            def offset: Int =
                metaData.columns.size

            def asTableParam(table: IrTable, cursor: Int): Table[T, Column, CL] =
                Table(
                    table,
                    metaData.copy(columns = metaData.columns.indices.map(i => s"c${cursor + i}"))
                )

    inline given mappedTable[N <: Tuple, V <: Tuple, RV <: Tuple, O <: Boolean, L <: Int, CL <: Int](using
        a: AsTableParam[V, CL],
        tt: ToTuple[a.R]
    ): Aux[MappedTable[N, V, RV, O, L], CL, MappedTable[N, tt.R, RV, O, CL]] =
        new AsTableParam[MappedTable[N, V, RV, O, L], CL]:
            type R = MappedTable[N, tt.R, RV, O, CL]

            def offset: Int =
                a.offset

            def asTableParam(table: IrTable, cursor: Int): R =
                MappedTable[N, tt.R, RV, O, CL](table, tt.toTuple(a.asTableParam(table, cursor)))

    given tuple[H, T <: Tuple, CL <: Int](using
        h: AsTableParam[H, CL],
        t: AsTableParam[T, CL],
        tt: ToTuple[t.R]
    ): Aux[H *: T, CL, h.R *: tt.R] =
        new AsTableParam[H *: T, CL]:
            type R = h.R *: tt.R

            def offset: Int =
                h.offset + t.offset

            def asTableParam(table: IrTable, cursor: Int): R =
                h.asTableParam(table, cursor) *:
                tt.toTuple(t.asTableParam(table, cursor + h.offset))

    given tuple1[H, CL <: Int](using h: AsTableParam[H, CL]): Aux[H *: EmptyTuple, CL, h.R *: EmptyTuple] =
        new AsTableParam[H *: EmptyTuple, CL]:
            type R = h.R *: EmptyTuple

            def offset: Int =
                h.offset

            def asTableParam(table: IrTable, cursor: Int): R =
                h.asTableParam(table, cursor) *: EmptyTuple