package sqala.static.dsl.table

import sqala.ast.expr.SqlExpr
import sqala.ast.table.{SqlTable, SqlTableAlias}
import sqala.metadata.TableMacro
import sqala.static.dsl.*

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
    def asTableParam(queryAlias: Option[String], cursor: Int): R

object AsTableParam:
    type Aux[T, CL <: Int, O] = AsTableParam[T, CL]:
        type R = O

    given expr[T, EK <: ExprKind, CL <: Int]: Aux[Expr[T, EK], CL, Expr[T, Column[CL]]] =
        new AsTableParam[Expr[T, EK], CL]:
            type R = Expr[T, Column[CL]]

            def offset: Int =
                1

            def asTableParam(queryAlias: Option[String], cursor: Int): Expr[T, Column[CL]] =
                Expr(SqlExpr.Column(queryAlias, s"c$cursor"))

    inline given table[T, L <: Int, CL <: Int]: Aux[Table[T, Column, L], CL, Table[T, Column, CL]] =
        new AsTableParam[Table[T, Column, L], CL]:
            type R = Table[T, Column, CL]

            def offset: Int =
                TableMacro.tableMetaData[Unwrap[T, Option]].columnNames.size

            def asTableParam(queryAlias: Option[String], cursor: Int): Table[T, Column, CL] =
                val metaData = TableMacro.tableMetaData[Unwrap[T, Option]]
                val sqlTable: SqlTable.Ident =
                    SqlTable.Ident(
                        metaData.tableName,
                        queryAlias.map: a =>
                            SqlTableAlias(a, Nil),
                        None,
                        None,
                        None
                    )
                Table(
                    queryAlias,
                    metaData.copy(columnNames = metaData.columnNames.indices.toList.map(i => s"c${cursor + i}")),
                    sqlTable
                )

    given tuple[H, T <: Tuple, CL <: Int](using
        h: AsTableParam[H, CL],
        t: AsTableParam[T, CL],
        tt: ToTuple[t.R]
    ): Aux[H *: T, CL, h.R *: tt.R] =
        new AsTableParam[H *: T, CL]:
            type R = h.R *: tt.R

            def offset: Int =
                h.offset + t.offset

            def asTableParam(queryAlias: Option[String], cursor: Int): R =
                h.asTableParam(queryAlias, cursor) *: 
                tt.toTuple(t.asTableParam(queryAlias, cursor + h.offset))

    given tuple1[H, CL <: Int](using h: AsTableParam[H, CL]): Aux[H *: EmptyTuple, CL, h.R *: EmptyTuple] =
        new AsTableParam[H *: EmptyTuple, CL]:
            type R = h.R *: EmptyTuple

            def offset: Int =
                h.offset

            def asTableParam(queryAlias: Option[String], cursor: Int): R =
                h.asTableParam(queryAlias, cursor) *: EmptyTuple