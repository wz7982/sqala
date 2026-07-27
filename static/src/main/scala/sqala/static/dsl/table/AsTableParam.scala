package sqala.static.dsl.table

import sqala.ast.expr.SqlExpr
import sqala.metadata.{TableMacro, TableMetaData}
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
    def asTableParam(queryAlias: String, cursor: Int): R

object AsTableParam:
    type Aux[T, CL <: Int, O] = AsTableParam[T, CL]:
        type R = O

    given expr[T, EK <: ExprKind, CL <: Int]: Aux[Expr[T, EK], CL, Expr[T, Column[CL]]] =
        new AsTableParam[Expr[T, EK], CL]:
            type R = Expr[T, Column[CL]]

            def offset: Int =
                1

            def asTableParam(queryAlias: String, cursor: Int): Expr[T, Column[CL]] =
                Expr(SqlExpr.Column(Some(queryAlias), s"c$cursor"))

    inline given table[T, L <: Int, CL <: Int]: Aux[Table[T, Column, L], CL, Table[T, Column, CL]] =
        val metaData: TableMetaData = TableMacro.tableMetaData[Unwrap[T, Option]]
        createTableInstance[T, L, CL](metaData)

    /**
      * Creates a table instance for a given table metadata.
      */
    private def createTableInstance[T, L <: Int, CL <: Int](metaData: TableMetaData): Aux[Table[T, Column, L], CL, Table[T, Column, CL]] =
        new AsTableParam[Table[T, Column, L], CL]:
            type R = Table[T, Column, CL]

            def offset: Int =
                metaData.columnNames.size

            def asTableParam(queryAlias: String, cursor: Int): Table[T, Column, CL] =
                Table(
                    queryAlias,
                    metaData.copy(columnNames = metaData.columnNames.indices.toList.map(i => s"c${cursor + i}"))
                )

    given subqueryTable[N <: Tuple, V <: Tuple, L <: Int, CL <: Int](using
        a: AsTableParam[V, CL],
        tt: ToTuple[a.R]
    ): Aux[SubqueryTable[N, V, L], CL, SubqueryTable[N, tt.R, CL]] =
        new AsTableParam[SubqueryTable[N, V, L], CL]:
            type R = SubqueryTable[N, tt.R, CL]

            def offset: Int =
                a.offset

            def asTableParam(queryAlias: String, cursor: Int): R =
                SubqueryTable[N, V, CL](queryAlias)

    given excludedTable[N <: Tuple, V <: Tuple, L <: Int, CL <: Int](using
        a: AsTableParam[V, CL],
        tt: ToTuple[a.R]
    ): Aux[ExcludedTable[N, V, L], CL, ExcludedTable[N, tt.R, CL]] =
        new AsTableParam[ExcludedTable[N, V, L], CL]:
            type R = ExcludedTable[N, tt.R, CL]

            def offset: Int =
                a.offset

            def asTableParam(queryAlias: String, cursor: Int): R =
                ExcludedTable(
                    queryAlias,
                    tt.toTuple(a.asTableParam(queryAlias, cursor))
                )

    given jsonTable[N <: Tuple, V <: Tuple, L <: Int, CL <: Int](using
        a: AsTableParam[V, CL],
        tt: ToTuple[a.R]
    ): Aux[JsonTable[N, V, L], CL, JsonTable[N, tt.R, CL]] =
        new AsTableParam[JsonTable[N, V, L], CL]:
            type R = JsonTable[N, tt.R, CL]

            def offset: Int =
                a.offset

            def asTableParam(queryAlias: String, cursor: Int): R =
                JsonTable(
                    queryAlias,
                    tt.toTuple(a.asTableParam(queryAlias, cursor))
                )

    inline given funcTable[T, L <: Int, CL <: Int]: Aux[FuncTable[T, Column, L], CL, FuncTable[T, Column, CL]] =
        val metaData: TableMetaData = TableMacro.tableMetaData[Unwrap[T, Option]]
        createFuncTableInstance[T, L, CL](metaData)

    /**
      * Creates a function table instance for a given table metadata.
      */
    private def createFuncTableInstance[T, L <: Int, CL <: Int](metaData: TableMetaData): Aux[FuncTable[T, Column, L], CL, FuncTable[T, Column, CL]] =
        new AsTableParam[FuncTable[T, Column, L], CL]:
            type R = FuncTable[T, Column, CL]

            def offset: Int =
                metaData.columnNames.size

            def asTableParam(queryAlias: String, cursor: Int): FuncTable[T, Column, CL] =
                FuncTable(
                    queryAlias,
                    metaData.fieldNames,
                    metaData.columnNames.indices.toList.map(i => s"c${cursor + i}")
                )

    given graphTable[N <: Tuple, V <: Tuple, L <: Int, CL <: Int](using
        a: AsTableParam[V, CL],
        tt: ToTuple[a.R]
    ): Aux[GraphTable[N, V, L], CL, GraphTable[N, tt.R, CL]] =
        new AsTableParam[GraphTable[N, V, L], CL]:
            type R = GraphTable[N, tt.R, CL]

            def offset: Int =
                a.offset

            def asTableParam(queryAlias: String, cursor: Int): R =
                GraphTable(
                    queryAlias,
                    tt.toTuple(a.asTableParam(queryAlias, cursor))
                )

    given recognizeTable[N <: Tuple, V <: Tuple, L <: Int, CL <: Int](using
        a: AsTableParam[V, CL],
        tt: ToTuple[a.R]
    ): Aux[RecognizeTable[N, V, L], CL, RecognizeTable[N, tt.R, CL]] =
        new AsTableParam[RecognizeTable[N, V, L], CL]:
            type R = RecognizeTable[N, tt.R, CL]

            def offset: Int =
                a.offset

            def asTableParam(queryAlias: String, cursor: Int): R =
                RecognizeTable(
                    queryAlias,
                    tt.toTuple(a.asTableParam(queryAlias, cursor))
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

            def asTableParam(queryAlias: String, cursor: Int): R =
                h.asTableParam(queryAlias, cursor) *:
                tt.toTuple(t.asTableParam(queryAlias, cursor + h.offset))

    given tuple1[H, CL <: Int](using h: AsTableParam[H, CL]): Aux[H *: EmptyTuple, CL, h.R *: EmptyTuple] =
        new AsTableParam[H *: EmptyTuple, CL]:
            type R = h.R *: EmptyTuple

            def offset: Int =
                h.offset

            def asTableParam(queryAlias: String, cursor: Int): R =
                h.asTableParam(queryAlias, cursor) *: EmptyTuple