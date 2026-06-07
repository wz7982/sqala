package sqala.static.dsl.table

import sqala.static.dsl.{ExprKind, ToTuple, TransformExprKind}

/**
 * Transforms the expression kind of table fields, used when table
 * columns are re-categorized.
 */
trait TransformTableKind[T, K[_ <: Int] <: ExprKind]:
    /**
     * The transformed table type.
     */
    type R

    /**
     * Applies the kind transformation to the table.
     */
    def transform(x: T): R

object TransformTableKind:
    type Aux[T, K[_ <: Int] <: ExprKind, O] = TransformTableKind[T, K]:
        type R = O

    given table[T, TK[_ <: Int] <: ExprKind, L <: Int, K[_ <: Int] <: ExprKind]: Aux[Table[T, TK, L], K, Table[T, K, L]] =
        new TransformTableKind[Table[T, TK, L], K]:
            type R = Table[T, K, L]

            def transform(x: Table[T, TK, L]): R =
                Table(x.__aliasName__, x.__metaData__, x.__sqlTable__)

    given funcTable[T, TK[_ <: Int] <: ExprKind, L <: Int, K[_ <: Int] <: ExprKind]: Aux[FuncTable[T, TK, L], K, FuncTable[T, K, L]] =
        new TransformTableKind[FuncTable[T, TK, L], K]:
            type R = FuncTable[T, K, L]

            def transform(x: FuncTable[T, TK, L]): R =
                FuncTable(x.__aliasName__, x.__fieldNames__, x.__columnNames__, x.__sqlTable__)

    given excludedTable[N <: Tuple, V <: Tuple, L <: Int, K[_ <: Int] <: ExprKind](using
        tv: TransformExprKind[V, K[L]],
        tt: ToTuple[tv.R]
    ): Aux[ExcludedTable[N, V, L], K, ExcludedTable[N, tt.R, L]] =
        new TransformTableKind[ExcludedTable[N, V, L], K]:
            type R = ExcludedTable[N, tt.R, L]

            def transform(x: ExcludedTable[N, V, L]): R =
                ExcludedTable(x.__aliasName__, tt.toTuple(tv.transform(x.__items__.asInstanceOf[V])), x.__sqlTable__)

    given jsonTable[N <: Tuple, V <: Tuple, L <: Int, K[_ <: Int] <: ExprKind](using
        tv: TransformExprKind[V, K[L]],
        tt: ToTuple[tv.R]
    ): Aux[JsonTable[N, V, L], K, JsonTable[N, tt.R, L]] =
        new TransformTableKind[JsonTable[N, V, L], K]:
            type R = JsonTable[N, tt.R, L]

            def transform(x: JsonTable[N, V, L]): R =
                JsonTable(x.__aliasName__, tt.toTuple(tv.transform(x.__items__)), x.__sqlTable__)

    given subqueryTable[N <: Tuple, V <: Tuple, L <: Int, K[_ <: Int] <: ExprKind](using
        tv: TransformExprKind[V, K[L]],
        tt: ToTuple[tv.R]
    ): Aux[SubqueryTable[N, V, L], K, SubqueryTable[N, tt.R, L]] =
        new TransformTableKind[SubqueryTable[N, V, L], K]:
            type R = SubqueryTable[N, tt.R, L]

            def transform(x: SubqueryTable[N, V, L]): R =
                SubqueryTable(x.__aliasName__, tt.toTuple(tv.transform(x.__items__)), x.__sqlTable__)

    given recognizeTable[N <: Tuple, V <: Tuple, L <: Int, K[_ <: Int] <: ExprKind](using
        tv: TransformExprKind[V, K[L]],
        tt: ToTuple[tv.R]
    ): Aux[RecognizeTable[N, V, L], K, RecognizeTable[N, tt.R, L]] =
        new TransformTableKind[RecognizeTable[N, V, L], K]:
            type R = RecognizeTable[N, tt.R, L]

            def transform(x: RecognizeTable[N, V, L]): R =
                RecognizeTable(x.__aliasName__, tt.toTuple(tv.transform(x.__items__)), x.__sqlTable__)

    given graphTable[N <: Tuple, V <: Tuple, L <: Int, K[_ <: Int] <: ExprKind](using
        tv: TransformExprKind[V, K[L]],
        tt: ToTuple[tv.R]
    ): Aux[GraphTable[N, V, L], K, GraphTable[N, tt.R, L]] =
        new TransformTableKind[GraphTable[N, V, L], K]:
            type R = GraphTable[N, tt.R, L]

            def transform(x: GraphTable[N, V, L]): R =
                GraphTable(x.__aliasName__, tt.toTuple(tv.transform(x.__items__)), x.__sqlTable__)

    given tuple[H, T <: Tuple, K[_ <: Int] <: ExprKind](using
        ah: TransformTableKind[H, K],
        at: TransformTableKind[T, K],
        t: ToTuple[at.R]
    ): Aux[H *: T, K, ah.R *: t.R] =
        new TransformTableKind[H *: T, K]:
            type R = ah.R *: t.R

            def transform(x: H *: T): R =
                ah.transform(x.head) *: t.toTuple(at.transform(x.tail))

    given tuple1[H, K[_ <: Int] <: ExprKind](using
        h: TransformTableKind[H, K]
    ): Aux[H *: EmptyTuple, K, h.R] =
        new TransformTableKind[H *: EmptyTuple, K]:
            type R = h.R

            def transform(x: H *: EmptyTuple): R =
                h.transform(x.head)