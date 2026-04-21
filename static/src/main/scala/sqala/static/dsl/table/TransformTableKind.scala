package sqala.static.dsl.table

import sqala.static.dsl.{ExprKind, ToTuple, TransformKind, Ungrouped}

trait TransformTableKind[T, K <: ExprKind]:
    type R

    def transform(x: T): R

object TransformTableKind:
    type Aux[T, K <: ExprKind, O] = TransformTableKind[T, K]:
        type R = O

    given table[T, TK <: ExprKind, F <: InFrom, K <: ExprKind]: Aux[Table[T, TK, F], K, Table[T, K, CanNotInFrom]] =
        new TransformTableKind[Table[T, TK, F], K]:
            type R = Table[T, K, CanNotInFrom]

            def transform(x: Table[T, TK, F]): R =
                Table(x.__aliasName__, x.__metaData__, x.__sqlTable__)

    given funcTable[T, TK <: ExprKind, F <: InFrom, K <: ExprKind]: Aux[FuncTable[T, TK, F], K, FuncTable[T, K, CanNotInFrom]] =
        new TransformTableKind[FuncTable[T, TK, F], K]:
            type R = FuncTable[T, K, CanNotInFrom]

            def transform(x: FuncTable[T, TK, F]): R =
                FuncTable(x.__aliasName__, x.__fieldNames__, x.__columnNames__, x.__sqlTable__)

    given excludedTable[N <: Tuple, V <: Tuple, F <: InFrom, K <: ExprKind](using
        tv: TransformKind[V, K],
        tt: ToTuple[tv.R]
    ): Aux[ExcludedTable[N, V, F], K, ExcludedTable[N, tt.R, CanNotInFrom]] =
        new TransformTableKind[ExcludedTable[N, V, F], K]:
            type R = ExcludedTable[N, tt.R, CanNotInFrom]

            def transform(x: ExcludedTable[N, V, F]): R =
                ExcludedTable(x.__aliasName__, tt.toTuple(tv.transform(x.__items__.asInstanceOf[V])), x.__sqlTable__)

    given jsonTable[N <: Tuple, V <: Tuple, F <: InFrom, K <: ExprKind](using
        tv: TransformKind[V, K],
        tt: ToTuple[tv.R]
    ): Aux[JsonTable[N, V, F], K, JsonTable[N, tt.R, CanNotInFrom]] =
        new TransformTableKind[JsonTable[N, V, F], K]:
            type R = JsonTable[N, tt.R, CanNotInFrom]

            def transform(x: JsonTable[N, V, F]): R =
                JsonTable(x.__aliasName__, tt.toTuple(tv.transform(x.__items__)), x.__sqlTable__)

    given subQueryTable[N <: Tuple, V <: Tuple, F <: InFrom, K <: ExprKind](using
        tv: TransformKind[V, K],
        tt: ToTuple[tv.R]
    ): Aux[SubQueryTable[N, V, F], K, SubQueryTable[N, tt.R, CanNotInFrom]] =
        new TransformTableKind[SubQueryTable[N, V, F], K]:
            type R = SubQueryTable[N, tt.R, CanNotInFrom]

            def transform(x: SubQueryTable[N, V, F]): R =
                SubQueryTable(x.__aliasName__, tt.toTuple(tv.transform(x.__items__)), x.__sqlTable__)

    given recognizeTable[N <: Tuple, V <: Tuple, F <: InFrom, K <: ExprKind](using
        tv: TransformKind[V, Ungrouped],
        tt: ToTuple[tv.R]
    ): Aux[RecognizeTable[N, V, F], K, RecognizeTable[N, tt.R, CanNotInFrom]] =
        new TransformTableKind[RecognizeTable[N, V, F], K]:
            type R = RecognizeTable[N, tt.R, CanNotInFrom]

            def transform(x: RecognizeTable[N, V, F]): R =
                RecognizeTable(x.__aliasName__, tt.toTuple(tv.transform(x.__items__)), x.__sqlTable__)

    given graphTable[N <: Tuple, V <: Tuple, F <: InFrom, K <: ExprKind](using
        tv: TransformKind[V, Ungrouped],
        tt: ToTuple[tv.R]
    ): Aux[GraphTable[N, V, F], K, GraphTable[N, tt.R, CanNotInFrom]] =
        new TransformTableKind[GraphTable[N, V, F], K]:
            type R = GraphTable[N, tt.R, CanNotInFrom]

            def transform(x: GraphTable[N, V, F]): R =
                GraphTable(x.__aliasName__, tt.toTuple(tv.transform(x.__items__)), x.__sqlTable__)

    given tuple[H, T <: Tuple, K <: ExprKind](using
        ah: TransformTableKind[H, K],
        at: TransformTableKind[T, K],
        t: ToTuple[at.R]
    ): Aux[H *: T, K, ah.R *: t.R] =
        new TransformTableKind[H *: T, K]:
            type R = ah.R *: t.R

            def transform(x: H *: T): R =
                ah.transform(x.head) *: t.toTuple(at.transform(x.tail))

    given tuple1[H, K <: ExprKind](using
        ah: TransformTableKind[H, K]
    ): Aux[H *: EmptyTuple, K, ah.R] =
        new TransformTableKind[H *: EmptyTuple, K]:
            type R = ah.R

            def transform(x: H *: EmptyTuple): R =
                ah.transform(x.head)