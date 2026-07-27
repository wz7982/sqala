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
                Table(x.__aliasName__, x.__metaData__)

    given mappedTable[N <: Tuple, V <: Tuple, L <: Int, K[_ <: Int] <: ExprKind](using
        tv: TransformExprKind[V, K[L]],
        tt: ToTuple[tv.R]
    ): Aux[MappedTable[N, V, L], K, MappedTable[N, tt.R, L]] =
        new TransformTableKind[MappedTable[N, V, L], K]:
            type R = MappedTable[N, tt.R, L]

            def transform(x: MappedTable[N, V, L]): R =
                MappedTable(x.__aliasName__, tt.toTuple(tv.transform(x.__items__)))

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