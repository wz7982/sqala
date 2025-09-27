package sqala.static.dsl.table

import sqala.static.dsl.ToTuple

trait AsUngroupedTable[T]:
    type R

    def asUngroupedTable(x: T): R

object AsUngroupedTable:
    type Aux[T, O] = AsUngroupedTable[T]:
        type R = O

    given table[T]: Aux[Table[T], UngroupedTable[T]] =
        new AsUngroupedTable[Table[T]]:
            type R = UngroupedTable[T]

            def asUngroupedTable(x: Table[T]): R =
                UngroupedTable(x.__aliasName__, x.__metaData__, x.__sqlTable__)

    given funcTable[T]: Aux[FuncTable[T], UngroupedFuncTable[T]] =
        new AsUngroupedTable[FuncTable[T]]:
            type R = UngroupedFuncTable[T]

            def asUngroupedTable(x: FuncTable[T]): R =
                UngroupedFuncTable(x.__aliasName__, x.__fieldNames__, x.__columnNames__, x.__sqlTable__)

    given jsonTable[N <: Tuple, V <: Tuple]: Aux[JsonTable[N, V], UngroupedJsonTable[N, V]] =
        new AsUngroupedTable[JsonTable[N, V]]:
            type R = UngroupedJsonTable[N, V]

            def asUngroupedTable(x: JsonTable[N, V]): R =
                UngroupedJsonTable(x.__aliasName__, x.__items__, x.__sqlTable__)

    given subQueryTable[N <: Tuple, V <: Tuple]: Aux[SubQueryTable[N, V], UngroupedSubQueryTable[N, V]] =
        new AsUngroupedTable[SubQueryTable[N, V]]:
            type R = UngroupedSubQueryTable[N, V]

            def asUngroupedTable(x: SubQueryTable[N, V]): R =
                UngroupedSubQueryTable(x.__aliasName__, x.__items__, x.__sqlTable__)

    given recognizeTable[N <: Tuple, V <: Tuple]: Aux[RecognizeTable[N, V], UngroupedRecognizeTable[N, V]] =
        new AsUngroupedTable[RecognizeTable[N, V]]:
            type R = UngroupedRecognizeTable[N, V]

            def asUngroupedTable(x: RecognizeTable[N, V]): R =
                UngroupedRecognizeTable(x.__aliasName__, x.__items__, x.__sqlTable__)

    given graphTable[N <: Tuple, V <: Tuple]: Aux[GraphTable[N, V], UngroupedGraphTable[N, V]] =
        new AsUngroupedTable[GraphTable[N, V]]:
            type R = UngroupedGraphTable[N, V]

            def asUngroupedTable(x: GraphTable[N, V]): R =
                UngroupedGraphTable(x.__aliasName__, x.__items__, x.__sqlTable__)

    given tuple[H: AsUngroupedTable as ah, T <: Tuple : AsUngroupedTable as at](using
        t: ToTuple[at.R]
    ): Aux[H *: T, ah.R *: t.R] =
        new AsUngroupedTable[H *: T]:
            type R = ah.R *: t.R

            def asUngroupedTable(x: H *: T): R =
                ah.asUngroupedTable(x.head) *: t.toTuple(at.asUngroupedTable(x.tail))

    given tuple1[H: AsUngroupedTable as ah]: Aux[H *: EmptyTuple, ah.R] =
        new AsUngroupedTable[H *: EmptyTuple]:
            type R = ah.R

            def asUngroupedTable(x: H *: EmptyTuple): R =
                ah.asUngroupedTable(x.head)