package sqala.static.dsl.table

import sqala.ast.expr.SqlExpr
import sqala.ast.order.SqlOrderingItem
import sqala.ast.table.*
import sqala.static.dsl.ExprKind

trait AsRecognizeTable[T]:
    def asRecognizeTable(x: T): T

    def alias(x: T, name: String): T

    def setPartitionBy(x: T, items: List[SqlExpr]): T

    def setOrderBy(x: T, items: List[SqlOrderingItem]): T

    def setPerMatch(x: T, perMatch: SqlRecognizePatternRowsPerMatchMode): T

    def setRecognize(x: T, recognize: SqlMatchRecognize): T

    def fetchRecognize(x: T): SqlMatchRecognize

    def fetchSqlTable(x: T): SqlTable

object AsRecognizeTable:
    private[sqala] def createEmptyRecognize: SqlMatchRecognize =
        SqlMatchRecognize(
            Nil,
            Nil,
            Nil,
            None,
            SqlRowPattern(
                None,
                None,
                SqlRowPatternTerm.Dollar(None),
                Nil,
                Nil
            ),
            None
        )

    given table[T, K <: ExprKind]: AsRecognizeTable[Table[T, K, CanNotInFrom]] with
        def asRecognizeTable(x: Table[T, K, CanNotInFrom]): Table[T, K, CanNotInFrom] =
            x.copy(
                __aliasName__ = None,
                __sqlTable__ = x.__sqlTable__.copy(matchRecognize = Some(createEmptyRecognize))
            )

        def alias(x: Table[T, K, CanNotInFrom], name: String): Table[T, K, CanNotInFrom] =
            x.copy(
                __aliasName__ = Some(name)
            )

        def setPartitionBy(x: Table[T, K, CanNotInFrom], items: List[SqlExpr]): Table[T, K, CanNotInFrom] =
            x.copy(
                __sqlTable__ =
                    x.__sqlTable__.copy(
                        matchRecognize =
                            x.__sqlTable__.matchRecognize.map: m =>
                                m.copy(partitionBy = items)
                    )
            )

        def setOrderBy(x: Table[T, K, CanNotInFrom], items: List[SqlOrderingItem]): Table[T, K, CanNotInFrom] =
            x.copy(
                __sqlTable__ =
                    x.__sqlTable__.copy(
                        matchRecognize =
                            x.__sqlTable__.matchRecognize.map: m =>
                                m.copy(orderBy = m.orderBy ++ items)
                    )
            )

        def setPerMatch(x: Table[T, K, CanNotInFrom], perMatch: SqlRecognizePatternRowsPerMatchMode): Table[T, K, CanNotInFrom] =
            x.copy(
                __sqlTable__ =
                    x.__sqlTable__.copy(
                        matchRecognize =
                            x.__sqlTable__.matchRecognize.map: m =>
                                m.copy(rowsPerMatch = Some(perMatch))
                    )
            )

        def setRecognize(x: Table[T, K, CanNotInFrom], recognize: SqlMatchRecognize): Table[T, K, CanNotInFrom] =
            x.copy(
                __sqlTable__ =
                    x.__sqlTable__.copy(
                        matchRecognize = Some(recognize)
                    )
            )

        def fetchRecognize(x: Table[T, K, CanNotInFrom]): SqlMatchRecognize =
            x.__sqlTable__.matchRecognize.get

        def fetchSqlTable(x: Table[T, K, CanNotInFrom]): SqlTable =
            x.__sqlTable__

    given subQuery[N <: Tuple, V <: Tuple]: AsRecognizeTable[SubQueryTable[N, V, CanNotInFrom]] with
        def asRecognizeTable(x: SubQueryTable[N, V, CanNotInFrom]): SubQueryTable[N, V, CanNotInFrom] =
            x.copy(
                __aliasName__ = None,
                __sqlTable__ = x.__sqlTable__.copy(matchRecognize = Some(createEmptyRecognize))
            )

        def alias(x: SubQueryTable[N, V, CanNotInFrom], name: String): SubQueryTable[N, V, CanNotInFrom] =
            x.copy(
                __aliasName__ = Some(name)
            )

        def setPartitionBy(x: SubQueryTable[N, V, CanNotInFrom], items: List[SqlExpr]): SubQueryTable[N, V, CanNotInFrom] =
            x.copy(
                __sqlTable__ =
                    x.__sqlTable__.copy(
                        matchRecognize =
                            x.__sqlTable__.matchRecognize.map: m =>
                                m.copy(partitionBy = items)
                    )
            )

        def setOrderBy(x: SubQueryTable[N, V, CanNotInFrom], items: List[SqlOrderingItem]): SubQueryTable[N, V, CanNotInFrom] =
            x.copy(
                __sqlTable__ =
                    x.__sqlTable__.copy(
                        matchRecognize =
                            x.__sqlTable__.matchRecognize.map: m =>
                                m.copy(orderBy = m.orderBy ++ items)
                    )
            )

        def setPerMatch(x: SubQueryTable[N, V, CanNotInFrom], perMatch: SqlRecognizePatternRowsPerMatchMode): SubQueryTable[N, V, CanNotInFrom] =
            x.copy(
                __sqlTable__ =
                    x.__sqlTable__.copy(
                        matchRecognize =
                            x.__sqlTable__.matchRecognize.map: m =>
                                m.copy(rowsPerMatch = Some(perMatch))
                    )
            )

        def setRecognize(x: SubQueryTable[N, V, CanNotInFrom], recognize: SqlMatchRecognize): SubQueryTable[N, V, CanNotInFrom] =
            x.copy(
                __sqlTable__ =
                    x.__sqlTable__.copy(
                        matchRecognize = Some(recognize)
                    )
            )

        def fetchRecognize(x: SubQueryTable[N, V, CanNotInFrom]): SqlMatchRecognize =
            x.__sqlTable__.matchRecognize.get

        def fetchSqlTable(x: SubQueryTable[N, V, CanNotInFrom]): SqlTable =
            x.__sqlTable__