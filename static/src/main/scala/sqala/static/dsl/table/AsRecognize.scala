package sqala.static.dsl.table

import sqala.ast.expr.SqlExpr
import sqala.ast.order.SqlOrderingItem
import sqala.ast.table.*
import sqala.static.dsl.ExprKind

trait AsRecognize[T]:
    def asRecognizeTable(x: T): T

    def alias(x: T, name: String): T

    def setPartitionBy(x: T, items: List[SqlExpr]): T

    def setOrderBy(x: T, items: List[SqlOrderingItem]): T

    def setPerMatch(x: T, perMatch: SqlRecognizePatternRowsPerMatchMode): T

    def setRecognize(x: T, recognize: SqlMatchRecognize): T

    def fetchRecognize(x: T): SqlMatchRecognize

    def fetchSqlTable(x: T): SqlTable

object AsRecognize:
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

    given table[T, K[_ <: Int] <: ExprKind, L <: Int]: AsRecognize[Table[T, K, L]] with
        def asRecognizeTable(x: Table[T, K, L]): Table[T, K, L] =
            x.copy(
                __sqlTable__ = x.__sqlTable__.copy(matchRecognize = Some(createEmptyRecognize))
            )

        def alias(x: Table[T, K, L], name: String): Table[T, K, L] =
            x.copy(
                __aliasName__ = Some(name)
            )

        def setPartitionBy(x: Table[T, K, L], items: List[SqlExpr]): Table[T, K, L] =
            x.copy(
                __sqlTable__ = x.__sqlTable__.copy(
                    matchRecognize =
                        x.__sqlTable__.matchRecognize.map: m =>
                            m.copy(partitionBy = items)
                )
            )

        def setOrderBy(x: Table[T, K, L], items: List[SqlOrderingItem]): Table[T, K, L] =
            x.copy(
                __sqlTable__ = x.__sqlTable__.copy(
                    matchRecognize =
                        x.__sqlTable__.matchRecognize.map: m =>
                            m.copy(orderBy = m.orderBy ++ items)
                )
            )

        def setPerMatch(x: Table[T, K, L], perMatch: SqlRecognizePatternRowsPerMatchMode): Table[T, K, L] =
            x.copy(
                __sqlTable__ = x.__sqlTable__.copy(
                    matchRecognize =
                        x.__sqlTable__.matchRecognize.map: m =>
                            m.copy(rowsPerMatch = Some(perMatch))
                    )
            )

        def setRecognize(x: Table[T, K, L], recognize: SqlMatchRecognize): Table[T, K, L] =
            x.copy(
                __sqlTable__ = x.__sqlTable__.copy(
                    matchRecognize = Some(recognize)
                )
            )

        def fetchRecognize(x: Table[T, K, L]): SqlMatchRecognize =
            x.__sqlTable__.matchRecognize.get

        def fetchSqlTable(x: Table[T, K, L]): SqlTable =
            x.__sqlTable__

    given subquery[N <: Tuple, V <: Tuple, L <: Int]: AsRecognize[SubqueryTable[N, V, L]] with
        def asRecognizeTable(x: SubqueryTable[N, V, L]): SubqueryTable[N, V, L] =
            x.copy(
                __sqlTable__ = x.__sqlTable__.copy(matchRecognize = Some(createEmptyRecognize))
            )

        def alias(x: SubqueryTable[N, V, L], name: String): SubqueryTable[N, V, L] =
            x.copy(
                __aliasName__ = Some(name)
            )

        def setPartitionBy(x: SubqueryTable[N, V, L], items: List[SqlExpr]): SubqueryTable[N, V, L] =
            x.copy(
                __sqlTable__ = x.__sqlTable__.copy(
                    matchRecognize =
                        x.__sqlTable__.matchRecognize.map: m =>
                            m.copy(partitionBy = items)
                    )
            )

        def setOrderBy(x: SubqueryTable[N, V, L], items: List[SqlOrderingItem]): SubqueryTable[N, V, L] =
            x.copy(
                __sqlTable__ = x.__sqlTable__.copy(
                    matchRecognize =
                        x.__sqlTable__.matchRecognize.map: m =>
                            m.copy(orderBy = m.orderBy ++ items)
                    )
            )

        def setPerMatch(x: SubqueryTable[N, V, L], perMatch: SqlRecognizePatternRowsPerMatchMode): SubqueryTable[N, V, L] =
            x.copy(
                __sqlTable__ = x.__sqlTable__.copy(
                    matchRecognize =
                        x.__sqlTable__.matchRecognize.map: m =>
                            m.copy(rowsPerMatch = Some(perMatch))
                    )
            )

        def setRecognize(x: SubqueryTable[N, V, L], recognize: SqlMatchRecognize): SubqueryTable[N, V, L] =
            x.copy(
                __sqlTable__ = x.__sqlTable__.copy(
                    matchRecognize = Some(recognize)
                )
            )

        def fetchRecognize(x: SubqueryTable[N, V, L]): SqlMatchRecognize =
            x.__sqlTable__.matchRecognize.get

        def fetchSqlTable(x: SubqueryTable[N, V, L]): SqlTable =
            x.__sqlTable__