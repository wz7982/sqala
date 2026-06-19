package sqala.static.dsl.table

import sqala.ast.expr.SqlExpr
import sqala.ast.order.SqlOrderingItem
import sqala.ast.table.*
import sqala.static.dsl.ExprKind
import sqala.util.NonEmptyList

/**
 * Applies `matchRecognize` configuration to a table. Each method
 * returns an immutable copy with the updated value.
 */
trait AsRecognize[T]:
    /**
     * Initializes the `matchRecognize` clause on the table.
     */
    def asRecognizeTable(x: T): T

    /**
     * Sets the table alias.
     */
    def alias(x: T, name: String): T

    /**
     * Sets the `partitionBy` expressions.
     */
    def setPartitionBy(x: T, items: List[SqlExpr]): T

    /**
     * Appends `orderBy` items.
     */
    def setOrderBy(x: T, items: List[SqlOrderingItem]): T

    /**
     * Sets the rows-per-match mode.
     */
    def setPerMatch(x: T, perMatch: SqlRecognizePatternRowsMode): T

    /**
     * Sets the full `matchRecognize` configuration.
     */
    def setRecognize(x: T, recognize: SqlMatchRecognize): T

    /**
     * Reads the current `matchRecognize` configuration.
     */
    def fetchRecognize(x: T): SqlMatchRecognize

    /**
     * Reads the underlying SQL table.
     */
    def fetchSqlTable(x: T): SqlTable

object AsRecognize:
    /**
     * Creates an empty `matchRecognize` configuration.
     */
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
                NonEmptyList(SqlRowPatternDefineItem("tmp", SqlExpr.NullLiteral), Nil)
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

        def setPerMatch(x: Table[T, K, L], perMatch: SqlRecognizePatternRowsMode): Table[T, K, L] =
            x.copy(
                __sqlTable__ = x.__sqlTable__.copy(
                    matchRecognize =
                        x.__sqlTable__.matchRecognize.map: m =>
                            m.copy(rowsMode = Some(perMatch))
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

        def setPerMatch(x: SubqueryTable[N, V, L], perMatch: SqlRecognizePatternRowsMode): SubqueryTable[N, V, L] =
            x.copy(
                __sqlTable__ = x.__sqlTable__.copy(
                    matchRecognize =
                        x.__sqlTable__.matchRecognize.map: m =>
                            m.copy(rowsMode = Some(perMatch))
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