package sqala.static.dsl.table

import sqala.ast.expr.SqlExpr
import sqala.ast.order.SqlOrderingItem
import sqala.ast.table.*
import sqala.static.metadata.TableMetaData

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

    given table[T]: AsRecognizeTable[Table[T]] with
        def asRecognizeTable(x: Table[T]): Table[T] =
            x.copy(
                __aliasName__ = None, 
                __sqlTable__ = x.__sqlTable__.copy(matchRecognize = Some(createEmptyRecognize))
            )

        def alias(x: Table[T], name: String): Table[T] =
            x.copy(
                __aliasName__ = Some(name)
            )

        def setPartitionBy(x: Table[T], items: List[SqlExpr]): Table[T] =
            x.copy(
                __sqlTable__ =
                    x.__sqlTable__.copy(
                        matchRecognize = 
                            x.__sqlTable__.matchRecognize.map: m =>
                                m.copy(partitionBy = items)
                    )
            )

        def setOrderBy(x: Table[T], items: List[SqlOrderingItem]): Table[T] =
            x.copy(
                __sqlTable__ =
                    x.__sqlTable__.copy(
                        matchRecognize = 
                            x.__sqlTable__.matchRecognize.map: m =>
                                m.copy(orderBy = items)
                    )
            )

        def setPerMatch(x: Table[T], perMatch: SqlRecognizePatternRowsPerMatchMode): Table[T] =
            x.copy(
                __sqlTable__ =
                    x.__sqlTable__.copy(
                        matchRecognize = 
                            x.__sqlTable__.matchRecognize.map: m =>
                                m.copy(rowsPerMatch = Some(perMatch))
                    )
            )

        def setRecognize(x: Table[T], recognize: SqlMatchRecognize): Table[T] =
            x.copy(
                __sqlTable__ =
                    x.__sqlTable__.copy(
                        matchRecognize = Some(recognize)
                    )
            )

        def fetchRecognize(x: Table[T]): SqlMatchRecognize =
            x.__sqlTable__.matchRecognize.get

        def fetchSqlTable(x: Table[T]): SqlTable =
            x.__sqlTable__

    given subQuery[N <: Tuple, V <: Tuple]: AsRecognizeTable[SubQueryTable[N, V]] with
        def asRecognizeTable(x: SubQueryTable[N, V]): SubQueryTable[N, V] =
            x.copy(
                __aliasName__ = None, 
                __sqlTable__ = x.__sqlTable__.copy(matchRecognize = Some(createEmptyRecognize))
            )

        def alias(x: SubQueryTable[N, V], name: String): SubQueryTable[N, V] =
            x.copy(
                __aliasName__ = Some(name)
            )

        def setPartitionBy(x: SubQueryTable[N, V], items: List[SqlExpr]): SubQueryTable[N, V] =
            x.copy(
                __sqlTable__ =
                    x.__sqlTable__.copy(
                        matchRecognize = 
                            x.__sqlTable__.matchRecognize.map: m =>
                                m.copy(partitionBy = items)
                    )
            )

        def setOrderBy(x: SubQueryTable[N, V], items: List[SqlOrderingItem]): SubQueryTable[N, V] =
            x.copy(
                __sqlTable__ =
                    x.__sqlTable__.copy(
                        matchRecognize = 
                            x.__sqlTable__.matchRecognize.map: m =>
                                m.copy(orderBy = items)
                    )
            )

        def setPerMatch(x: SubQueryTable[N, V], perMatch: SqlRecognizePatternRowsPerMatchMode): SubQueryTable[N, V] =
            x.copy(
                __sqlTable__ =
                    x.__sqlTable__.copy(
                        matchRecognize = 
                            x.__sqlTable__.matchRecognize.map: m =>
                                m.copy(rowsPerMatch = Some(perMatch))
                    )
            )

        def setRecognize(x: SubQueryTable[N, V], recognize: SqlMatchRecognize): SubQueryTable[N, V] =
            x.copy(
                __sqlTable__ =
                    x.__sqlTable__.copy(
                        matchRecognize = Some(recognize)
                    )
            )

        def fetchRecognize(x: SubQueryTable[N, V]): SqlMatchRecognize =
            x.__sqlTable__.matchRecognize.get

        def fetchSqlTable(x: SubQueryTable[N, V]): SqlTable =
            x.__sqlTable__