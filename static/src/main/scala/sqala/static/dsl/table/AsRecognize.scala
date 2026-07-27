package sqala.static.dsl.table

import sqala.ast.expr.SqlExpr
import sqala.ast.order.SqlOrderingItem
import sqala.ast.table.*
import sqala.metadata.FetchCompanion
import sqala.static.dsl.*
import sqala.static.dsl.statement.query.Query
import sqala.util.NonEmptyList

import scala.NamedTuple.NamedTuple
import scala.util.NotGiven
import scala.compiletime.ops.int.>

/**
 * Applies `matchRecognize` configuration to a table. Each method
 * returns an immutable copy with the updated value. `CL` is the
 * current query context level, used for scope validation.
 */
trait AsRecognize[T, CL <: Int]:
    /**
     * The table reference type.
     */
    type R

    /**
     * The kind tuple of the outer query.
     */
    type OKS <: Tuple

    /**
     * Initializes the `matchRecognize` clause on the table.
     */
    def asRecognizeTable(x: T)(using QueryContext[CL]): (R, SqlTable)

object AsRecognize:
    type Aux[T, CL <: Int, O, OOKS <: Tuple] = AsRecognize[T, CL]:
        type R = O

        type OKS = OOKS

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

    given entity[O, CL <: Int](using
        na: NotGiven[O <:< AnyTable],
        nt: NotGiven[O <:< Tuple],
        nq: NotGiven[O <:< Query[?, ?, ?, ?]],
        ns: NotGiven[O <:< Seq[?]],
        fc: FetchCompanion[O]
    ): Aux[O, CL, Table[fc.R, Column, CL], EmptyTuple] =
        new AsRecognize[O, CL]:
            type R = Table[fc.R, Column, CL]

            type OKS = EmptyTuple

            def asRecognizeTable(x: O)(using qc: QueryContext[CL]): (R, SqlTable) =
                val metaData = fc.metaData
                val alias = qc.fetchAlias
                val sqlTable: SqlTable.Ident =
                    SqlTable.Ident(
                        metaData.tableName,
                        Some(SqlTableAlias(alias, Nil)),
                        None,
                        Some(createEmptyRecognize),
                        None
                    )
                val table = Table[fc.R, Column, CL](
                    alias,
                    metaData
                )
                (table, sqlTable)

    given subquery[N <: Tuple, V <: Tuple, TOKS <: Tuple, L <: Int, S <: QuerySize, Q <: Query[NamedTuple[N, V], TOKS, L, S], CL <: Int](using
        p: AsTableParam[V, CL],
        tt: ToTuple[p.R],
        refl: L > CL =:= true
    ): Aux[Q, CL, SubqueryTable[N, tt.R, CL], TOKS] =
        new AsRecognize[Q, CL]:
            type R = SubqueryTable[N, tt.R, CL]

            type OKS = TOKS

            def asRecognizeTable(x: Q)(using qc: QueryContext[CL]): (R, SqlTable) =
                val alias = qc.fetchAlias
                val sqlTable: SqlTable.Subquery =
                    SqlTable.Subquery(
                        false,
                        x.tree,
                        Some(SqlTableAlias(alias, Nil)),
                        None
                    )
                val table = SubqueryTable[N, V, CL](x, alias)
                (table, sqlTable)

/**
  * Sets the `matchRecognize` configuration.
  */
trait SetRecognizeProperty[T]:
    /**
     * Sets the `partitionBy` expressions.
     */
    def setPartitionBy(table: SqlTable, items: List[SqlExpr]): SqlTable

    /**
     * Appends `orderBy` items.
     */
    def setOrderBy(table: SqlTable, items: List[SqlOrderingItem]): SqlTable

    /**
     * Sets the rows-per-match mode.
     */
    def setPerMatch(table: SqlTable, perMatch: SqlRecognizePatternRowsMode): SqlTable

    /**
     * Sets the full `matchRecognize` configuration.
     */
    def setRecognize(table: SqlTable, recognize: SqlMatchRecognize): SqlTable

    /**
     * Fetches the `matchRecognize` configuration.
     */
    def fetchRecognize(table: SqlTable): SqlMatchRecognize

object SetRecognizeProperty:
    given table[T, K[_ <: Int] <: ExprKind, L <: Int]: SetRecognizeProperty[Table[T, K, L]] with
        def setPartitionBy(table: SqlTable, items: List[SqlExpr]): SqlTable =
            table.asInstanceOf[SqlTable.Ident].copy(
                matchRecognize =
                    table.asInstanceOf[SqlTable.Ident].matchRecognize.map: m =>
                        m.copy(partitionBy = items)
            )

        def setOrderBy(table: SqlTable, items: List[SqlOrderingItem]): SqlTable =
            table.asInstanceOf[SqlTable.Ident].copy(
                matchRecognize =
                    table.asInstanceOf[SqlTable.Ident].matchRecognize.map: m =>
                        m.copy(orderBy = m.orderBy ++ items)
            )

        def setPerMatch(table: SqlTable, perMatch: SqlRecognizePatternRowsMode): SqlTable =
            table.asInstanceOf[SqlTable.Ident].copy(
                matchRecognize =
                    table.asInstanceOf[SqlTable.Ident].matchRecognize.map: m =>
                        m.copy(rowsMode = Some(perMatch))
            )

        def setRecognize(table: SqlTable, recognize: SqlMatchRecognize): SqlTable =
            table.asInstanceOf[SqlTable.Ident].copy(
                matchRecognize = Some(recognize)
            )

        def fetchRecognize(table: SqlTable): SqlMatchRecognize =
            table.asInstanceOf[SqlTable.Ident].matchRecognize.get

    given subquery[N <: Tuple, V <: Tuple, L <: Int]: SetRecognizeProperty[SubqueryTable[N, V, L]] with
        def setPartitionBy(table: SqlTable, items: List[SqlExpr]): SqlTable =
            table.asInstanceOf[SqlTable.Subquery].copy(
                matchRecognize =
                    table.asInstanceOf[SqlTable.Subquery].matchRecognize.map: m =>
                        m.copy(partitionBy = items)
            )

        def setOrderBy(table: SqlTable, items: List[SqlOrderingItem]): SqlTable =
            table.asInstanceOf[SqlTable.Subquery].copy(
                matchRecognize =
                    table.asInstanceOf[SqlTable.Subquery].matchRecognize.map: m =>
                        m.copy(orderBy = m.orderBy ++ items)
            )

        def setPerMatch(table: SqlTable, perMatch: SqlRecognizePatternRowsMode): SqlTable =
            table.asInstanceOf[SqlTable.Subquery].copy(
                matchRecognize =
                    table.asInstanceOf[SqlTable.Subquery].matchRecognize.map: m =>
                        m.copy(rowsMode = Some(perMatch))
            )

        def setRecognize(table: SqlTable, recognize: SqlMatchRecognize): SqlTable =
            table.asInstanceOf[SqlTable.Subquery].copy(
                matchRecognize = Some(recognize)
            )

        def fetchRecognize(table: SqlTable): SqlMatchRecognize =
            table.asInstanceOf[SqlTable.Subquery].matchRecognize.get

/**
 * Sets the table alias.
 */
trait AliasRecognize[T]:
    /**
     * Sets the table alias.
     */
    def alias(x: T, name: String): T

object AliasRecognize:
    given table[T, K[_ <: Int] <: ExprKind, L <: Int]: AliasRecognize[Table[T, K, L]] with
        def alias(x: Table[T, K, L], name: String): Table[T, K, L] =
            x.copy(
                __aliasName__ = name
            )

    given subquery[N <: Tuple, V <: Tuple, L <: Int]: AliasRecognize[SubqueryTable[N, V, L]] with
        def alias(x: SubqueryTable[N, V, L], name: String): SubqueryTable[N, V, L] =
            x.copy(
                __aliasName__ = name
            )