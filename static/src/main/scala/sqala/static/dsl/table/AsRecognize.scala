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
      * The table reference type.
      */
    type ST <: SqlTable

    /**
     * The kind tuple of the outer query.
     */
    type OKS <: Tuple

    /**
     * Initializes the `matchRecognize` clause on the table.
     */
    def asRecognizeTable(x: T)(using QueryContext[CL]): (R, ST)

object AsRecognize:
    type Aux[T, CL <: Int, O, OST <: SqlTable, OOKS <: Tuple] = AsRecognize[T, CL]:
        type R = O

        type ST = OST

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
    ): Aux[O, CL, Table[fc.R, Column, CL], SqlTable.Ident, EmptyTuple] =
        new AsRecognize[O, CL]:
            type R = Table[fc.R, Column, CL]

            type ST = SqlTable.Ident

            type OKS = EmptyTuple

            def asRecognizeTable(x: O)(using qc: QueryContext[CL]): (R, ST) =
                val metaData = fc.metaData
                val alias = qc.fetchAlias
                val table = Table[fc.R, Column, CL](
                    alias,
                    metaData
                )
                val sqlTable: SqlTable.Ident =
                    SqlTable.Ident(
                        metaData.tableName,
                        Some(SqlTableAlias(alias, Nil)),
                        None,
                        Some(createEmptyRecognize),
                        None
                    )
                (table, sqlTable)

    given subquery[N <: Tuple, V <: Tuple, TOKS <: Tuple, L <: Int, S <: QuerySize, Q <: Query[NamedTuple[N, V], TOKS, L, S], CL <: Int](using
        p: AsTableParam[V, CL],
        tt: ToTuple[p.R],
        refl: L > CL =:= true
    ): Aux[Q, CL, MappedTable[N, tt.R, CL], SqlTable.Subquery, TOKS] =
        new AsRecognize[Q, CL]:
            type R = MappedTable[N, tt.R, CL]

            type ST = SqlTable.Subquery

            type OKS = TOKS

            def asRecognizeTable(x: Q)(using qc: QueryContext[CL]): (R, ST) =
                val alias = qc.fetchAlias
                val table = MappedTable[N, V, CL](alias)
                val sqlTable: SqlTable.Subquery =
                    SqlTable.Subquery(
                        false,
                        x.tree,
                        Some(SqlTableAlias(alias, Nil)),
                        None
                    )
                (table, sqlTable)

/**
  * Sets the `matchRecognize` configuration. ST is the SQL table type.
  */
trait SetRecognizeProperty[T, ST <: SqlTable]:
    /**
     * Sets the `partitionBy` expressions.
     */
    def setPartitionBy(table: ST, items: List[SqlExpr]): ST

    /**
     * Appends `orderBy` items.
     */
    def setOrderBy(table: ST, items: List[SqlOrderingItem]): ST

    /**
     * Sets the rows-per-match mode.
     */
    def setPerMatch(table: ST, perMatch: SqlRecognizePatternRowsMode): ST

    /**
     * Sets the full `matchRecognize` configuration.
     */
    def setRecognize(table: ST, recognize: SqlMatchRecognize): ST

    /**
     * Fetches the `matchRecognize` configuration.
     */
    def fetchRecognize(table: ST): Option[SqlMatchRecognize]

object SetRecognizeProperty:
    given table[T, K[_ <: Int] <: ExprKind, L <: Int]: SetRecognizeProperty[Table[T, K, L], SqlTable.Ident] with
        def setPartitionBy(table: SqlTable.Ident, items: List[SqlExpr]): SqlTable.Ident =
            table.copy(
                matchRecognize =
                    table.matchRecognize.map: m =>
                        m.copy(partitionBy = items)
            )

        def setOrderBy(table: SqlTable.Ident, items: List[SqlOrderingItem]): SqlTable.Ident =
            table.copy(
                matchRecognize =
                    table.matchRecognize.map: m =>
                        m.copy(orderBy = m.orderBy ++ items)
            )

        def setPerMatch(table: SqlTable.Ident, perMatch: SqlRecognizePatternRowsMode): SqlTable.Ident =
            table.copy(
                matchRecognize =
                    table.matchRecognize.map: m =>
                        m.copy(rowsMode = Some(perMatch))
            )

        def setRecognize(table: SqlTable.Ident, recognize: SqlMatchRecognize): SqlTable.Ident =
            table.copy(
                matchRecognize = Some(recognize)
            )

        def fetchRecognize(table: SqlTable.Ident): Option[SqlMatchRecognize] =
            table.matchRecognize

    given subquery[N <: Tuple, V <: Tuple, L <: Int]: SetRecognizeProperty[MappedTable[N, V, L], SqlTable.Subquery] with
        def setPartitionBy(table: SqlTable.Subquery, items: List[SqlExpr]): SqlTable.Subquery =
            table.copy(
                matchRecognize =
                    table.matchRecognize.map: m =>
                        m.copy(partitionBy = items)
            )

        def setOrderBy(table: SqlTable.Subquery, items: List[SqlOrderingItem]): SqlTable.Subquery =
            table.copy(
                matchRecognize =
                    table.matchRecognize.map: m =>
                        m.copy(orderBy = m.orderBy ++ items)
            )

        def setPerMatch(table: SqlTable.Subquery, perMatch: SqlRecognizePatternRowsMode): SqlTable.Subquery =
            table.copy(
                matchRecognize =
                    table.matchRecognize.map: m =>
                        m.copy(rowsMode = Some(perMatch))
            )

        def setRecognize(table: SqlTable.Subquery, recognize: SqlMatchRecognize): SqlTable.Subquery =
            table.copy(
                matchRecognize = Some(recognize)
            )

        def fetchRecognize(table: SqlTable.Subquery): Option[SqlMatchRecognize] =
            table.matchRecognize

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

    given subquery[N <: Tuple, V <: Tuple, L <: Int]: AliasRecognize[MappedTable[N, V, L]] with
        def alias(x: MappedTable[N, V, L], name: String): MappedTable[N, V, L] =
            x.copy(
                __aliasName__ = name
            )