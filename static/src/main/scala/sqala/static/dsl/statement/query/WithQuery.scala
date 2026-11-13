package sqala.static.dsl.statement.query

import sqala.ast.expr.{SqlBinaryOperator, SqlExpr}
import sqala.ast.group.{SqlGroup, SqlGroupingItem}
import sqala.ast.limit.{SqlFetch, SqlFetchMode, SqlFetchUnit, SqlLimit}
import sqala.ast.quantifier.SqlQuantifier
import sqala.ast.statement.*
import sqala.ast.table.{SqlJoinCondition, SqlJoinType, SqlTable, SqlTableAlias}
import sqala.metadata.{Dialect, SqlBoolean}
import sqala.static.dsl.*
import sqala.static.dsl.table.{Table, TransformTableKind}
import sqala.util.queryToString
import sqala.util.NonEmptyList

import scala.compiletime.ops.int.-
import sqala.ir.IrQuery
import sqala.ir.ToAst
import sqala.ir.IrLimit
import sqala.ir.IrFetch
import sqala.ir.IrLock
import sqala.ir.IrLockWaitMode
import sqala.ir.IrGroup
import sqala.ir.IrGroupingItem
import sqala.ir.IrExpr
import sqala.ir.IrTable
import sqala.ir.IrWithItem
import sqala.static.dsl.table.AsTableParam
import scala.NamedTuple.NamedTuple
import sqala.static.dsl.table.MappedTable
import sqala.ir.IrSelectItem

/**
 * A hierarchical recursive query created by `connectBy`. Converts to a
 * standard `WITH RECURSIVE` CTE at SQL generation time.
 */
final case class ConnectBy[T, C, OKS <: Tuple, L <: Int](
    private[sqala] val row: T,
    private[sqala] val cteRow: C,
    private[sqala] val stepCte: IrTable,
    private[sqala] val finalCte: IrTable,
    private[sqala] val seedTree: IrQuery.Select,
    private[sqala] val stepTree: IrQuery.Select
)(using private[sqala] val qc: QueryContext[L]):
    given ConnectByContext = ConnectByContext()

    /**
     * Specifies the root-level condition for starting recursion.
     * Maps to the base `WHERE` clause of the CTE.
     *
     * {{{
     * from(Comment).connectBy((c, prior) => c.parentId === prior.id)
     *     .startWith(c => c.parentId.isNull)
     * }}}
     */
    def startWith[F](f: QueryContext[L] ?=> ConnectByContext ?=> T => F)(using
        a: AsExpr[F, L],
        b: ValidateArgType[SqlBoolean, a.R, "boolean"],
        kt: KindToTuple[a.K],
        i: CanInSimpleClause[kt.R, "startWith"],
        e: ExcludeCurrentLevelColumn[kt.R, L],
        c: CombineKindTuple[OKS, e.R]
    ): ConnectBy[T, C, c.R, L] =
        copy(seedTree = seedTree.addWhere(a.asExpr(f(row)).expr))

    /**
     * Limits the recursion depth. Maps to a filter on the pseudo
     * `LEVEL` column.
     *
     * {{{
     * from(Comment).connectBy(...).startWith(...).maxDepth(5)
     * }}}
     */
    def maxDepth(n: Int): ConnectBy[T, C, OKS, L] =
        val cond = IrExpr.Binary(
            IrExpr.Column(Some(stepCte), seedTree.select.toList.last.alias),
            SqlBinaryOperator.LessThan,
            IrExpr.Native(SqlExpr.NumberLiteral(n))
        )
        copy(stepTree = stepTree.addWhere(cond))

    /**
     * Defines the projection of the recursive CTE rows, i.e. the columns
     * carried through each level, before the final `map`.
     *
     * The first parameter list is the `seed` projection (the rows selected
     * by `startWith`); the second is the `step` projection, which computes
     * each subsequent row from the current row `c`, the previous level
     * `prior`, and the current depth `level` (starting from 1).
     *
     * If `preMap` is omitted, the CTE carries all source columns plus
     * `level` automatically.
     *
     * {{{
     * val postId = 1
     *
     * from(Comment)
     *     .filter(c => c.postId === postId)
     *     .connectBy((c, prior) => c.parentId === prior.id)
     *     .startWith(c => c.parentId.isNull)
     *     .preMap(
     *         c => (id = c.id, content = c.content, parentId = c.parentId)
     *     )(
     *         (c, prior, level) => (id = c.id, content = c.content, parentId = c.parentId)
     *     )
     *     .map((c, level) => (id = c.id, content = c.content, level = level))
     * }}}
     *
     * Because `step` can read the previous level through `prior`, derived
     * columns can be accumulated down the tree. Here every comment keeps
     * the id of its root comment:
     *
     * {{{
     * from(Comment)
     *     .filter(c => c.postId === postId)
     *     .connectBy((c, prior) => c.parentId === prior.id)
     *     .startWith(c => c.parentId.isNull)
     *     .preMap(
     *         c => (id = c.id, content = c.content, rootId = c.id)
     *     )(
     *         (c, prior, level) => (id = c.id, content = c.content, rootId = prior.rootId)
     *     )
     *     .map((c, level) => (id = c.id, content = c.content, rootId = c.rootId, level = level))
     * }}}
     */
    inline def preMap[N <: Tuple, V <: Tuple, UN <: Tuple, UV <: Tuple](
        seed: QueryContext[L] ?=> T => NamedTuple[N, V]
    )(using
        a: AsMap[NamedTuple[N, V], L],
        p: AsTableParam[V, L],
        t: ToTuple[p.R]
    )(
        step: QueryContext[L] ?=> (T, MappedTable[N, t.R, V, false, L], Expr[Int, Column[L]]) => NamedTuple[UN, UV]
    )(using
        au: AsMap[NamedTuple[UN, UV], L],
        u: Union[V, UV, L],
        tu: ToTuple[u.R],
        up: AsTableParam[tu.R, L],
        tup: ToTuple[up.R]
    ): ConnectBy[T, MappedTable[N, tup.R, tu.R, false, L], OKS, L] =
        val seedItems = a.asSelectItems(seed(row), 1)
        val levelName = s"c${seedItems.size + 1}"
        val stepPrior = MappedTable[N, V, V, false, L](stepCte)
        val finalRow = MappedTable[N, tu.R, tu.R, false, L](finalCte)
        val stepItems = au.asSelectItems(
            step(
                row,
                stepPrior,
                Expr(IrExpr.Binary(
                    IrExpr.Column(Some(stepCte), levelName),
                    SqlBinaryOperator.Plus,
                    IrExpr.Native(SqlExpr.NumberLiteral(1))
                ))
            ),
            1
        )
        val newSeedTree = seedTree.copy(
            select = seedItems :+ IrSelectItem(IrExpr.Native(SqlExpr.NumberLiteral(1)), levelName)
        )
        val newStepTree = stepTree.copy(
            select = stepItems :+ IrSelectItem(
                IrExpr.Binary(
                    IrExpr.Column(Some(stepCte), levelName),
                    SqlBinaryOperator.Plus,
                    IrExpr.Native(SqlExpr.NumberLiteral(1))
                ),
                levelName
            )
        )
        ConnectBy(row, finalRow, stepCte, finalCte, newSeedTree, newStepTree)

    /**
     * Projects the final result from the recursive CTE. Maps to
     * `SELECT` on the CTE table, generating the complete `WITH RECURSIVE`
     * query.
     *
     * {{{
     * from(Comment).connectBy((c, prior) => c.parentId === prior.id)
     *     .startWith(c => c.parentId.isNull)
     *     .map((c, level) => (id = c.id, content = c.content, level = level))
     * }}}
     */
    def map[M](f: QueryContext[L] ?=> ConnectByContext ?=> (C, Expr[Int, Column[L]]) => M)(using
        a: AsMap[M, L],
        e: ExcludeCurrentLevelColumn[a.KS, L],
        i: CanInMap[a.KS, e.R],
        c: CombineKindTuple[OKS, e.R],
        an: AggNotNested[a.KS]
    ): WithQuery[a.R, C, c.R, L, i.R] =
        val level: Expr[Int, Column[L]] = Expr(IrExpr.Column(Some(finalCte), seedTree.select.toList.last.alias))
        val mapped = f(cteRow, level)
        val selectItems = a.asSelectItems(mapped, 1)
        val unionTree = IrQuery.Set(seedTree, SqlSetOperator.Union(Some(SqlQuantifier.All)), stepTree, Nil, None)
        val columns = seedTree.select.map(_.alias).toList
        val finalSelect: IrQuery.Select = IrQuery.Select(None, selectItems, Some(finalCte), None, None, None, Nil, None, None)
        WithQuery(a.transform(mapped), cteRow, level, NonEmptyList(IrWithItem(columns, unionTree), Nil), true, finalSelect)

/**
 * A recursive `WITH RECURSIVE` query, produced by `connectBy`.
 */
sealed class WithQuery[M, T, OKS <: Tuple, L <: Int, S <: QuerySize](
    override private[sqala] val params: M,
    private[sqala] val cteRow: T,
    private[sqala] val level: Expr[Int, Column[L]],
    private[sqala] val withItems: NonEmptyList[IrWithItem],
    private[sqala] val recursive: Boolean,
    private[sqala] val finalSelect: IrQuery.Select
)(using
    override private[sqala] val qc: QueryContext[L]
) extends Query[M, OKS, L, S](params, IrQuery.With(recursive, withItems, finalSelect)):
    /**
     * Skips the first `n` rows of the result. Maps to `OFFSET`.
     */
    def drop(n: Int): WithQuery[M, T, OKS, L, S] =
        val expr = IrExpr.Native(SqlExpr.NumberLiteral(n))
        val limit = finalSelect.limit
            .map(l => IrLimit(Some(expr), l.fetch))
            .orElse(Some(IrLimit(Some(expr), None)))
        WithQuery(params, cteRow, level, withItems, recursive, finalSelect.copy(limit = limit))

    /**
     * Limits the result to at most `n` rows. Maps to `LIMIT` or `FETCH NEXT`.
     */
    def take(n: Int)(using
        s: TakeSize[n.type]
    ): WithQuery[M, T, OKS, L, s.R] =
        val expr = IrExpr.Native(SqlExpr.NumberLiteral(n))
        val limit = finalSelect.limit
            .map(l => IrLimit(l.offset, Some(IrFetch(expr, SqlFetchUnit.RowCount, SqlFetchMode.Only))))
            .orElse(Some(IrLimit(None, Some(IrFetch(expr, SqlFetchUnit.RowCount, SqlFetchMode.Only)))))
        WithQuery(params, cteRow, level, withItems, recursive, finalSelect.copy(limit = limit))

    /**
     * Adds an `ORDER BY` clause. The lambda receives the CTE row and
     * the `level` expression, so both projected and non-projected
     * columns plus `level` can be used.
     *
     * {{{
     * from(Comment).connectBy(...).map((c, level) => (id = c.id, level = level))
     *     .sortBy((c, level) => level.desc)
     * }}}
     */
    def sortBy[SS](f: QueryContext[L] ?=> (T, Expr[Int, Column[L]]) => SS)(using
        a: AsSort[SS, S, L],
        e: ExcludeCurrentLevelColumn[a.KS, L],
        c: CombineKindTuple[OKS, e.R],
        an: AggNotNested[a.KS]
    ): SortedWithQuery[M, T, c.R, L, S] =
        val sort = a.asSorts(f(cteRow, level))
        SortedWithQuery(
            params,
            cteRow,
            level,
            withItems,
            recursive,
            finalSelect.copy(orderBy = finalSelect.orderBy ++ sort.map(_.ordering))
        )

/**
 * A sorted recursive query after `sortBy`.
 */
final class SortedWithQuery[M, T, OKS <: Tuple, L <: Int, S <: QuerySize](
    override private[sqala] val params: M,
    override private[sqala] val cteRow: T,
    override private[sqala] val level: Expr[Int, Column[L]],
    override private[sqala] val withItems: NonEmptyList[IrWithItem],
    override private[sqala] val recursive: Boolean,
    override private[sqala] val finalSelect: IrQuery.Select
)(using
    override private[sqala] val qc: QueryContext[L]
) extends WithQuery[M, T, OKS, L, S](params, cteRow, level, withItems, recursive, finalSelect):
    override def drop(n: Int): SortedWithQuery[M, T, OKS, L, S] =
        val expr = IrExpr.Native(SqlExpr.NumberLiteral(n))
        val limit = finalSelect.limit
            .map(l => IrLimit(Some(expr), l.fetch))
            .orElse(Some(IrLimit(Some(expr), None)))
        SortedWithQuery(params, cteRow, level, withItems, recursive, finalSelect.copy(limit = limit))

    /**
     * Limits the result to at most `n` rows, including rows tied with
     * the last row's sort key. Maps to `FETCH NEXT n ROWS WITH TIES`.
     */
    def takeWithTies(n: Int)(using
        s: TakeSize[n.type]
    ): WithQuery[M, T, OKS, L, s.R] =
        val expr = IrExpr.Native(SqlExpr.NumberLiteral(n))
        val limit = finalSelect.limit
            .map(l => IrLimit(l.offset, Some(IrFetch(expr, SqlFetchUnit.RowCount, SqlFetchMode.WithTies))))
            .orElse(Some(IrLimit(None, Some(IrFetch(expr, SqlFetchUnit.RowCount, SqlFetchMode.WithTies)))))
        WithQuery(params, cteRow, level, withItems, recursive, finalSelect.copy(limit = limit))

    override def sortBy[SS](f: QueryContext[L] ?=> (T, Expr[Int, Column[L]]) => SS)(using
        a: AsSort[SS, S, L],
        e: ExcludeCurrentLevelColumn[a.KS, L],
        c: CombineKindTuple[OKS, e.R],
        an: AggNotNested[a.KS]
    ): SortedWithQuery[M, T, c.R, L, S] =
        val sort = a.asSorts(f(cteRow, level))
        SortedWithQuery(
            params,
            cteRow,
            level,
            withItems,
            recursive,
            finalSelect.copy(orderBy = finalSelect.orderBy ++ sort.map(_.ordering))
        )