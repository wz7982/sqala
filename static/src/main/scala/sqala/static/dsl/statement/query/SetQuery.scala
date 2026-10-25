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

/**
 * A query produced by set operations (`union`, `except`, `intersect`).
 */
sealed case class SetQuery[T, OKS <: Tuple, L <: Int, S <: QuerySize](
    override private[sqala] val params: T,
    override private[sqala] val tree: IrQuery.Set
)(using
    override private[sqala] val qc: QueryContext[L]
) extends Query[T, OKS, L, ManyRows](params, tree):
    /**
     * Skips the first `n` rows of the result. Maps to `OFFSET`.
     *
     * {{{
     * q1.union(q2).drop(10).take(5)
     * }}}
     */
    def drop(n: Int): SetQuery[T, OKS, L, S] =
        val expr = IrExpr.Native(SqlExpr.NumberLiteral(n))
        val limit = tree.limit
            .map(l => IrLimit(Some(expr), l.fetch))
            .orElse(Some(IrLimit(Some(expr), None)))
        SetQuery(params, tree.copy(limit = limit))

    /**
     * Limits the result to at most `n` rows. Maps to `LIMIT` or `FETCH NEXT`.
     *
     * {{{
     * q1.union(q2).drop(10).take(5)
     * }}}
     */
    def take(n: Int)(using
        s: TakeSize[n.type]
    ): SetQuery[T, OKS, L, s.R] =
        val expr = IrExpr.Native(SqlExpr.NumberLiteral(n))
        val limit = tree.limit
            .map(l => IrLimit(l.offset, Some(IrFetch(expr, SqlFetchUnit.RowCount, SqlFetchMode.Only))))
            .orElse(Some(IrLimit(None, Some(IrFetch(expr, SqlFetchUnit.RowCount, SqlFetchMode.Only)))))
        SetQuery(params, tree.copy(limit = limit))

    /**
     * Adds an `ORDER BY` clause. Sort expressions default to `ASC` unless
     * `.desc` or other ordering is specified.
     *
     * {{{
     * q1.union(q2).sortBy(c => c.name.desc)
     * }}}
     */
    def sortBy[SS](f: QueryContext[L] ?=> T => SS)(using
        a: AsSort[SS, S, L],
        e: ExcludeCurrentLevelColumn[a.KS, L],
        c: CombineKindTuple[OKS, e.R],
        an: AggNotNested[a.KS]
    ): SortedSetQuery[T, c.R, L, S] =
        val sort = a.asSorts(f(params))
        SortedSetQuery(
            params,
            tree.copy(orderBy = tree.orderBy ++ sort.map(_.ordering))
        )

/**
 * A sorted set-operation query after `sortBy`.
 */
final class SortedSetQuery[T, OKS <: Tuple, L <: Int, S <: QuerySize](
    override private[sqala] val params: T,
    override private[sqala] val tree: IrQuery.Set
)(using
    override private[sqala] val qc: QueryContext[L]
) extends SetQuery[T, OKS, L, S](params, tree):
    override def drop(n: Int): SortedSetQuery[T, OKS, L, S] =
        val expr = IrExpr.Native(SqlExpr.NumberLiteral(n))
        val limit = tree.limit
            .map(l => IrLimit(Some(expr), l.fetch))
            .orElse(Some(IrLimit(Some(expr), None)))
        SortedSetQuery(params, tree.copy(limit = limit))

    /**
     * Limits the result to at most `n` rows, including rows tied with
     * the last row's sort key. Maps to `FETCH NEXT n ROWS WITH TIES`.
     *
     * {{{
     * q1.union(q2).sortBy(c => c.name).takeWithTies(5)
     * }}}
     */
    def takeWithTies(n: Int)(using
        s: TakeSize[n.type]
    ): SetQuery[T, OKS, L, s.R] =
        val expr = IrExpr.Native(SqlExpr.NumberLiteral(n))
        val limit = tree.limit
            .map(l => IrLimit(l.offset, Some(IrFetch(expr, SqlFetchUnit.RowCount, SqlFetchMode.WithTies))))
            .orElse(Some(IrLimit(None, Some(IrFetch(expr, SqlFetchUnit.RowCount, SqlFetchMode.WithTies)))))
        SetQuery(params, tree.copy(limit = limit))

    /**
     * Adds an `ORDER BY` clause. Sort expressions default to `ASC` unless
     * `.desc` or other ordering is specified. Multiple calls accumulate.
     *
     * {{{
     * q1.union(q2).sortBy(c => c.name).sortBy(c => c.id.desc)
     * }}}
     */
    override def sortBy[SS](f: QueryContext[L] ?=> T => SS)(using
        a: AsSort[SS, S, L],
        e: ExcludeCurrentLevelColumn[a.KS, L],
        c: CombineKindTuple[OKS, e.R],
        an: AggNotNested[a.KS]
    ): SortedSetQuery[T, c.R, L, S] =
        val sort = a.asSorts(f(params))
        SortedSetQuery(
            params,
            tree.copy(orderBy = tree.orderBy ++ sort.map(_.ordering))
        )