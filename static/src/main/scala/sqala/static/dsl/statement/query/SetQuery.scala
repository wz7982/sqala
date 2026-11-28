package sqala.static.dsl.statement.query

import sqala.ast.expr.SqlExpr
import sqala.ast.limit.{SqlFetchMode, SqlFetchUnit}
import sqala.ir.{IrExpr, IrFetch, IrLimit, IrQuery}
import sqala.static.dsl.*

/**
 * A query produced by set operations (`union`, `except`, `intersect`).
 */
sealed class SetQuery[T, OKS <: Tuple, L <: Int, S <: QuerySize](
    override private[sqala] val params: T,
    override private[sqala] val tree: IrQuery.Set
)(using
    override private[sqala] val qc: QueryContext[L]
) extends Query[T, OKS, L, S](params, tree):
    override def drop(n: Int): SetQuery[T, OKS, L, S] =
        val expr = IrExpr.Native(SqlExpr.NumberLiteral(n))
        val limit = tree.limit
            .map(l => IrLimit(Some(expr), l.fetch))
            .orElse(Some(IrLimit(Some(expr), None)))
        SetQuery(params, tree.copy(limit = limit))

    override def take(n: Int)(using
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