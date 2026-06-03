package sqala.static.dsl.table

import sqala.ast.table.{SqlJoinCondition, SqlTable}
import sqala.metadata.SqlBoolean
import sqala.static.dsl.*

/**
 * Computes the combined row type of an inner join.
 */
trait Join[A, B]:
    /**
     * The combined row type.
     */
    type R

    /**
     * Concatenates the row tuples.
     */
    def join(a: A, b: B): R

object Join:
    type Aux[A, B, O] = Join[A, B]:
        type R = O

    given join[A, B](using
        ta: ToTuple[A],
        tb: ToTuple[B]
    ): Aux[A, B, Tuple.Concat[ta.R, tb.R]] =
        new Join[A, B]:
            type R = Tuple.Concat[ta.R, tb.R]

            def join(a: A, b: B): R =
                ta.toTuple(a) ++ tb.toTuple(b)

/**
 * Computes the combined row type of a left outer join.
 * The right side is wrapped with `Option`.
 */
trait LeftJoin[A, B]:
    /**
     * The combined row type.
     */
    type R

    /**
     * Concatenates the row tuples, with the right side optionally null.
     */
    def join(a: A, b: B): R

object LeftJoin:
    type Aux[A, B, O] = LeftJoin[A, B]:
        type R = O

    given join[A, B](using
        ta: ToTuple[A],
        to: ToOption[B],
        tb: ToTuple[to.R]
    ): Aux[A, B, Tuple.Concat[ta.R, tb.R]] =
        new LeftJoin[A, B]:
            type R = Tuple.Concat[ta.R, tb.R]

            def join(a: A, b: B): R =
                ta.toTuple(a) ++ tb.toTuple(to.toOption(b))

/**
 * Computes the combined row type of a right outer join.
 * The left side is wrapped with `Option`.
 */
trait RightJoin[A, B]:
    /**
     * The combined row type.
     */
    type R

    /**
     * Concatenates the row tuples, with the left side optionally null.
     */
    def join(a: A, b: B): R

object RightJoin:
    type Aux[A, B, O] = RightJoin[A, B]:
        type R = O

    given join[A, B](using
        to: ToOption[A],
        ta: ToTuple[to.R],
        tb: ToTuple[B]
    ): Aux[A, B, Tuple.Concat[ta.R, tb.R]] =
        new RightJoin[A, B]:
            type R = Tuple.Concat[ta.R, tb.R]

            def join(a: A, b: B): R =
                ta.toTuple(to.toOption(a)) ++ tb.toTuple(b)

/**
 * Computes the combined row type of a full outer join.
 * Both sides are wrapped with `Option`.
 */
trait FullJoin[A, B]:
    /**
     * The combined row type.
     */
    type R

    /**
     * Concatenates the row tuples, with both sides optionally null.
     */
    def join(a: A, b: B): R

object FullJoin:
    type Aux[A, B, O] = FullJoin[A, B]:
        type R = O

    given join[A, B](using
        toa: ToOption[A],
        ta: ToTuple[toa.R],
        tob: ToOption[B],
        tb: ToTuple[tob.R]
    ): Aux[A, B, Tuple.Concat[ta.R, tb.R]] =
        new FullJoin[A, B]:
            type R = Tuple.Concat[ta.R, tb.R]

            def join(a: A, b: B): R =
                ta.toTuple(toa.toOption(a)) ++ tb.toTuple(tob.toOption(b))

/**
 * A join table source produced by calling `.on(...)` on a `JoinPart`. 
 */
final case class FromJoin[T, OKS <: Tuple, L <: Int](
    private[sqala] val params: T,
    private[sqala] val sqlTable: SqlTable.Join
) extends AnyTable

/**
 * An intermediate join before the condition is specified. Created by
 * `join`, `leftJoin`, etc., and converted to `FromJoin` via `.on(...)`.
 */
final case class JoinPart[T, OKS <: Tuple, L <: Int](
    private[sqala] val params: T,
    private[sqala] val sqlTable: SqlTable.Join
):
    /**
     * Specifies the join condition. Maps to SQL `ON`. The condition
     * must be a valid filter expression — aggregate functions, window
     * functions, and other expressions not allowed in `ON` are rejected
     * at compile time.
     *
     * {{{
     * // Two-table join
     * from(Channel.join(Post).on((c, p) => c.id == p.channelId))
     *
     * // Multi-table join
     * from(
     *     Channel
     *     .join(Post).on((c, p) => c.id == p.channelId)
     *     .join(Comment).on((_, p, ct) => p.id == ct.postId)
     * )
     * }}}
     */
    def on[F](f: T => F)(using
        qc: QueryContext[L],
        a: AsExpr[F, L],
        b: SqlBoolean[a.R],
        kt: KindToTuple[a.K],
        i: CanInFilter[kt.R],
        e: ExcludeCurrentLevelColumn[kt.R, L],
        c: CombineKindTuple[OKS, e.R]
    ): FromJoin[T, c.R, L] =
        val cond = a.asExpr(f(params))
        FromJoin(
            params,
            sqlTable.copy(condition = Some(SqlJoinCondition.On(cond.asSqlExpr)))
        )