package sqala.static.dsl.table

import sqala.ir.IrTable
import sqala.metadata.SqlBoolean
import sqala.static.dsl.*

import scala.annotation.{implicitAmbiguous, implicitNotFound}

/**
 * Computes the combined row type of inner join.
 */
@implicitAmbiguous("This value cannot be used as a join operand.")
@implicitNotFound("This value cannot be used as a join operand.")
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

    given join[A, B]: Aux[A, B, Tuple.Concat[WrapToTuple[A], WrapToTuple[B]]] =
        new Join[A, B]:
            type R = Tuple.Concat[WrapToTuple[A], WrapToTuple[B]]

            def join(a: A, b: B): R =
                wrapToTuple(a) ++ wrapToTuple(b)

/**
 * Computes the combined row type of a left outer join.
 * The right side is wrapped with `Option`.
 */
@implicitAmbiguous("This value cannot be used as a join operand.")
@implicitNotFound("This value cannot be used as a join operand.")
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
        to: ToOption[B]
    ): Aux[A, B, Tuple.Concat[WrapToTuple[A], WrapToTuple[to.R]]] =
        new LeftJoin[A, B]:
            type R = Tuple.Concat[WrapToTuple[A], WrapToTuple[to.R]]

            def join(a: A, b: B): R =
                wrapToTuple(a) ++ wrapToTuple(to.toOption(b))

/**
 * Computes the combined row type of a right outer join.
 * The left side is wrapped with `Option`.
 */
@implicitAmbiguous("This value cannot be used as a join operand.")
@implicitNotFound("This value cannot be used as a join operand.")
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
        to: ToOption[A]
    ): Aux[A, B, Tuple.Concat[WrapToTuple[to.R], WrapToTuple[B]]] =
        new RightJoin[A, B]:
            type R = Tuple.Concat[WrapToTuple[to.R], WrapToTuple[B]]

            def join(a: A, b: B): R =
                wrapToTuple(to.toOption(a)) ++ wrapToTuple(b)

/**
 * Computes the combined row type of a full outer join.
 * Both sides are wrapped with `Option`.
 */
@implicitAmbiguous("This value cannot be used as a join operand.")
@implicitNotFound("This value cannot be used as a join operand.")
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
        tob: ToOption[B]
    ): Aux[A, B, Tuple.Concat[WrapToTuple[toa.R], WrapToTuple[tob.R]]] =
        new FullJoin[A, B]:
            type R = Tuple.Concat[WrapToTuple[toa.R], WrapToTuple[tob.R]]

            def join(a: A, b: B): R =
                wrapToTuple(toa.toOption(a)) ++ wrapToTuple(tob.toOption(b))

/**
 * A join table source produced by calling `on` on a `JoinPart`.
 */
final class FromJoin[T, OKS <: Tuple, L <: Int](
    private[sqala] val params: T,
    private[sqala] val table: IrTable.Join
) extends AnyTable

/**
 * An intermediate join before the condition is specified. Created by
 * `join`, `leftJoin`, etc., and converted to `FromJoin` via `on`.
 */
final class JoinPart[T, OKS <: Tuple, L <: Int](
    private[sqala] val params: T,
    private[sqala] val table: IrTable.Join
):
    /**
     * Specifies the join condition. Maps to SQL `ON`. The condition
     * must be a valid filter expression — aggregate functions, window
     * functions, and other expressions not allowed in `ON` are rejected
     * at compile time.
     *
     * {{{
     * // Two-table join
     * from(Channel.join(Post).on((c, p) => c.id === p.channelId))
     *
     * // Multi-table join
     * from(
     *     Channel
     *     .join(Post).on((c, p) => c.id === p.channelId)
     *     .join(Comment).on((_, p, ct) => p.id === ct.postId)
     * )
     *
     * // Unconditional join
     * from(Channel.join(Post).on((_, _) => true))
     * }}}
     */
    def on[F](f: T => F)(using
        qc: QueryContext[L],
        a: AsExpr[F, L],
        ba: ValidateArgType[SqlBoolean, a.R, "boolean"],
        kt: KindToTuple[a.K],
        ci: CanInSimpleClause[kt.R, "on"],
        e: ExcludeCurrentLevelColumn[kt.R, L],
        c: CombineKindTuple[OKS, e.R]
    ): FromJoin[T, c.R, L] =
        val cond = a.asExpr(f(params))
        FromJoin(
            params,
            table.copy(on = Some(cond.expr))
        )