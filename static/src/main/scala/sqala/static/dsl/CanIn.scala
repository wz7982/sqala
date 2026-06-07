package sqala.static.dsl

import sqala.ast.expr.SqlExpr
import sqala.metadata.AsSqlExpr

/**
 * Lifts values, tuples, sequences, and arrays into `in` operands, 
 * computing the result type and kind tuple. `CL` is the
 * current query context level.
 */
trait CanIn[A, B, CL <: Int]:
    /**
     * The result type.
     */
    type R

    /**
     * The kind tuple.
     */
    type KS <: Tuple

    /**
     * Converts the value to a list of expressions.
     */
    def asExprs(x: B): List[Expr[?, ?]]

    /**
     * Converts the value to a single expression, wrapping multiple
     * operands in a tuple. Subqueries are passed through without
     * wrapping.
     */
    def asExpr(x: B): Expr[?, ?] =
        val exprList = asExprs(x)
        exprList.map(_.asSqlExpr) match
            case SqlExpr.Subquery(None, ast) :: Nil =>
                Expr(SqlExpr.Subquery(None, ast))
            case e =>
                Expr(SqlExpr.Tuple(e))

object CanIn:
    type Aux[A, B, CL <: Int, O, OKS <: Tuple] = CanIn[A, B, CL]:
        type R = O

        type KS = OKS

    given expr[A, B, CL <: Int](using
        aa: AsExpr[A, CL],
        ab: AsExpr[B, CL],
        r: Relation[aa.R, ab.R],
        kt: KindToTuple[ab.K]
    ): Aux[A, B, CL, r.R, kt.R] =
        new CanIn[A, B, CL]:
            type R = r.R

            type KS = kt.R

            def asExprs(x: B): List[Expr[?, ?]] =
                ab.asExpr(x) :: Nil

    given tuple[A, H, T <: Tuple, CL <: Int](using
        aa: AsExpr[A, CL],
        ah: AsExpr[H, CL],
        t: CanIn[A, T, CL],
        rh: Relation[aa.R, ah.R],
        r: Relation[rh.R, t.R],
        kth: KindToTuple[ah.K],
        c: CombineKindTuple[kth.R, t.KS]
    ): Aux[A, H *: T, CL, r.R, c.R] =
        new CanIn[A, H *: T, CL]:
            type R = r.R

            type KS = c.R

            def asExprs(x: H *: T): List[Expr[?, ?]] =
                ah.asExpr(x.head) :: t.asExprs(x.tail)

    given tuple1[A, H, CL <: Int](using
        aa: AsExpr[A, CL],
        ah: AsExpr[H, CL],
        r: Relation[aa.R, ah.R],
        kth: KindToTuple[ah.K]
    ): Aux[A, H *: EmptyTuple, CL, r.R, kth.R] =
        new CanIn[A, H *: EmptyTuple, CL]:
            type R = r.R

            type KS = kth.R

            def asExprs(x: H *: EmptyTuple): List[Expr[?, ?]] =
                ah.asExpr(x.head) :: Nil

    given seq[A, B, S <: Seq[B], CL <: Int](using
        aa: AsExpr[A, CL],
        ab: AsSqlExpr[B],
        r: Relation[aa.R, B]
    ): Aux[A, S, CL, r.R, Value *: EmptyTuple] =
        new CanIn[A, S, CL]:
            type R = r.R

            type KS = Value *: EmptyTuple

            def asExprs(x: S): List[Expr[?, ?]] =
                x.toList.map(i => Expr(ab.asSqlExpr(i)))

    given array[A, B, CL <: Int](using
        aa: AsExpr[A, CL],
        ab: AsSqlExpr[B],
        r: Relation[aa.R, B],
    ): Aux[A, Array[B], CL, r.R, Value *: EmptyTuple] =
        new CanIn[A, Array[B], CL]:
            type R = r.R

            type KS = Value *: EmptyTuple

            def asExprs(x: Array[B]): List[Expr[?, ?]] =
                x.toList.map(i => Expr(ab.asSqlExpr(i)))