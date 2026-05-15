package sqala.static.dsl

import sqala.ast.expr.SqlExpr
import sqala.metadata.AsSqlExpr

import scala.compiletime.ops.boolean.||

trait CanIn[A, B, CL <: Int]:
    type R

    type KS <: Tuple

    def asExprs(x: B): List[Expr[?, ?]]

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
        r: Relation[aa.R, ab.R, IsOption[aa.R] || IsOption[ab.R]],
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
        rh: Relation[aa.R, ah.R, IsOption[aa.R] || IsOption[ah.R]],
        r: Relation[rh.R, t.R, IsOption[rh.R] || IsOption[t.R]],
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
        r: Relation[Unwrap[aa.R, Option], Unwrap[ah.R, Option], IsOption[aa.R] || IsOption[ah.R]],
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
        r: Relation[Unwrap[aa.R, Option], Unwrap[B, Option], IsOption[A] || IsOption[B]],
    ): Aux[A, S, CL, r.R, Value *: EmptyTuple] =
        new CanIn[A, S, CL]:
            type R = r.R

            type KS = Value *: EmptyTuple

            def asExprs(x: S): List[Expr[?, ?]] =
                x.toList.map(i => Expr(ab.asSqlExpr(i)))

    given array[A, B, CL <: Int](using
        aa: AsExpr[A, CL],
        ab: AsSqlExpr[B],
        r: Relation[Unwrap[aa.R, Option], Unwrap[B, Option], IsOption[A] || IsOption[B]],
    ): Aux[A, Array[B], CL, r.R, Value *: EmptyTuple] =
        new CanIn[A, Array[B], CL]:
            type R = r.R

            type KS = Value *: EmptyTuple

            def asExprs(x: Array[B]): List[Expr[?, ?]] =
                x.toList.map(i => Expr(ab.asSqlExpr(i)))