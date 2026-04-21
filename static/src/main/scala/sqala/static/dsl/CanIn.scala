package sqala.static.dsl

import sqala.ast.expr.SqlExpr
import sqala.static.metadata.AsSqlExpr

import scala.compiletime.ops.boolean.||

trait CanIn[A, B]:
    type R

    type K <: ExprKind

    def asExprs(x: B): List[Expr[?, ?]]

    def asExpr(x: B): Expr[?, ?] =
        val exprList = asExprs(x)
        exprList.map(_.asSqlExpr) match
            case SqlExpr.SubQuery(ast) :: Nil =>
                Expr(SqlExpr.SubQuery(ast))
            case e =>
                Expr(SqlExpr.Tuple(e))

object CanIn:
    type Aux[A, B, O, OK] = CanIn[A, B]:
        type R = O

        type K = OK

    given in[A, B](using
        aa: AsExpr[A],
        ab: AsExpr[B],
        r: Relation[Unwrap[aa.R, Option], Unwrap[ab.R, Option], IsOption[aa.R] || IsOption[ab.R]],
        o: KindOperation[aa.K, ab.K]
    ): Aux[A, B, r.R, o.R] =
        new CanIn[A, B]:
            type R = r.R

            type K = o.R

            def asExprs(x: B): List[Expr[?, ?]] =
                ab.asExpr(x) :: Nil

    given inTuple[A, H, T <: Tuple](using
        aa: AsExpr[A],
        ah: AsExpr[H],
        t: CanIn[A, T],
        rh: Relation[Unwrap[aa.R, Option], Unwrap[ah.R, Option], IsOption[aa.R] || IsOption[ah.R]],
        r: Relation[Unwrap[rh.R, Option], Unwrap[t.R, Option], IsOption[rh.R] || IsOption[t.R]],
        oh: KindOperation[aa.K, ah.K],
        o: KindOperation[oh.R, t.K]
    ): Aux[A, H *: T, r.R, o.R] =
        new CanIn[A, H *: T]:
            type R = r.R

            type K = o.R

            def asExprs(x: H *: T): List[Expr[?, ?]] =
                ah.asExpr(x.head) :: t.asExprs(x.tail)

    given inEmptyTuple[A](using
        a: AsExpr[A],
        o: KindOperation[a.K, Value]
    ): Aux[A, EmptyTuple, WrapIf[a.R, IsOption[a.R], Option], o.R] =
        new CanIn[A, EmptyTuple]:
            type R = WrapIf[a.R, IsOption[a.R], Option]

            type K = o.R

            def asExprs(x: EmptyTuple): List[Expr[?, ?]] = Nil

    given inSeq[A, B, S <: Seq[B]](using
        aa: AsExpr[A],
        ab: AsSqlExpr[B],
        r: Relation[Unwrap[aa.R, Option], Unwrap[B, Option], IsOption[A] || IsOption[B]],
        o: KindOperation[aa.K, Value]
    ): Aux[A, S, r.R, o.R] =
        new CanIn[A, S]:
            type R = r.R

            type K = o.R

            def asExprs(x: S): List[Expr[?, ?]] =
                x.toList.map(i => Expr(ab.asSqlExpr(i)))

    given inArray[A, B](using
        aa: AsExpr[A],
        ab: AsSqlExpr[B],
        r: Relation[Unwrap[aa.R, Option], Unwrap[B, Option], IsOption[A] || IsOption[B]],
        o: KindOperation[aa.K, Value]
    ): Aux[A, Array[B], r.R, o.R] =
        new CanIn[A, Array[B]]:
            type R = r.R

            type K = o.R

            def asExprs(x: Array[B]): List[Expr[?, ?]] =
                x.toList.map(i => Expr(ab.asSqlExpr(i)))