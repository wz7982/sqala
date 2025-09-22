package sqala.static.dsl

import sqala.ast.expr.SqlExpr

import scala.annotation.implicitNotFound
import scala.compiletime.ops.boolean.||

@implicitNotFound("Types ${A} and ${B} be cannot compared.")
trait CanIn[A, B]:
    type R

    def exprs(x: B): List[Expr[?]]

    def asExpr(x: B): Expr[?] =
        val exprList = exprs(x)
        exprList.map(_.asSqlExpr) match
            case SqlExpr.SubQuery(ast) :: Nil => 
                Expr(SqlExpr.SubQuery(ast))
            case e => 
                Expr(SqlExpr.Tuple(e))

object CanIn:
    type Aux[A, B, O] = CanIn[A, B]:
        type R = O

    given in[A, B](using
        a: AsExpr[B],
        r: Relation[Unwrap[A, Option], Unwrap[a.R, Option], IsOption[A] || IsOption[a.R]]
    ): Aux[A, B, r.R] =
        new CanIn[A, B]:
            type R = r.R

            def exprs(x: B): List[Expr[?]] =
                a.asExpr(x) :: Nil

    given inTuple[A, H, T <: Tuple](using
        a: AsExpr[H],
        t: CanIn[A, T],
        rh: Relation[Unwrap[A, Option], Unwrap[a.R, Option], IsOption[A] || IsOption[a.R]],
        r: Relation[Unwrap[rh.R, Option], Unwrap[t.R, Option], IsOption[rh.R] || IsOption[t.R]]
    ): Aux[A, H *: T, r.R] =
        new CanIn[A, H *: T]:
            type R = r.R

            def exprs(x: H *: T): List[Expr[?]] =
                a.asExpr(x.head) :: t.exprs(x.tail)

    given inEmptyTuple[A]: Aux[A, EmptyTuple, WrapIf[A, IsOption[A], Option]] =
        new CanIn[A, EmptyTuple]:
            type R = WrapIf[A, IsOption[A], Option]
            
            def exprs(x: EmptyTuple): List[Expr[?]] = Nil

    given inSeq[A, B, S <: Seq[B]](using 
        a: AsExpr[B],
        r: Relation[Unwrap[A, Option], Unwrap[a.R, Option], IsOption[A] || IsOption[a.R]]
    ): Aux[A, S, r.R] =
        new CanIn[A, S]:
            type R = r.R

            def exprs(x: S): List[Expr[?]] =
                x.toList.map(a.asExpr)

    given inArray[A, B](using 
        a: AsExpr[B], 
        r: Relation[Unwrap[A, Option], Unwrap[a.R, Option], IsOption[A] || IsOption[a.R]]
    ): Aux[A, Array[B], r.R] =
        new CanIn[A, Array[B]]:
            type R = r.R
            
            def exprs(x: Array[B]): List[Expr[?]] =
                x.toList.map(a.asExpr)