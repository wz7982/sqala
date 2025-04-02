package sqala.static.dsl

import sqala.ast.expr.SqlExpr

import scala.annotation.implicitNotFound

@implicitNotFound("Types ${L} and ${R} be cannot compared.")
trait CanIn[L, R]:
    def exprs(x: R): List[Expr[?]]

    def asExpr(x: R): Expr[?] =
        val exprList = exprs(x)
        exprList.map(_.asSqlExpr) match
            case SqlExpr.SubQuery(ast) :: Nil => Expr.SubQuery(ast)
            case _ => Expr.Tuple(exprList)

object CanIn:
    given in[L, R](using
        a: AsExpr[R],
        c: Compare[Unwrap[L, Option], Unwrap[a.R, Option]]
    ): CanIn[L, R] with
        def exprs(x: R): List[Expr[?]] =
            a.asExpr(x) :: Nil

    given inTuple[L, H, T <: Tuple](using
        a: AsExpr[H],
        t: CanIn[L, T],
        c: Compare[Unwrap[L, Option], Unwrap[a.R, Option]]
    ): CanIn[L, H *: T] with
        def exprs(x: H *: T): List[Expr[?]] =
            a.asExpr(x.head) :: t.exprs(x.tail)

    given inEmptyTuple[L]: CanIn[L, EmptyTuple] with
        def exprs(x: EmptyTuple): List[Expr[?]] = Nil

    given inSeq[L, R, S <: Seq[R]](using 
        a: AsExpr[R], 
        c: Compare[Unwrap[L, Option], Unwrap[a.R, Option]]
    ): CanIn[L, S] with
        def exprs(x: S): List[Expr[?]] =
            x.toList.map(a.asExpr(_))

    given inArray[L, R](using 
        a: AsExpr[R], 
        c: Compare[Unwrap[L, Option], Unwrap[a.R, Option]]
    ): CanIn[L, Array[R]] with
        def exprs(x: Array[R]): List[Expr[?]] =
            x.toList.map(a.asExpr(_))