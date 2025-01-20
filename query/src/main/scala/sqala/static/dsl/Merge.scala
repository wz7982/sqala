package sqala.static.dsl

import sqala.common.AsSqlExpr

import scala.annotation.implicitNotFound

@implicitNotFound("The type ${T} cannot be merged into a SQL expression.")
trait Merge[T]:
    type R

    def exprs(x: T): List[Expr[?]]

    def asExpr(x: T): Expr[R] =
        val exprList = exprs(x)
        if exprList.size == 1 then
            Expr.Ref(exprList.head.asSqlExpr)
        else
            Expr.Tuple(exprList)

object Merge:
    type Aux[T, O] = Merge[T]:
        type R = O

    given mergeExpr[T: AsSqlExpr]: Aux[Expr[T], T] =
        new Merge[Expr[T]]:
            type R = T

            def exprs(x: Expr[T]): List[Expr[?]] = 
                x :: Nil

    given mergeTuple[H, T <: Tuple](using
        h: Merge[H],
        t: Merge[T],
        tt: ToTuple[t.R]
    ): Aux[H *: T, h.R *: tt.R] =
        new Merge[H *: T]:
            type R = h.R *: tt.R

            def exprs(x: H *: T): List[Expr[?]] =
                h.asExpr(x.head) :: t.exprs(x.tail)

    given mergeTuple1[H](using h: Merge[H]): Aux[H *: EmptyTuple, h.R] =
        new Merge[H *: EmptyTuple]:
            type R = h.R

            def exprs(x: H *: EmptyTuple): List[Expr[?]] =
                h.asExpr(x.head) :: Nil

@implicitNotFound("The type ${R} cannot be merged into a SQL expression.")
trait MergeIn[L, R]:
    def exprs(x: R): List[Expr[?]]

    def asExpr(x: R): Expr[?] =
        val exprList = exprs(x)
        Expr.Tuple(exprList)

object MergeIn:
    given mergeTuple[L, H, T <: Tuple](using
        t: MergeIn[L, T],
        c: CompareOperation[Unwrap[L, Option], Unwrap[H, Option]]
    ): MergeIn[L, Expr[H] *: T] with
        def exprs(x: Expr[H] *: T): List[Expr[?]] =
            x.head :: t.exprs(x.tail)

    given mergeEmptyTuple[L]: MergeIn[L, EmptyTuple] with
        def exprs(x: EmptyTuple): List[Expr[?]] = Nil