package sqala.dsl

import scala.annotation.implicitNotFound

@implicitNotFound("The type ${T} cannot be merged into a SQL expression.")
trait Merge[T]:
    type R

    def exprs(x: T): List[Expr[?]]

    def asExpr(x: T): Expr[R] =
        val exprList = exprs(x)
        Expr.Vector(exprList)

object Merge:
    type Aux[T, O] = Merge[T]:
        type R = O

    given mergeTuple[H, T <: Tuple](using
        h: MergeItem[H],
        t: Merge[T],
        tt: ToTuple[t.R]
    ): Aux[H *: T, h.R *: tt.R] =
        new Merge[H *: T]:
            type R = h.R *: tt.R

            def exprs(x: H *: T): List[Expr[?]] =
                h.asExpr(x.head) :: t.exprs(x.tail)

    given mergeTuple1[H](using h: MergeItem[H]): Aux[H *: EmptyTuple, h.R] =
        new Merge[H *: EmptyTuple]:
            type R = h.R

            def exprs(x: H *: EmptyTuple): List[Expr[?]] =
                h.asExpr(x.head) :: Nil

@implicitNotFound("The type ${T} cannot be merged into a SQL expression.")
trait MergeItem[T]:
    type R

    def asExpr(x: T): Expr[R]

object MergeItem:
    type Aux[T, O] = MergeItem[T]:
        type R = O

    given mergeValue[T: AsSqlExpr]: Aux[T, T] =
        new MergeItem[T]:
            type R = T

            def asExpr(x: T): Expr[R] =
                Expr.Literal(x, summon[AsSqlExpr[T]])

    given mergeExpr[T: AsSqlExpr]: Aux[Expr[T], T] =
        new MergeItem[Expr[T]]:
            type R = T

            def asExpr(x: Expr[T]): Expr[R] = x