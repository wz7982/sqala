package sqala.static.dsl

import sqala.common.AsSqlExpr
import sqala.static.statement.query.Query

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

    given mergeValue[T: AsSqlExpr as a]: Aux[T, T] =
        new Merge[T]:
            type R = T

            def exprs(x: T): List[Expr[?]] = 
                Expr.Literal(x, a) :: Nil            

    given mergeExpr[T]: Aux[Expr[T], T] =
        new Merge[Expr[T]]:
            type R = T

            def exprs(x: Expr[T]): List[Expr[?]] = 
                x :: Nil

    given mergeQuery[Q](using m: Merge[Q]): Aux[Query[Q], m.R] =
        new Merge[Query[Q]]:
            type R = m.R

            def exprs(x: Query[Q]): List[Expr[?]] =
                Expr.SubQuery(x.ast) :: Nil

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

@implicitNotFound("Types ${L} and ${R} be cannot compared.")
trait MergeIn[L, R]:
    def exprs(x: R): List[Expr[?]]

    def asExpr(x: R): Expr[?] =
        val exprList = exprs(x)
        exprList match
            case Expr.SubQuery(ast) :: Nil => Expr.SubQuery(ast)
            case _ => Expr.Tuple(exprList)

object MergeIn:
    given merge[L, R](using 
        m: Merge[R],
        c: CompareOperation[Unwrap[L, Option], Unwrap[m.R, Option]]
    ): MergeIn[L, R] with
        def exprs(x: R): List[Expr[?]] =
            m.asExpr(x) :: Nil

    given mergeTuple[L, H, T <: Tuple](using
        m: Merge[H],
        t: MergeIn[L, T],
        c: CompareOperation[Unwrap[L, Option], Unwrap[m.R, Option]]
    ): MergeIn[L, H *: T] with
        def exprs(x: H *: T): List[Expr[?]] =
            m.asExpr(x.head) :: t.exprs(x.tail)

    given mergeEmptyTuple[L]: MergeIn[L, EmptyTuple] with
        def exprs(x: EmptyTuple): List[Expr[?]] = Nil