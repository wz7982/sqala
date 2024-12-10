package sqala.static.common

import sqala.ast.expr.SqlExpr

import scala.annotation.implicitNotFound

@implicitNotFound("The type ${T} cannot be converted to SQL expression.")
trait Merge[T]:
    def exprs(x: T): List[SqlExpr]

    def asSqlExpr(x: T): SqlExpr =
        val exprList = exprs(x)
        if exprList.size == 1 then exprList.head
        else SqlExpr.Vector(exprList)

object Merge:
    given mergeValue[T](using a: AsSqlExpr[T]): Merge[T] with
        def exprs(x: T): List[SqlExpr] = 
            a.asSqlExpr(x) :: Nil

    given mergeTuple[H, T <: Tuple](using
        h: Merge[H],
        t: Merge[T]
    ): Merge[H *: T] with
        def exprs(x: H *: T): List[SqlExpr] =
            h.exprs(x.head) ++ t.exprs(x.tail)

    given mergeEmptyTuple: Merge[EmptyTuple] with
        def exprs(x: EmptyTuple): List[SqlExpr] = 
            Nil