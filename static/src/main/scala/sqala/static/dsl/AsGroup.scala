package sqala.static.dsl

import sqala.ast.expr.SqlExpr
import sqala.static.dsl.statement.query.Query
import sqala.static.metadata.AsSqlExpr

import scala.NamedTuple.NamedTuple
import scala.annotation.implicitNotFound

@implicitNotFound("Type ${T} cannot be converted to SQL expressions.")
trait AsGroup[T]:
    def exprs(x: T): List[Expr[?]]

    def asExpr(x: T): Expr[?] =
        val exprList = exprs(x)
        if exprList.size == 1 then
            Expr(exprList.head.asSqlExpr)
        else
            Expr(SqlExpr.Tuple(exprList.map(_.asSqlExpr)))

object AsGroup:
    given expr[T: AsSqlExpr]: AsGroup[Expr[T]] with
        def exprs(x: Expr[T]): List[Expr[?]] =
            x :: Nil

    given query[T: AsSqlExpr, Q <: Query[Expr[T]]]: AsGroup[Q] with
        def exprs(x: Q): List[Expr[?]] =
            Expr(SqlExpr.SubQuery(x.tree)) :: Nil

    given tuple[H, T <: Tuple](using
        h: AsGroup[H],
        t: AsGroup[T]
    ): AsGroup[H *: T] with
        def exprs(x: H *: T): List[Expr[?]] =
            h.asExpr(x.head) :: t.exprs(x.tail)

    given tuple1[H](using 
        h: AsGroup[H]
    ): AsGroup[H *: EmptyTuple] with
        def exprs(x: H *: EmptyTuple): List[Expr[?]] =
            h.asExpr(x.head) :: Nil

    given namedTuple[N <: Tuple, V <: Tuple : AsGroup as a]: AsGroup[NamedTuple[N, V]] with
        def exprs(x: NamedTuple[N, V]): List[Expr[?]] =
            a.exprs(x.toTuple)