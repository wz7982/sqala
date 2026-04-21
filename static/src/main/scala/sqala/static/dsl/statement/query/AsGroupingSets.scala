package sqala.static.dsl.statement.query

import sqala.ast.expr.SqlExpr
import sqala.static.dsl.{Expr, Grouped}

trait AsGroupingSetsItem[T]:
    def asSqlExpr(x: T): SqlExpr

object GroupingSetsItem:
    given expr[T]: AsGroupingSetsItem[Expr[T, Grouped]] with
        override def asSqlExpr(x: Expr[T, Grouped]): SqlExpr =
            x.asSqlExpr

    given tuple[T, Tail <: Tuple](using
        hi: AsGroupingSetsItem[Expr[T, Grouped]],
        ti: AsGroupingSetsItem[Tail]
    ): AsGroupingSetsItem[Expr[T, Grouped] *: Tail] with
        override def asSqlExpr(x: Expr[T, Grouped] *: Tail): SqlExpr =
            val tailExpr = ti.asSqlExpr(x.tail) match
                case SqlExpr.Tuple(list) => list
                case i => i :: Nil
            SqlExpr.Tuple(hi.asSqlExpr(x.head) :: tailExpr)

    given emptyTuple: AsGroupingSetsItem[EmptyTuple] with
        override def asSqlExpr(x: EmptyTuple): SqlExpr =
            SqlExpr.Tuple(Nil)

    given unit: AsGroupingSetsItem[Unit] with
        override def asSqlExpr(x: Unit): SqlExpr =
            SqlExpr.Tuple(Nil)

trait AsGroupingSets[T]:
    def asSqlExprs(x: T): List[SqlExpr]

object AsGroupingSets:
    given expr[T]: AsGroupingSets[Expr[T, Grouped]] with
        override def asSqlExprs(x: Expr[T, Grouped]): List[SqlExpr] =
            x.asSqlExpr :: Nil

    given tuple[H, T <: Tuple](using
        hi: AsGroupingSetsItem[H],
        ti: AsGroupingSets[T]
    ): AsGroupingSets[H *: T] with
        override def asSqlExprs(x: H *: T): List[SqlExpr] =
            hi.asSqlExpr(x.head) :: ti.asSqlExprs(x.tail)

    given tuple1[H](using hi: AsGroupingSetsItem[H]): AsGroupingSets[H *: EmptyTuple] with
        override def asSqlExprs(x: H *: EmptyTuple): List[SqlExpr] =
            hi.asSqlExpr(x.head) :: Nil