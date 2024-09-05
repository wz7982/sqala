package sqala.dsl.statement.query

import sqala.ast.expr.SqlExpr
import sqala.dsl.{Expr, ExprKind}

trait GroupingSetsItem[T]:
    def asSqlExpr(x: T): SqlExpr

object GroupingSetsItem:
    given exprGrouping[T, K <: ExprKind]: GroupingSetsItem[Expr[T, K]] with
        override def asSqlExpr(x: Expr[T, K]): SqlExpr =
            x.asSqlExpr

    given tupleGrouping[T, K <: ExprKind, Tail <: Tuple](using hi: GroupingSetsItem[Expr[T, K]], ti: GroupingSetsItem[Tail]): GroupingSetsItem[Expr[T, K] *: Tail] with
        override def asSqlExpr(x: Expr[T, K] *: Tail): SqlExpr =
            val tailExpr = ti.asSqlExpr(x.tail) match
                case SqlExpr.Vector(list) => list
                case i => i :: Nil
            SqlExpr.Vector(hi.asSqlExpr(x.head) :: tailExpr)

    given emptyTupleGrouping: GroupingSetsItem[EmptyTuple] with
        override def asSqlExpr(x: EmptyTuple): SqlExpr =
            SqlExpr.Vector(Nil)

    given unitGrouping: GroupingSetsItem[Unit] with
        override def asSqlExpr(x: Unit): SqlExpr =
            SqlExpr.Vector(Nil)

trait GroupingSets[T]:
    def asSqlExprs(x: T): List[SqlExpr]

object GroupingSets:
    given exprGrouping[T, K <: ExprKind]: GroupingSets[Expr[T, K]] with
        override def asSqlExprs(x: Expr[T, K]): List[SqlExpr] =
            x.asSqlExpr :: Nil

    given tupleGrouping[H, T <: Tuple](using hi: GroupingSetsItem[H], ti: GroupingSets[T]): GroupingSets[H *: T] with
        override def asSqlExprs(x: H *: T): List[SqlExpr] =
            hi.asSqlExpr(x.head) :: ti.asSqlExprs(x.tail)

    given emptyTupleGrouping: GroupingSets[EmptyTuple] with
        override def asSqlExprs(x: EmptyTuple): List[SqlExpr] = Nil