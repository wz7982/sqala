package sqala.dsl.statement.query

import sqala.ast.expr.SqlExpr
import sqala.dsl.Expr

import scala.annotation.implicitNotFound

@implicitNotFound("Type ${T} cannot be converted to GROUPING SETS expressions.")
trait GroupingSetsItem[T]:
    def asSqlExpr(x: T): SqlExpr

object GroupingSetsItem:
    given exprGrouping[T]: GroupingSetsItem[Expr[T]] with
        override def asSqlExpr(x: Expr[T]): SqlExpr =
            x.asSqlExpr

    given tupleGrouping[T, Tail <: Tuple](using
        hi: GroupingSetsItem[Expr[T]],
        ti: GroupingSetsItem[Tail]
    ): GroupingSetsItem[Expr[T] *: Tail] with
        override def asSqlExpr(x: Expr[T] *: Tail): SqlExpr =
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

@implicitNotFound("Type ${T} cannot be converted to GROUPING SETS expressions.")
trait GroupingSets[T]:
    def asSqlExprs(x: T): List[SqlExpr]

object GroupingSets:
    given exprGrouping[T]: GroupingSets[Expr[T]] with
        override def asSqlExprs(x: Expr[T]): List[SqlExpr] =
            x.asSqlExpr :: Nil

    given tupleGrouping[H, T <: Tuple](using
        hi: GroupingSetsItem[H],
        ti: GroupingSets[T]
    ): GroupingSets[H *: T] with
        override def asSqlExprs(x: H *: T): List[SqlExpr] =
            hi.asSqlExpr(x.head) :: ti.asSqlExprs(x.tail)

    given tuple1Grouping[H](using hi: GroupingSetsItem[H]): GroupingSets[H *: EmptyTuple] with
        override def asSqlExprs(x: H *: EmptyTuple): List[SqlExpr] =
            hi.asSqlExpr(x.head) :: Nil