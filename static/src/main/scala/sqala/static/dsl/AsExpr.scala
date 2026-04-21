package sqala.static.dsl

import sqala.ast.expr.SqlExpr
import sqala.static.dsl.statement.query.Query
import sqala.static.metadata.AsSqlExpr

trait AsExpr[T]:
    type R

    type K <: ExprKind

    def asExprs(x: T): List[Expr[?, ?]]

    def asExpr(x: T): Expr[R, K] =
        val exprList = asExprs(x)
        if exprList.size == 1 then
            Expr(exprList.head.asSqlExpr)
        else
            Expr(SqlExpr.Tuple(exprList.map(_.asSqlExpr)))

object AsExpr:
    type Aux[T, O, OK] = AsExpr[T]:
        type R = O

        type K = OK

    given value[T: AsSqlExpr as a]: Aux[T, T, Value] =
        new AsExpr[T]:
            type R = T

            type K = Value

            def asExprs(x: T): List[Expr[?, ?]] =
                Expr(a.asSqlExpr(x)) :: Nil

    given expr[T: AsSqlExpr, EK <: ExprKind]: Aux[Expr[T, EK], T, EK] =
        new AsExpr[Expr[T, EK]]:
            type R = T

            type K = EK

            def asExprs(x: Expr[T, EK]): List[Expr[?, ?]] =
                x :: Nil

    given query[T, Q <: Query[T, OneRow]](using a: AsExpr[T]): Aux[Q, a.R, Value] =
        new AsExpr[Q]:
            type R = a.R

            type K = Value

            def asExprs(x: Q): List[Expr[?, ?]] =
                Expr(SqlExpr.SubQuery(x.tree)) :: Nil

    given tuple[H, T <: Tuple](using
        h: AsExpr[H],
        t: AsExpr[T],
        tt: ToTuple[t.R],
        a: AsSqlExpr[h.R],
        o: KindOperation[h.K, t.K]
    ): Aux[H *: T, h.R *: tt.R, o.R] =
        new AsExpr[H *: T]:
            type R = h.R *: tt.R

            type K = o.R

            def asExprs(x: H *: T): List[Expr[?, ?]] =
                h.asExpr(x.head) :: t.asExprs(x.tail)

    given tuple1[H](using
        h: AsExpr[H],
        a: AsSqlExpr[h.R],
        o: KindOperation[h.K, Value]
    ): Aux[H *: EmptyTuple, h.R, o.R] =
        new AsExpr[H *: EmptyTuple]:
            type R = h.R

            type K = o.R

            def asExprs(x: H *: EmptyTuple): List[Expr[?, ?]] =
                h.asExpr(x.head) :: Nil