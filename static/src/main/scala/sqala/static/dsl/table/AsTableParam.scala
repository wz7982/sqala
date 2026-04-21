package sqala.static.dsl.table

import sqala.ast.expr.SqlExpr
import sqala.static.dsl.{Column, Expr, ExprKind, ToTuple}

trait AsTableParam[T]:
    type R

    def offset: Int

    def asTableParam(queryAlias: Option[String], cursor: Int): R

object AsTableParam:
    type Aux[T, O] = AsTableParam[T]:
        type R = O

    given expr[T, K <: ExprKind]: Aux[Expr[T, K], Expr[T, Column]] =
        new AsTableParam[Expr[T, K]]:
            type R = Expr[T, Column]

            def offset: Int = 1

            def asTableParam(queryAlias: Option[String], cursor: Int): Expr[T, Column] =
                Expr(SqlExpr.Column(queryAlias, s"c$cursor"))

    given tuple[H, T <: Tuple](using
        h: AsTableParam[H],
        t: AsTableParam[T],
        tt: ToTuple[t.R]
    ): Aux[H *: T, h.R *: tt.R] =
        new AsTableParam[H *: T]:
            type R = h.R *: tt.R

            def offset: Int = h.offset + t.offset

            def asTableParam(queryAlias: Option[String], cursor: Int): R =
                h.asTableParam(queryAlias, cursor) *: tt.toTuple(t.asTableParam(queryAlias, cursor + h.offset))

    given tuple1[H](using h: AsTableParam[H]): Aux[H *: EmptyTuple, h.R *: EmptyTuple] =
        new AsTableParam[H *: EmptyTuple]:
            type R = h.R *: EmptyTuple

            def offset: Int = h.offset

            def asTableParam(queryAlias: Option[String], cursor: Int): R =
                h.asTableParam(queryAlias, cursor) *: EmptyTuple