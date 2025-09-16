package sqala.static.dsl.table

import sqala.ast.expr.SqlExpr
import sqala.static.dsl.Expr

trait AsTableParam[T]:
    def offset: Int

    def asTableParam(queryAlias: Option[String], cursor: Int): T

object AsTableParam:
    given expr[T]: AsTableParam[Expr[T]] with
        def offset: Int = 1

        def asTableParam(queryAlias: Option[String], cursor: Int): Expr[T] =
            Expr(SqlExpr.Column(queryAlias, s"c$cursor"))

    given tuple[H, T <: Tuple](using
        h: AsTableParam[H],
        t: AsTableParam[T]
    ): AsTableParam[H *: T] with
        def offset: Int = h.offset + t.offset

        def asTableParam(queryAlias: Option[String], cursor: Int): H *: T =
            h.asTableParam(queryAlias, cursor) *:
            t.asTableParam(queryAlias, cursor + h.offset)

    given tuple1[H](using h: AsTableParam[H]): AsTableParam[H *: EmptyTuple] with
        def offset: Int = h.offset

        def asTableParam(queryAlias: Option[String], cursor: Int): H *: EmptyTuple =
            h.asTableParam(queryAlias, cursor) *: EmptyTuple