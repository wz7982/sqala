package sqala.static.dsl

import sqala.ast.expr.{SqlExpr, SqlWindow}

final case class WindowFunc[T, KS <: Tuple](private[sqala] val expr: SqlExpr):
    def over(): Expr[T, Window[KS]] =
        Expr(
            SqlExpr.Window(
                asSqlExpr,
                SqlWindow(
                    Nil,
                    Nil,
                    None
                )
            )
        )

    def over[OKS <: Tuple](over: OverContext ?=> Over[OKS])(using
        i: CanInWindow[OKS],
        c: CombineKindTuple[KS, OKS]
    ): Expr[T, Window[c.R]] =
        given OverContext = OverContext()
        val o = over
        Expr(
            SqlExpr.Window(
                asSqlExpr,
                SqlWindow(
                    o.partitionBy.map(_.asSqlExpr),
                    o.sortBy.map(_.asSqlOrderBy),
                    o.frame
                )
            )
        )

    private[sqala] def asSqlExpr: SqlExpr =
        Expr(expr).asSqlExpr