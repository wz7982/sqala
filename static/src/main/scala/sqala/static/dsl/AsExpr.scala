package sqala.static.dsl

import sqala.ast.expr.SqlExpr
import sqala.metadata.AsSqlExpr
import sqala.static.dsl.statement.query.Query
import sqala.util.NonEmptyList.toNonEmptyList

import scala.compiletime.ops.int.>

/**
 * Lifts values, expressions, subqueries, and tuples into the expression
 * layer for use in `filter`, `map`, and other DSL methods.
 * `CL` is the current query context level.
 */
trait AsExpr[T, CL <: Int]:
    /**
     * The result type of the expression.
     */
    type R

    /**
     * The expression kind.
     */
    type K <: ExprKind

    /**
     * Converts the value to a list of expressions.
     */
    def asExprs(x: T): List[Expr[?, ?]]

    /**
     * Converts the value to a single expression, wrapping multiple
     * sub-expressions in a tuple.
     */
    def asExpr(x: T): Expr[R, K] =
        val exprList = asExprs(x)
        if exprList.size == 1 then
            Expr(exprList.head.asSqlExpr)
        else
            val exprs = exprList.map(_.asSqlExpr)
            Expr(SqlExpr.Tuple(exprs.toNonEmptyList))

object AsExpr:
    type Aux[T, CL <: Int, O, OK <: ExprKind] = AsExpr[T, CL]:
        type R = O

        type K = OK

    given value[T: AsSqlExpr as a, CL <: Int]: Aux[T, CL, T, Value] =
        new AsExpr[T, CL]:
            type R = T

            type K = Value

            def asExprs(x: T): List[Expr[?, ?]] =
                Expr(a.asSqlExpr(x)) :: Nil

    given expr[T, EK <: ExprKind, CL <: Int]: Aux[Expr[T, EK], CL, T, EK] =
        new AsExpr[Expr[T, EK], CL]:
            type R = T

            type K = EK

            def asExprs(x: Expr[T, EK]): List[Expr[?, ?]] =
                x :: Nil

    given query[T, OKS <: Tuple, L <: Int, Q <: Query[T, OKS, L, OneRow], CL <: Int](using
        a: AsExpr[T, CL],
        refl: L > CL =:= true
    ): Aux[Q, CL, a.R, Composite[OKS]] =
        new AsExpr[Q, CL]:
            type R = a.R

            type K = Composite[OKS]

            def asExprs(x: Q): List[Expr[?, ?]] =
                Expr(SqlExpr.Subquery(x.tree)) :: Nil

    given tuple[H, T <: Tuple, CL <: Int](using
        ah: AsExpr[H, CL],
        at: AsExpr[T, CL],
        tt: ToTuple[at.R],
        ck: CombineKind[ah.K, at.K]
    ): Aux[H *: T, CL, ah.R *: tt.R, ck.R] =
        new AsExpr[H *: T, CL]:
            type R = ah.R *: tt.R

            type K = ck.R

            def asExprs(x: H *: T): List[Expr[?, ?]] =
                ah.asExpr(x.head) :: at.asExprs(x.tail)

    given tuple1[H, CL <: Int](using
        h: AsExpr[H, CL],
        kt: KindToTuple[h.K]
    ): Aux[H *: EmptyTuple, CL, h.R *: EmptyTuple, Composite[kt.R]] =
        new AsExpr[H *: EmptyTuple, CL]:
            type R = h.R *: EmptyTuple

            type K = Composite[kt.R]

            def asExprs(x: H *: EmptyTuple): List[Expr[?, ?]] =
                h.asExpr(x.head) :: Nil