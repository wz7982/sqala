package sqala.static.dsl

import sqala.ir.IrExpr
import sqala.metadata.AsSqlExpr
import sqala.util.NonEmptyList

import scala.compiletime.ops.int.>
import sqala.static.dsl.statement.query.Query
import scala.compiletime.error
import scala.util.NotGiven
import scala.quoted.Type
import scala.quoted.Quotes

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
    def asExprs(x: T): NonEmptyList[Expr[?, ?]]

    /**
     * Converts the value to a single expression, wrapping multiple
     * sub-expressions in a tuple.
     */
    def asExpr(x: T): Expr[R, K] =
        val exprList = asExprs(x)
        if exprList.size == 1 then
            Expr(exprList.head.asIrExpr)
        else
            val exprs = exprList.map(_.asIrExpr)
            Expr(IrExpr.Tuple(exprs))

    /**
     * Converts the value to a single expression, erasing the type.
     */
    def asErasedExpr(x: T): Expr[?, ?] =
        asExpr(x)

object AsExpr:
    type Aux[T, CL <: Int, O, OK <: ExprKind] = AsExpr[T, CL]:
        type R = O

        type K = OK

    given expr[T, EK <: ExprKind, E <: Expr[T, EK], CL <: Int]: Aux[E, CL, T, EK] =
        new AsExpr[E, CL]:
            type R = T

            type K = EK

            def asExprs(x: E): NonEmptyList[Expr[?, ?]] =
                NonEmptyList(x, Nil)

    given value[T: AsSqlExpr as a, CL <: Int]: Aux[T, CL, T, Value] =
        new AsExpr[T, CL]:
            type R = T

            type K = Value

            def asExprs(x: T): NonEmptyList[Expr[?, ?]] =
                NonEmptyList(Expr(IrExpr.Native(a.asSqlExpr(x))), Nil)

    given query[T, OKS <: Tuple, L <: Int, S <: QuerySize, Q <: Query[T, OKS, L, S], CL <: Int](using
        a: AsExpr[T, CL],
        to: ToOption[Expr[a.R, Column[CL]]],
        ao: AsExpr[to.R, CL],
        vs: ValidateSubqueryExprSize[S],
        vss: ValidateSubqueryScope[L, CL]
    ): Aux[Q, CL, ao.R, Composite[OKS]] =
        new AsExpr[Q, CL]:
            type R = ao.R

            type K = Composite[OKS]

            def asExprs(x: Q): NonEmptyList[Expr[?, ?]] =
                NonEmptyList(Expr(IrExpr.Subquery(x.tree)), Nil)

    inline given window[T, KS <: Tuple, CL <: Int]: Aux[WindowPart[T, KS], CL, WindowPart[T, KS], Window[KS]] =
        error("Window function requires an over clause.")

    given tuple[H, T <: Tuple, CL <: Int](using
        ah: AsExpr[H, CL],
        at: AsExpr[T, CL],
        tt: ToTuple[at.R],
        ck: CombineKind[ah.K, at.K]
    ): Aux[H *: T, CL, ah.R *: tt.R, ck.R] =
        new AsExpr[H *: T, CL]:
            type R = ah.R *: tt.R

            type K = ck.R

            def asExprs(x: H *: T): NonEmptyList[Expr[?, ?]] =
                ah.asExpr(x.head) :: at.asExprs(x.tail)

    given tuple1[H, CL <: Int](using
        h: AsExpr[H, CL],
        kt: KindToTuple[h.K]
    ): Aux[H *: EmptyTuple, CL, h.R *: EmptyTuple, Composite[kt.R]] =
        new AsExpr[H *: EmptyTuple, CL]:
            type R = h.R *: EmptyTuple

            type K = Composite[kt.R]

            def asExprs(x: H *: EmptyTuple): NonEmptyList[Expr[?, ?]] =
                NonEmptyList(h.asExpr(x.head), Nil)

    inline given fallback[T, CL <: Int](using
        NotGiven[AsExpr[T, CL]]
    ): Aux[T, CL, EmptyTuple, Composite[EmptyTuple]] =
        ${ errorMacro[T] }

    def errorMacro[T: Type](using q: Quotes): Nothing =
        import q.reflect.*

        val t = Type.show[T]

        report.errorAndAbort(s"Cannot convert type $t into a SQL expression.")