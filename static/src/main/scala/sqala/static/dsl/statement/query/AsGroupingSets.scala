package sqala.static.dsl.statement.query

import sqala.ast.expr.SqlExpr
import sqala.static.dsl.{Expr, Grouped}

/**
 * Converts a single grouping sets item into a `SqlExpr`.
 */
trait AsGroupingSetsItem[T]:
    /**
     * Converts the item to a SQL expression.
     */
    def asSqlExpr(x: T): SqlExpr

object AsGroupingSetsItem:
    given expr[T, K <: Grouped[?]]: AsGroupingSetsItem[Expr[T, K]] with
        def asSqlExpr(x: Expr[T, K]): SqlExpr =
            x.asSqlExpr

    given tuple[H, K <: Grouped[?], T <: Tuple](using
        h: AsGroupingSetsItem[Expr[H, K]],
        t: AsGroupingSetsItem[T]
    ): AsGroupingSetsItem[Expr[H, K] *: T] with
        def asSqlExpr(x: Expr[H, K] *: T): SqlExpr =
            val tailExpr = t.asSqlExpr(x.tail) match
                case SqlExpr.Tuple(list) => list
                case i => i :: Nil
            SqlExpr.Tuple(h.asSqlExpr(x.head) :: tailExpr)

    given emptyTuple: AsGroupingSetsItem[EmptyTuple] with
        def asSqlExpr(x: EmptyTuple): SqlExpr =
            SqlExpr.Tuple(Nil)

    given unit: AsGroupingSetsItem[Unit] with
        def asSqlExpr(x: Unit): SqlExpr =
            SqlExpr.Tuple(Nil)

/**
 * Collects grouping sets items into a list of `SqlExpr` for the
 * `groupBySets` clause.
 */
trait AsGroupingSets[T]:
    /**
     * Converts the grouping sets to a list of SQL expressions.
     */
    def asSqlExprs(x: T): List[SqlExpr]

object AsGroupingSets:
    given expr[T, K <: Grouped[?]]: AsGroupingSets[Expr[T, K]] with
        def asSqlExprs(x: Expr[T, K]): List[SqlExpr] =
            x.asSqlExpr :: Nil

    given tuple[H, T <: Tuple](using
        h: AsGroupingSetsItem[H],
        t: AsGroupingSets[T]
    ): AsGroupingSets[H *: T] with
        def asSqlExprs(x: H *: T): List[SqlExpr] =
            h.asSqlExpr(x.head) :: t.asSqlExprs(x.tail)

    given tuple1[H](using
        h: AsGroupingSetsItem[H]
    ): AsGroupingSets[H *: EmptyTuple] with
        def asSqlExprs(x: H *: EmptyTuple): List[SqlExpr] =
            h.asSqlExpr(x.head) :: Nil