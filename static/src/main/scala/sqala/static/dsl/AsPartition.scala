package sqala.static.dsl

import sqala.ast.expr.SqlExpr
import sqala.static.dsl.statement.query.Query
import sqala.static.metadata.AsSqlExpr

trait AsPartition[T]:
    type K <: ExprKind

    def asExprs(x: T): List[Expr[?, ?]]

object AsPartition:
    type Aux[T, OK] = AsPartition[T]:
        type K = OK

    given value[T: AsSqlExpr as a]: Aux[T, Value] =
        new AsPartition[T]:
            type K = Value

            def asExprs(x: T): List[Expr[?, ?]] =
                Expr(a.asSqlExpr(x)) :: Nil

    given expr[T: AsSqlExpr, EK <: ExprKind](using CanInOver[EK]): Aux[Expr[T, EK], EK] =
        new AsPartition[Expr[T, EK]]:
            type K = EK

            def asExprs(x: Expr[T, EK]): List[Expr[?, ?]] =
                x :: Nil

    given query[T, Q <: Query[T, OneRow]](using a: AsPartition[T]): Aux[Q, ValueOperation] =
        new AsPartition[Q]:
            type K = ValueOperation

            def asExprs(x: Q): List[Expr[?, ?]] =
                Expr(SqlExpr.SubQuery(x.tree)) :: Nil

    given tuple[H, T <: Tuple](using
        h: AsPartition[H],
        t: AsPartition[T],
        e: AsExpr[H],
        a: AsSqlExpr[e.R],
        o: KindOperation[h.K, t.K]
    ): Aux[H *: T, o.R] =
        new AsPartition[H *: T]:
            type K = o.R

            def asExprs(x: H *: T): List[Expr[?, ?]] =
                h.asExprs(x.head) ++ t.asExprs(x.tail)

    given tuple1[H](using
        h: AsPartition[H],
        e: AsExpr[H],
        a: AsSqlExpr[e.R],
        o: KindOperation[h.K, Value]
    ): Aux[H *: EmptyTuple, o.R] =
        new AsPartition[H *: EmptyTuple]:
            type K = o.R

            def asExprs(x: H *: EmptyTuple): List[Expr[?, ?]] =
                h.asExprs(x.head)

trait AsRecognizePartition[T]:
    def asExprs(x: T): List[Expr[?, ?]]

object AsRecognizePartition:
    given expr[T]: AsRecognizePartition[Expr[T, Column]] with
        def asExprs(x: Expr[T, Column]): List[Expr[?, ?]] =
            x :: Nil

    given tuple[H, T <: Tuple](using
        h: AsRecognizePartition[H],
        e: AsExpr[H],
        a: AsSqlExpr[e.R],
        t: AsRecognizePartition[T]
    ): AsRecognizePartition[H *: T] with
        def asExprs(x: H *: T): List[Expr[?, ?]] =
            h.asExprs(x.head) ++ t.asExprs(x.tail)

    given tuple1[H](using
        h: AsRecognizePartition[H],
        e: AsExpr[H],
        a: AsSqlExpr[e.R]
    ): AsRecognizePartition[H *: EmptyTuple] with
        def asExprs(x: H *: EmptyTuple): List[Expr[?, ?]] =
            h.asExprs(x.head)