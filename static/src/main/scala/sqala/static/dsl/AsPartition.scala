package sqala.static.dsl

import sqala.ast.expr.SqlExpr
import sqala.metadata.AsSqlExpr
import sqala.static.dsl.statement.query.Query

import scala.compiletime.ops.int.>

trait AsPartitionItem[T, CL <: Int]:
    type KS <: Tuple

    def asExpr(x: T): Expr[?, ?]

object AsPartitionItem:
    type Aux[T, CL <: Int, OKS <: Tuple] = AsPartitionItem[T, CL]:
        type KS = OKS

    given value[T: AsSqlExpr as a, CL <: Int]: Aux[T, CL, Value *: EmptyTuple] =
        new AsPartitionItem[T, CL]:
            type KS = Value *: EmptyTuple

            def asExpr(x: T): Expr[?, ?] =
                Expr(a.asSqlExpr(x))

    given expr[T: AsSqlExpr, EK <: ExprKind, CL <: Int](using
        kt: KindToTuple[EK],
        i: CanInWindow[kt.R]
    ): Aux[Expr[T, EK], CL, kt.R] =
        new AsPartitionItem[Expr[T, EK], CL]:
            type KS = kt.R

            def asExpr(x: Expr[T, EK]): Expr[?, ?] =
                x

    given query[T, OKS <: Tuple, L <: Int, Q <: Query[T, OKS, L, OneRow], CL <: Int](using
        a: AsExpr[T, CL],
        i: CanInWindow[OKS],
        refl: L > CL =:= true
    ): Aux[Q, CL, OKS] =
        new AsPartitionItem[Q, CL]:
            type KS = OKS

            def asExpr(x: Q): Expr[?, ?] =
                Expr(SqlExpr.Subquery(None, x.tree))

trait AsPartition[T, CL <: Int]:
    type KS <: Tuple

    def asExprs(x: T): List[Expr[?, ?]]

object AsPartition:
    type Aux[T, CL <: Int, OKS <: Tuple] = AsPartition[T, CL]:
        type KS = OKS

    given item[T, CL <: Int](using a: AsPartitionItem[T, CL]): Aux[T, CL, a.KS] =
        new AsPartition[T, CL]:
            type KS = a.KS

            def asExprs(x: T): List[Expr[?, ?]] =
                a.asExpr(x) :: Nil

    given tuple[H, T <: Tuple, CL <: Int](using
        h: AsPartitionItem[H, CL],
        t: AsPartition[T, CL],
        c: CombineKindTuple[h.KS, t.KS]
    ): Aux[H *: T, CL, c.R] =
        new AsPartition[H *: T, CL]:
            type KS = c.R

            def asExprs(x: H *: T): List[Expr[?, ?]] =
                h.asExpr(x.head) :: t.asExprs(x.tail)

    given tuple1[H, CL <: Int](using
        h: AsPartitionItem[H, CL],
        c: CombineKindTuple[h.KS, EmptyTuple]
    ): Aux[H *: EmptyTuple, CL, c.R] =
        new AsPartition[H *: EmptyTuple, CL]:
            type KS = c.R

            def asExprs(x: H *: EmptyTuple): List[Expr[?, ?]] =
                h.asExpr(x.head) :: Nil

trait AsRecognizePartition[T, CL <: Int]:
    def asExprs(x: T): List[Expr[?, ?]]

object AsRecognizePartition:
    given expr[T: AsSqlExpr, EL <: Int, CL <: Int](using
        EL =:= CL
    ): AsRecognizePartition[Expr[T, Column[EL]], CL] with
        def asExprs(x: Expr[T, Column[EL]]): List[Expr[?, ?]] =
            x :: Nil

    given tuple[H, T <: Tuple, CL <: Int](using
        h: AsRecognizePartition[H, CL],
        e: AsExpr[H, CL],
        a: AsSqlExpr[e.R],
        t: AsRecognizePartition[T, CL]
    ): AsRecognizePartition[H *: T, CL] with
        def asExprs(x: H *: T): List[Expr[?, ?]] =
            h.asExprs(x.head) ++ t.asExprs(x.tail)

    given tuple1[H, CL <: Int](using
        h: AsRecognizePartition[H, CL],
        e: AsExpr[H, CL],
        a: AsSqlExpr[e.R]
    ): AsRecognizePartition[H *: EmptyTuple, CL] with
        def asExprs(x: H *: EmptyTuple): List[Expr[?, ?]] =
            h.asExprs(x.head)