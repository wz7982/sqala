package sqala.dsl

import scala.annotation.implicitNotFound

@implicitNotFound("Type ${T} cannot be converted to PARTITION BY expressions.")
trait PartitionArg[T]:
    def exprs(x: T): List[Expr[?, ?]]

object PartitionArg:
    given expr[T, K <: SimpleKind]: PartitionArg[Expr[T, K]] with
        def exprs(x: Expr[T, K]): List[Expr[?, ?]] =
            x :: Nil

    given tuple[X, K <: SimpleKind, T <: Tuple](using 
        t: PartitionArg[T]
    ): PartitionArg[Expr[X, K] *: T] with
        def exprs(x: Expr[X, K] *: T): List[Expr[?, ?]] =
            x.head :: t.exprs(x.tail)

    given tuple1[X, K <: SimpleKind]: PartitionArg[Expr[X, K] *: EmptyTuple] with
        def exprs(x: Expr[X, K] *: EmptyTuple): List[Expr[?, ?]] =
            x.head :: Nil

@implicitNotFound("Type ${T} cannot be converted to ORDER BY expressions.")
trait OrderArg[T]:
    def orders(x: T): List[OrderBy[?, ?]]

object OrderArg:
    given order[T, K <: ColumnKind | CommonKind]: OrderArg[OrderBy[T, K]] with
        def orders(x: OrderBy[T, K]): List[OrderBy[?, ?]] =
            x :: Nil

    given tuple[X, K <: ColumnKind | CommonKind, T <: Tuple](using 
        t: OrderArg[T]
    ): OrderArg[OrderBy[X, K] *: T] with
        def orders(x: OrderBy[X, K] *: T): List[OrderBy[?, ?]] =
            x.head :: t.orders(x.tail)

    given tuple1[X, K <: ColumnKind | CommonKind]: OrderArg[OrderBy[X, K] *: EmptyTuple] with
        def orders(x: OrderBy[X, K] *: EmptyTuple): List[OrderBy[?, ?]] =
            x.head :: Nil

@implicitNotFound("Type ${T} cannot be converted to GROUPING expressions.")
trait GroupingArg[T]:
    def exprs(x: T): List[Expr[?, ?]]

object GroupingArg:
    given expr[T]: GroupingArg[Expr[T, GroupKind]] with
        def exprs(x: Expr[T, GroupKind]): List[Expr[?, ?]] =
            x :: Nil

    given tuple[X, T <: Tuple](using 
        t: GroupingArg[T]
    ): GroupingArg[Expr[X, GroupKind] *: T] with
        def exprs(x: Expr[X, GroupKind] *: T): List[Expr[?, ?]] =
            x.head :: t.exprs(x.tail)

    given tuple1[X]: GroupingArg[Expr[X, GroupKind] *: EmptyTuple] with
        def exprs(x: Expr[X, GroupKind] *: EmptyTuple): List[Expr[?, ?]] =
            x.head :: Nil