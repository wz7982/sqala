package sqala.static.dsl

import sqala.ast.order.*

trait AsSort[T]:
    def asSort(x: T): List[SqlOrderItem]

object AsSort:
    given expr[T]: AsSort[Expr[T]] with
        def asSort(x: Expr[T]): List[SqlOrderItem] =
            SqlOrderItem(x.asSqlExpr, Some(SqlOrderOption.Asc), None) :: Nil

    given sort[T]: AsSort[Sort[T]] with
        def asSort(x: Sort[T]): List[SqlOrderItem] =
            x.asSqlOrderBy :: Nil

    given tuple[H, T <: Tuple](using
        h: AsSort[H],
        t: AsSort[T]
    ): AsSort[H *: T] with
        def asSort(x: H *: T): List[SqlOrderItem] =
            h.asSort(x.head) ++ t.asSort(x.tail)

    given emptyTuple: AsSort[EmptyTuple] with
        def asSort(x: EmptyTuple): List[SqlOrderItem] =
            Nil