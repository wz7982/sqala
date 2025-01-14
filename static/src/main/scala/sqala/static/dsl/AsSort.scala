package sqala.static.dsl

import sqala.ast.order.*

trait AsSort[T]:
    def asSort(x: T): List[SqlOrderBy]

object AsSort:
    given expr[T]: AsSort[Expr[T]] with
        def asSort(x: Expr[T]): List[SqlOrderBy] =
            SqlOrderBy(x.asSqlExpr, Some(SqlOrderByOption.Asc), None) :: Nil

    given sort[T]: AsSort[Sort[T]] with
        def asSort(x: Sort[T]): List[SqlOrderBy] = 
            x.asSqlOrderBy :: Nil

    given tuple[H, T <: Tuple](using 
        h: AsSort[H],
        t: AsSort[T]
    ): AsSort[H *: T] with
        def asSort(x: H *: T): List[SqlOrderBy] = 
            h.asSort(x.head) ++ t.asSort(x.tail)

    given emptyTuple: AsSort[EmptyTuple] with
        def asSort(x: EmptyTuple): List[SqlOrderBy] =
            Nil