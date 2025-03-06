package sqala.static.dsl

import sqala.ast.order.*
import sqala.common.AsSqlExpr
import sqala.static.statement.query.Query

trait AsSort[T]:
    def asSort(x: T): List[Sort[?]]

object AsSort:
    given value[T: AsSqlExpr as a]: AsSort[T] with
        def asSort(x: T): List[Sort[?]] =
            Sort(Expr.Literal(x, a), SqlOrderOption.Asc, None) :: Nil

    given expr[T]: AsSort[Expr[T]] with
        def asSort(x: Expr[T]): List[Sort[?]] =
            Sort(x, SqlOrderOption.Asc, None) :: Nil

    given sort[T]: AsSort[Sort[T]] with
        def asSort(x: Sort[T]): List[Sort[?]] =
            x :: Nil

    given query[T]: AsSort[Query[T]] with
        def asSort(x: Query[T]): List[Sort[?]] =
            Sort(Expr.SubQuery(x.ast), SqlOrderOption.Asc, None) :: Nil

    given tuple[H, T <: Tuple](using
        h: AsSort[H],
        t: AsSort[T]
    ): AsSort[H *: T] with
        def asSort(x: H *: T): List[Sort[?]] =
            h.asSort(x.head) ++ t.asSort(x.tail)

    given emptyTuple: AsSort[EmptyTuple] with
        def asSort(x: EmptyTuple): List[Sort[?]] =
            Nil