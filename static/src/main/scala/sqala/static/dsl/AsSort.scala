package sqala.static.dsl

import sqala.ast.expr.SqlExpr
import sqala.ast.order.SqlOrdering
import sqala.metadata.AsSqlExpr
import sqala.static.dsl.statement.query.Query

import scala.annotation.implicitNotFound

@implicitNotFound("Type ${T} cannot be converted to SQL expressions.")
trait AsSortItem[T]:
    def asSort(x: T): Sort[?]

object AsSortItem:
    given expr[T: AsSqlExpr]: AsSortItem[Expr[T]] with
        def asSort(x: Expr[T]): Sort[?] =
            Sort(x, SqlOrdering.Asc, None)

    given sort[T: AsSqlExpr]: AsSortItem[Sort[T]] with
        def asSort(x: Sort[T]): Sort[?] =
            x

    given query[T: AsSqlExpr, Q <: Query[Expr[T]]]: AsSortItem[Q] with
        def asSort(x: Q): Sort[?] =
            Sort(Expr(SqlExpr.SubQuery(x.tree)), SqlOrdering.Asc, None)

@implicitNotFound("Type ${T} cannot be converted to SQL expressions.")
trait AsSort[T]:
    def asSort(x: T): List[Sort[?]]

object AsSort:
    given sort[T: AsSortItem as a]: AsSort[T] with
        def asSort(x: T): List[Sort[?]] =
            a.asSort(x) :: Nil

    given tuple[H, T <: Tuple](using
        h: AsSortItem[H],
        t: AsSort[T]
    ): AsSort[H *: T] with
        def asSort(x: H *: T): List[Sort[?]] =
            h.asSort(x.head) :: t.asSort(x.tail)

    given emptyTuple: AsSort[EmptyTuple] with
        def asSort(x: EmptyTuple): List[Sort[?]] =
            Nil