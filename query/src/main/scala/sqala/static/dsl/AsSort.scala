package sqala.static.dsl

import sqala.ast.order.*
import sqala.common.AsSqlExpr
import sqala.static.statement.query.Query

trait AsSort[T]:
    def asSort(x: T): List[SqlOrderItem]

object AsSort:
    given value[T: AsSqlExpr as a]: AsSort[T] with
        def asSort(x: T): List[SqlOrderItem] =
            SqlOrderItem(a.asSqlExpr(x), Some(SqlOrderOption.Asc), None) :: Nil

    given expr[T]: AsSort[Expr[T]] with
        def asSort(x: Expr[T]): List[SqlOrderItem] =
            SqlOrderItem(x.asSqlExpr, Some(SqlOrderOption.Asc), None) :: Nil

    given sort[T]: AsSort[Sort[T]] with
        def asSort(x: Sort[T]): List[SqlOrderItem] =
            x.asSqlOrderBy :: Nil

    given query[T]: AsSort[Query[T]] with
        def asSort(x: Query[T]): List[SqlOrderItem] =
            SqlOrderItem(Expr.SubQuery(x.ast).asSqlExpr, Some(SqlOrderOption.Asc), None) :: Nil

    given tuple[H, T <: Tuple](using
        h: AsSort[H],
        t: AsSort[T]
    ): AsSort[H *: T] with
        def asSort(x: H *: T): List[SqlOrderItem] =
            h.asSort(x.head) ++ t.asSort(x.tail)

    given emptyTuple: AsSort[EmptyTuple] with
        def asSort(x: EmptyTuple): List[SqlOrderItem] =
            Nil