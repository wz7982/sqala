package sqala.static.dsl.statement.query

import sqala.ast.group.SqlGroupingItem
import sqala.static.dsl.*
import sqala.util.NonEmptyList.toNonEmptyList

/**
 * Collects grouping items into a list of `SqlGroupingItem`.
 */
trait AsMultidimensionalGrouping[T]:
    /**
     * Converts the grouping items to a list of SQL grouping items.
     */
    def asSqlGroupingItems(x: T): List[SqlGroupingItem]

object AsMultidimensionalGrouping:
    given expr[T, K <: ExprKind](using 
        kt: KindToTuple[K],
        ak: AllIsKind[kt.R, Grouped[?]],
        refl: ak.R =:= true
    ): AsMultidimensionalGrouping[Expr[T, K]] with
        def asSqlGroupingItems(x: Expr[T, K]): List[SqlGroupingItem] =
            SqlGroupingItem.Expr(x.asSqlExpr) :: Nil

    given unit: AsMultidimensionalGrouping[Unit] with
        def asSqlGroupingItems(x: Unit): List[SqlGroupingItem] =
            SqlGroupingItem.EmptyGroup :: Nil

    given cube: AsMultidimensionalGrouping[Cube] with
        def asSqlGroupingItems(x: Cube): List[SqlGroupingItem] =
            SqlGroupingItem.Cube(x.exprs.toNonEmptyList) :: Nil

    given rollup: AsMultidimensionalGrouping[Rollup] with
        def asSqlGroupingItems(x: Rollup): List[SqlGroupingItem] =
            SqlGroupingItem.Rollup(x.exprs.toNonEmptyList) :: Nil

    given groupingSets: AsMultidimensionalGrouping[GroupingSets] with
        def asSqlGroupingItems(x: GroupingSets): List[SqlGroupingItem] =
            SqlGroupingItem.GroupingSets(x.items.toNonEmptyList) :: Nil

    given tuple[H, T <: Tuple](using
        h: AsMultidimensionalGrouping[H],
        t: AsMultidimensionalGrouping[T]
    ): AsMultidimensionalGrouping[H *: T] with
        def asSqlGroupingItems(x: H *: T): List[SqlGroupingItem] =
            h.asSqlGroupingItems(x.head) ++ t.asSqlGroupingItems(x.tail)

    given tuple1[H](using
        h: AsMultidimensionalGrouping[H]
    ): AsMultidimensionalGrouping[H *: EmptyTuple] with
        def asSqlGroupingItems(x: H *: EmptyTuple): List[SqlGroupingItem] =
            h.asSqlGroupingItems(x.head)