package sqala.static.dsl.statement.query

import sqala.ast.group.SqlGroupingItem
import sqala.static.dsl.*
import sqala.util.NonEmptyList.toNonEmptyList

/**
  * Represents a single grouping item.
  */
trait AsGroupingItem[T]:
    /**
     * Converts the grouping item to a list of SQL grouping item.
     */
    def asSqlGroupingItem(x: T): SqlGroupingItem

object AsGroupingItem:
    given expr[T, CL <: Int](using
        a: AsExpr[T, CL],
        kt: KindToTuple[a.K],
        ak: AllIsKind[kt.R, Grouped[?]],
        refl: ak.R =:= true
    ): AsGroupingItem[T] with
        def asSqlGroupingItem(x: T): SqlGroupingItem =
            SqlGroupingItem.Expr(a.asExpr(x).asSqlExpr)

    given unit: AsGroupingItem[Unit] with
        def asSqlGroupingItem(x: Unit): SqlGroupingItem =
            SqlGroupingItem.EmptyGroup

    given cube: AsGroupingItem[Cube] with
        def asSqlGroupingItem(x: Cube): SqlGroupingItem =
            SqlGroupingItem.Cube(x.exprs.toNonEmptyList)

    given rollup: AsGroupingItem[Rollup] with
        def asSqlGroupingItem(x: Rollup): SqlGroupingItem =
            SqlGroupingItem.Rollup(x.exprs.toNonEmptyList)

    given groupingSets: AsGroupingItem[GroupingSets] with
        def asSqlGroupingItem(x: GroupingSets): SqlGroupingItem =
            SqlGroupingItem.GroupingSets(x.items.toNonEmptyList)


/**
 * Collects grouping items into a list of `SqlGroupingItem`.
 */
trait AsMultidimensionalGrouping[T]:
    /**
     * Converts the grouping items to a list of SQL grouping items.
     */
    def asSqlGroupingItems(x: T): List[SqlGroupingItem]

object AsMultidimensionalGrouping:
    given expr[T, CL <: Int](using
        a: AsExpr[T, CL],
        kt: KindToTuple[a.K],
        ak: AllIsKind[kt.R, Grouped[?]],
        refl: ak.R =:= true
    ): AsMultidimensionalGrouping[T] with
        def asSqlGroupingItems(x: T): List[SqlGroupingItem] =
            SqlGroupingItem.Expr(a.asExpr(x).asSqlExpr) :: Nil

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
        h: AsGroupingItem[H],
        t: AsMultidimensionalGrouping[T]
    ): AsMultidimensionalGrouping[H *: T] with
        def asSqlGroupingItems(x: H *: T): List[SqlGroupingItem] =
            h.asSqlGroupingItem(x.head) :: t.asSqlGroupingItems(x.tail)

    given tuple1[H](using
        h: AsMultidimensionalGrouping[H]
    ): AsMultidimensionalGrouping[H *: EmptyTuple] with
        def asSqlGroupingItems(x: H *: EmptyTuple): List[SqlGroupingItem] =
            h.asSqlGroupingItems(x.head)