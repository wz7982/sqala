package sqala.static.dsl.statement.query

import sqala.ast.group.SqlGroupingItem
import sqala.static.dsl.*
import sqala.util.NonEmptyList
import sqala.ir.IrGroupingItem
import scala.util.NotGiven
import scala.quoted.Type
import scala.quoted.Quotes

/**
  * Represents a single grouping item.
  */
trait AsGroupingItem[T]:
    /**
     * Converts the grouping item to a list of SQL grouping item.
     */
    def asIrGroupingItem(x: T): IrGroupingItem

object AsGroupingItem:
    given expr[T, CL <: Int](using
        a: AsExpr[T, CL],
        kt: KindToTuple[a.K],
        ig: IsGrouped[kt.R]
    ): AsGroupingItem[T] with
        def asIrGroupingItem(x: T): IrGroupingItem =
            IrGroupingItem.Expr(a.asExpr(x).asIrExpr)

    given unit: AsGroupingItem[Unit] with
        def asIrGroupingItem(x: Unit): IrGroupingItem =
            IrGroupingItem.EmptyGroup

    given cube: AsGroupingItem[Cube] with
        def asIrGroupingItem(x: Cube): IrGroupingItem =
            IrGroupingItem.Cube(x.exprs)

    given rollup: AsGroupingItem[Rollup] with
        def asIrGroupingItem(x: Rollup): IrGroupingItem =
            IrGroupingItem.Rollup(x.exprs)

    given groupingSets: AsGroupingItem[GroupingSets] with
        def asIrGroupingItem(x: GroupingSets): IrGroupingItem =
            IrGroupingItem.GroupingSets(x.items)

/**
 * Collects grouping items into a list of `SqlGroupingItem`.
 */
trait AsMultidimensionalGrouping[T]:
    /**
     * Converts the grouping items to a list of SQL grouping items.
     */
    def asIrGroupingItems(x: T): NonEmptyList[IrGroupingItem]

object AsMultidimensionalGrouping:
    given expr[T, CL <: Int](using
        a: AsExpr[T, CL],
        kt: KindToTuple[a.K],
        ig: IsGrouped[kt.R]
    ): AsMultidimensionalGrouping[T] with
        def asIrGroupingItems(x: T): NonEmptyList[IrGroupingItem] =
            NonEmptyList(IrGroupingItem.Expr(a.asExpr(x).asIrExpr), Nil)

    given unit: AsMultidimensionalGrouping[Unit] with
        def asIrGroupingItems(x: Unit): NonEmptyList[IrGroupingItem] =
            NonEmptyList(IrGroupingItem.EmptyGroup, Nil)

    given cube: AsMultidimensionalGrouping[Cube] with
        def asIrGroupingItems(x: Cube): NonEmptyList[IrGroupingItem] =
            NonEmptyList(IrGroupingItem.Cube(x.exprs), Nil)

    given rollup: AsMultidimensionalGrouping[Rollup] with
        def asIrGroupingItems(x: Rollup): NonEmptyList[IrGroupingItem] =
            NonEmptyList(IrGroupingItem.Rollup(x.exprs), Nil)

    given groupingSets: AsMultidimensionalGrouping[GroupingSets] with
        def asIrGroupingItems(x: GroupingSets): NonEmptyList[IrGroupingItem] =
            NonEmptyList(IrGroupingItem.GroupingSets(x.items), Nil)

    given tuple[H, T <: Tuple](using
        h: AsGroupingItem[H],
        t: AsMultidimensionalGrouping[T]
    ): AsMultidimensionalGrouping[H *: T] with
        def asIrGroupingItems(x: H *: T): NonEmptyList[IrGroupingItem] =
            NonEmptyList(h.asIrGroupingItem(x.head), t.asIrGroupingItems(x.tail))

    given tuple1[H](using
        h: AsMultidimensionalGrouping[H]
    ): AsMultidimensionalGrouping[H *: EmptyTuple] with
        def asIrGroupingItems(x: H *: EmptyTuple): NonEmptyList[IrGroupingItem] =
            h.asIrGroupingItems(x.head)