package sqala.dsl.statement.query

import sqala.ast.expr.SqlExpr
import sqala.ast.statement.SqlSelectItem
import sqala.dsl.*

import scala.annotation.implicitNotFound
import scala.collection.mutable.ListBuffer
import sqala.ast.statement.SqlSelectItem.Item

@implicitNotFound("Type ${T} cannot be converted to SELECT items.")
trait SelectItem[T]:
    def offset(item: T): Int

    def selectItems(item: T, cursor: Int): List[SqlSelectItem.Item]

object SelectItem:
    given valueSelectItem[T: AsSqlExpr]: SelectItem[T] with
        def offset(item: T): Int = 1

        def selectItems(item: T, cursor: Int): List[Item] =
            SqlSelectItem.Item(summon[AsSqlExpr[T]].asSqlExpr(item), Some(s"c${cursor}")) :: Nil

    given exprSelectItem[T]: SelectItem[Expr[T]] with
        def offset(item: Expr[T]): Int = 1

        def selectItems(item: Expr[T], cursor: Int): List[SqlSelectItem.Item] =
            SqlSelectItem.Item(item.asSqlExpr, Some(s"c${cursor}")) :: Nil

    given tableSelectItem[T]: SelectItem[Table[T]] with
        override def offset(item: Table[T]): Int = item.__metaData__.fieldNames.size

        override def selectItems(item: Table[T], cursor: Int): List[SqlSelectItem.Item] =
            var tmpCursor = cursor
            val items = ListBuffer[SqlSelectItem.Item]()
            for field <- item.__metaData__.columnNames do
                items.addOne(
                    SqlSelectItem.Item(SqlExpr.Column(Some(item.__aliasName__), field), Some(s"c${tmpCursor}"))
                )
                tmpCursor += 1
            items.toList

    given subQuerySelectItem[N <: Tuple, V <: Tuple]: SelectItem[SubQuery[N, V]] with
        override def offset(item: SubQuery[N, V]): Int = item.__columnSize__

        override def selectItems(item: SubQuery[N, V], cursor: Int): List[SqlSelectItem.Item] =
            var tmpCursor = cursor
            val items = ListBuffer[SqlSelectItem.Item]()
            for index <- (0 until offset(item)) do
                items.addOne(
                    SqlSelectItem.Item(SqlExpr.Column(Some(item.__alias__), s"c${index}"), Some(s"c${tmpCursor}"))
                )
                tmpCursor += 1
            items.toList

    given tupleSelectItem[H, T <: Tuple](using sh: SelectItem[H], st: SelectItem[T]): SelectItem[H *: T] with
        def offset(item: H *: T): Int = sh.offset(item.head) + st.offset(item.tail)

        def selectItems(item: H *: T, cursor: Int): List[SqlSelectItem.Item] =
            sh.selectItems(item.head, cursor) ++ st.selectItems(item.tail, cursor + sh.offset(item.head))

    given tuple1SelectItem[H](using sh: SelectItem[H]): SelectItem[H *: EmptyTuple] with
        def offset(item: H *: EmptyTuple): Int = sh.offset(item.head)

        def selectItems(item: H *: EmptyTuple, cursor: Int): List[SqlSelectItem.Item] =
            sh.selectItems(item.head, cursor)

@implicitNotFound("Type ${T} cannot be converted to SELECT items.")
trait SelectItemAsExpr[T]:
    type R <: Tuple

    def asExpr(x: T): R

object SelectItemAsExpr:
    type Aux[T, O <: Tuple] = SelectItemAsExpr[T]:
        type R = O

    given valueAsExpr[T: AsSqlExpr]: Aux[T, Expr[T] *: EmptyTuple] =
        new SelectItemAsExpr[T]:
            type R = Expr[T] *: EmptyTuple

            def asExpr(x: T): R = Expr.Literal(x, summon[AsSqlExpr[T]]) *: EmptyTuple

    given exprAsExpr[T: AsSqlExpr]: Aux[Expr[T], Expr[T] *: EmptyTuple] =
        new SelectItemAsExpr[Expr[T]]:
            type R = Expr[T] *: EmptyTuple

            def asExpr(x: Expr[T]): R = x *: EmptyTuple

    given tupleAsExpr[H, T <: Tuple](using
        h: SelectItemAsExpr[H],
        t: SelectItemAsExpr[T]
    ): Aux[H *: T, Tuple.Concat[h.R, t.R]] =
        new SelectItemAsExpr[H *: T]:
            type R = Tuple.Concat[h.R, t.R]

            def asExpr(x: H *: T): R = h.asExpr(x.head) ++ t.asExpr(x.tail)

    given tuple1AsExpr[H](using
        h: SelectItemAsExpr[H]
    ): Aux[H *: EmptyTuple, h.R] =
        new SelectItemAsExpr[H *: EmptyTuple]:
            type R = h.R

            def asExpr(x: H *: EmptyTuple): R = h.asExpr(x.head)