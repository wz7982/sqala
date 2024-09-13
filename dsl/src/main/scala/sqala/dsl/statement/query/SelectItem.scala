package sqala.dsl.statement.query

import sqala.ast.expr.SqlExpr
import sqala.ast.statement.SqlSelectItem
import sqala.dsl.{Expr, ExprKind, Table}

import scala.NamedTuple.NamedTuple
import scala.collection.mutable.ListBuffer
import scala.annotation.implicitNotFound

@implicitNotFound("Type ${T} cannot be converted to SELECT items.")
trait SelectItem[T]:
    def offset(item: T): Int

    def selectItems(item: T, cursor: Int): List[SqlSelectItem.Item]

object SelectItem:
    given exprSelectItem[T, K <: ExprKind]: SelectItem[Expr[T, K]] with
        override def offset(item: Expr[T, K]): Int = 1

        override def selectItems(item: Expr[T, K], cursor: Int): List[SqlSelectItem.Item] = 
            SqlSelectItem.Item(item.asSqlExpr, Some(s"c${cursor}")) :: Nil

    given tableSelectItem[T]: SelectItem[Table[T]] with
        override def offset(item: Table[T]): Int = item.__metaData__.fieldNames.size

        override def selectItems(item: Table[T], cursor: Int): List[SqlSelectItem.Item] =
            var tmpCursor = cursor
            val items = ListBuffer[SqlSelectItem.Item]()
            for field <- item.__metaData__.columnNames do
                items.addOne(SqlSelectItem.Item(SqlExpr.Column(Some(item.__aliasName__), field), Some(s"c${tmpCursor}")))
                tmpCursor += 1
            items.toList

    given namedQuerySelectItem[N <: Tuple, V <: Tuple](using s: SelectItem[NamedTuple[N, V]]): SelectItem[NamedQuery[N, V]] with
        override def offset(item: NamedQuery[N, V]): Int = s.offset(item.__query__.queryItems)

        override def selectItems(item: NamedQuery[N, V], cursor: Int): List[SqlSelectItem.Item] =
            val queryItems = s.selectItems(item.__query__.queryItems, 0)
            var tmpCursor = cursor
            val items = ListBuffer[SqlSelectItem.Item]()
            for field <- queryItems.map(_.alias.get) do
                items.addOne(SqlSelectItem.Item(SqlExpr.Column(Some(item.__alias__), field), Some(s"c${tmpCursor}")))
                tmpCursor += 1
            items.toList

    given tupleSelectItem[H, T <: Tuple](using sh: SelectItem[H], st: SelectItem[T]): SelectItem[H *: T] with
        override def offset(item: H *: T): Int = sh.offset(item.head) + st.offset(item.tail)

        override def selectItems(item: H *: T, cursor: Int): List[SqlSelectItem.Item] =
            sh.selectItems(item.head, cursor) ++ st.selectItems(item.tail, cursor + sh.offset(item.head))

    given emptyTupleSelectItem: SelectItem[EmptyTuple] with
        override def offset(item: EmptyTuple): Int = 0

        override def selectItems(item: EmptyTuple, cursor: Int): List[SqlSelectItem.Item] = Nil

    given namedTupleSelectItem[N <: Tuple, V <: Tuple](using s: SelectItem[V]): SelectItem[NamedTuple[N, V]] with
        override def offset(item: NamedTuple[N, V]): Int = s.offset(item.toTuple)

        override def selectItems(item: NamedTuple[N, V], cursor: Int): List[SqlSelectItem.Item] =
            s.selectItems(item, cursor)