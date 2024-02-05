package sqala.dsl.statement.query

import sqala.ast.expr.SqlExpr
import sqala.ast.statement.SqlSelectItem
import sqala.dsl.{Expr, Table}

import scala.collection.mutable.ListBuffer

trait SelectItem[T]:
    def offset(item: T): Int

    def selectItems(item: T, cursor: Int): List[SqlSelectItem]

object SelectItem:
    given exprSelectItem[T, E <: Expr[T]]: SelectItem[E] with
        override def offset(item: E): Int = 1

        override def selectItems(item: E, cursor: Int): List[SqlSelectItem] = 
            SqlSelectItem(item.asSqlExpr, Some(s"c${cursor}")) :: Nil

    given tableSelectItem[T]: SelectItem[Table[T]] with
        override def offset(item: Table[T]): Int = item.__metaData__.fieldNames.size

        override def selectItems(item: Table[T], cursor: Int): List[SqlSelectItem] =
            var tmpCursor = cursor
            val items = ListBuffer[SqlSelectItem]()
            for field <- item.__metaData__.columnNames do
                items.addOne(SqlSelectItem(SqlExpr.Column(Some(item.__aliasName__), field), Some(s"c${tmpCursor}")))
                tmpCursor += 1
            items.toList

    given namedQuerySelectItem[T](using s: SelectItem[T]): SelectItem[NamedQuery[T]] with
        override def offset(item: NamedQuery[T]): Int = s.offset(item.query.queryItems)

        override def selectItems(item: NamedQuery[T], cursor: Int): List[SqlSelectItem] =
            val queryItems = s.selectItems(item.query.queryItems, 0)
            var tmpCursor = cursor
            val items = ListBuffer[SqlSelectItem]()
            for field <- queryItems.map(_.alias.get) do
                items.addOne(SqlSelectItem(SqlExpr.Column(Some(item.alias), field), Some(s"c${tmpCursor}")))
                tmpCursor += 1
            items.toList

    given tupleSelectItem[H, T <: Tuple](using sh: SelectItem[H], st: SelectItem[T]): SelectItem[H *: T] with
        override def offset(item: H *: T): Int = sh.offset(item.head)

        override def selectItems(item: H *: T, cursor: Int): List[SqlSelectItem] =
            sh.selectItems(item.head, cursor) ++ st.selectItems(item.tail, cursor + offset(item))

    given emptyTupleSelectItem: SelectItem[EmptyTuple] with
        override def offset(item: EmptyTuple): Int = 0

        override def selectItems(item: EmptyTuple, cursor: Int): List[SqlSelectItem] = Nil