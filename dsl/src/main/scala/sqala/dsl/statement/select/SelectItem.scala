package sqala.dsl.statement.select

import sqala.ast.statement.SqlSelectItem
import sqala.dsl.*

import scala.NamedTuple.NamedTuple

trait SelectItem[T]:
    type R

    def offset(item: T): Int

    def selectItems(item: T, cursor: Int): List[SqlSelectItem]

object SelectItem:
    type Aux[T, O] = SelectItem[T] { type R = O }

    given exprSelectItem[T, K <: ExprKind]: Aux[Expr[T, K], Expr[T, ColumnKind]] = new SelectItem[Expr[T, K]]:
        type R = Expr[T, ColumnKind]

        override def offset(item: Expr[T, K]): Int = 1

        override def selectItems(item: Expr[T, K], cursor: Int): List[SqlSelectItem] =
            SqlSelectItem(item.asSqlExpr, Some(s"c${cursor}")) :: Nil

    given tupleSelectItem[H, T <: Tuple](using sh: SelectItem[H], st: SelectItem[T]): Aux[H *: T, sh.R *: ToTuple[st.R]] = new SelectItem[H *: T]:
        type R = sh.R *: ToTuple[st.R]

        override def offset(item: H *: T): Int = sh.offset(item.head) + st.offset(item.tail)

        override def selectItems(item: H *: T, cursor: Int): List[SqlSelectItem] =
            sh.selectItems(item.head, cursor) ++ st.selectItems(item.tail, cursor + sh.offset(item.head))

    given emptyTupleSelectItem: Aux[EmptyTuple, EmptyTuple] = new SelectItem[EmptyTuple]:
        type R = EmptyTuple

        override def offset(item: EmptyTuple): Int = 0

        override def selectItems(item: EmptyTuple, cursor: Int): List[SqlSelectItem] = Nil

    given namedTupleSelectItem[N <: Tuple, V <: Tuple](using s: SelectItem[V]): Aux[NamedTuple[N, V], NamedTuple[N, ToTuple[s.R]]] = new SelectItem[NamedTuple[N, V]]:
        type R = NamedTuple[N, ToTuple[s.R]]

        override def offset(item: NamedTuple[N, V]): Int = s.offset(item.toTuple)

        override def selectItems(item: NamedTuple[N, V], cursor: Int): List[SqlSelectItem] =
            s.selectItems(item, cursor)