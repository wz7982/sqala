package sqala.dsl.statement.query

import sqala.ast.expr.SqlExpr
import sqala.ast.statement.SqlSelectItem
import sqala.dsl.{Expr, ExprKind}

import scala.annotation.implicitNotFound
import sqala.dsl.ColumnKind
import sqala.dsl.ToTuple

@implicitNotFound("Type ${T} cannot be converted to SELECT items")
trait SelectItem[T]:
    type R

    def subQueryItems(item: T, cursor: Int, alias: String): R

    def offset(item: T): Int

    def selectItems(item: T, cursor: Int): List[SqlSelectItem.Item]

object SelectItem:
    transparent inline given exprSelectItem[T, K <: ExprKind]: SelectItem[Expr[T, K]] = new SelectItem[Expr[T, K]]:
        type R = Expr[T, ColumnKind]

        def subQueryItems(item: Expr[T, K], cursor: Int, alias: String): R =
            Expr.Column(alias, s"c${cursor}")

        def offset(item: Expr[T, K]): Int = 1

        def selectItems(item: Expr[T, K], cursor: Int): List[SqlSelectItem.Item] = 
            SqlSelectItem.Item(item.asSqlExpr, Some(s"c${cursor}")) :: Nil

    transparent inline given tupleSelectItem[H, T <: Tuple](using sh: SelectItem[H], st: SelectItem[T]): SelectItem[H *: T] = new SelectItem[H *: T]:
        type R = sh.R *: ToTuple[st.R]

        def subQueryItems(item: H *: T, cursor: Int, alias: String): R =
            val head = sh.subQueryItems(item.head, cursor, alias)
            val tail = st.subQueryItems(item.tail, cursor + sh.offset(item.head), alias) match
                case x: Tuple => x
            (head *: tail).asInstanceOf[R]

        def offset(item: H *: T): Int = sh.offset(item.head) + st.offset(item.tail)

        def selectItems(item: H *: T, cursor: Int): List[SqlSelectItem.Item] =
            sh.selectItems(item.head, cursor) ++ st.selectItems(item.tail, cursor + sh.offset(item.head))

    transparent inline given emptyTupleSelectItem: SelectItem[EmptyTuple] = new SelectItem[EmptyTuple]:
        type R = EmptyTuple

        def subQueryItems(item: EmptyTuple, cursor: Int, alias: String): R = EmptyTuple

        def offset(item: EmptyTuple): Int = 0

        def selectItems(item: EmptyTuple, cursor: Int): List[SqlSelectItem.Item] = Nil