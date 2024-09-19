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

    transparent inline given tupleSelectItem[X, K <: ExprKind, T <: Tuple](using sh: SelectItem[Expr[X, K]], st: SelectItem[T]): SelectItem[Expr[X, K] *: T] = new SelectItem[Expr[X, K] *: T]:
        type R = sh.R *: ToTuple[st.R]

        def subQueryItems(item: Expr[X, K] *: T, cursor: Int, alias: String): R =
            val head = sh.subQueryItems(item.head, cursor, alias)
            val tail = st.subQueryItems(item.tail, cursor + sh.offset(item.head), alias) match
                case x: Tuple => x
            (head *: tail).asInstanceOf[R]

        def offset(item: Expr[X, K] *: T): Int = sh.offset(item.head) + st.offset(item.tail)

        def selectItems(item: Expr[X, K] *: T, cursor: Int): List[SqlSelectItem.Item] =
            sh.selectItems(item.head, cursor) ++ st.selectItems(item.tail, cursor + sh.offset(item.head))

    transparent inline given tuple1SelectItem[X, K <: ExprKind](using sh: SelectItem[Expr[X, K]]): SelectItem[Expr[X, K] *: EmptyTuple] = new SelectItem[Expr[X, K] *: EmptyTuple]:
        type R = sh.R *: EmptyTuple

        def subQueryItems(item: Expr[X, K] *: EmptyTuple, cursor: Int, alias: String): R =
            val head = sh.subQueryItems(item.head, cursor, alias)
            head *: EmptyTuple

        def offset(item: Expr[X, K] *: EmptyTuple): Int = sh.offset(item.head)

        def selectItems(item: Expr[X, K] *: EmptyTuple, cursor: Int): List[SqlSelectItem.Item] =
            sh.selectItems(item.head, cursor)