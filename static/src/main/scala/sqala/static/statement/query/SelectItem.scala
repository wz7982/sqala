package sqala.static.statement.query

import sqala.ast.expr.SqlExpr
import sqala.ast.statement.SqlSelectItem
import sqala.static.common.*

import scala.annotation.implicitNotFound

@implicitNotFound("Type ${T} cannot be converted to SELECT clause.")
trait SelectItem[T]:
    def selectItems(item: T, tableNames: List[String]): List[SqlSelectItem.Item]

object SelectItem:
    given table[T]: SelectItem[Table[T]] with
        def selectItems(item: Table[T], tableNames: List[String]): List[SqlSelectItem.Item] = 
            val metaData = item.__metaData__
            val columns = metaData.columnNames
            columns.map: c =>
                SqlSelectItem.Item(SqlExpr.Column(Some(tableNames.head), c), None)

    given subQuery[N <: Tuple, V <: Tuple]: SelectItem[SubQuery[N, V]] with
        def selectItems(item: SubQuery[N, V], tableNames: List[String]): List[SqlSelectItem.Item] = 
            val columns = item.__columns__
            columns.map: c =>
                SqlSelectItem.Item(SqlExpr.Column(Some(tableNames.head), c), None)

    given tableSubQuery[T]: SelectItem[TableSubQuery[T]] with
        def selectItems(item: TableSubQuery[T], tableNames: List[String]): List[SqlSelectItem.Item] = 
            val metaData = item.__metaData__
            val columns = metaData.columnNames
            columns.map: c =>
                SqlSelectItem.Item(SqlExpr.Column(Some(tableNames.head), c), None)

    given tuple[H, T <: Tuple](using h: SelectItem[H], t: SelectItem[T]): SelectItem[H *: T] with
        def selectItems(item: H *: T, tableNames: List[String]): List[SqlSelectItem.Item] =
            h.selectItems(item.head, tableNames.head :: Nil) ++ 
            t.selectItems(item.tail, tableNames.tail)

    given emptyTuple: SelectItem[EmptyTuple] with
        def selectItems(item: EmptyTuple, tableNames: List[String]): List[SqlSelectItem.Item] =
            Nil