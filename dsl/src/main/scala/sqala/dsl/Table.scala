package sqala.dsl

import sqala.ast.expr.SqlExpr
import sqala.ast.statement.SqlSelectItem

import scala.collection.mutable.ListBuffer

case class Table[T](
    private[sqala] __tableName__ : String, 
    private[sqala] __aliasName__ : String,
    private[sqala] __metaData__ : TableMetaData
) extends Selectable:
    def selectDynamic(name: String): Expr[?, ?] =
        val columnMap = __metaData__.fieldNames.zip(__metaData__.columnNames).toMap
        Expr.Column(__aliasName__, columnMap(name))
    
    private[sqala] def __offset__ : Int = __metaData__.fieldNames.size

    private[sqala] def __selectItems__(cursor: Int): List[SqlSelectItem.Item] =
        var tmpCursor = cursor
        val items = ListBuffer[SqlSelectItem.Item]()
        for field <- __metaData__.columnNames do
            items.addOne(SqlSelectItem.Item(SqlExpr.Column(Some(__aliasName__), field), Some(s"c${tmpCursor}")))
            tmpCursor += 1
        items.toList