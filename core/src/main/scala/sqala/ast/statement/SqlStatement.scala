package sqala.ast.statement

import sqala.ast.expr.{SqlBinaryOperator, SqlExpr}
import sqala.ast.limit.SqlLimit
import sqala.ast.order.SqlOrderItem
import sqala.ast.table.SqlTable
import sqala.ast.group.SqlGroupItem
import sqala.ast.param.SqlParam

enum SqlStatement:
    case Delete(table: SqlTable, where: Option[SqlExpr])
    case Insert(table: SqlTable, columns: List[SqlExpr], values: List[List[SqlExpr]], query: Option[SqlQuery])
    case Update(table: SqlTable, setList: List[(SqlExpr, SqlExpr)], where: Option[SqlExpr])
    case Truncate(table: SqlTable)
    case Upsert(table: SqlTable, columns: List[SqlExpr], values: List[SqlExpr], pkList: List[SqlExpr], updateList: List[SqlExpr])

object SqlStatement:
    extension (delete: Delete)
        def addWhere(condition: SqlExpr): Delete =
            delete.copy(where = delete.where.map(SqlExpr.Binary(_, SqlBinaryOperator.And, condition)).orElse(Some(condition)))

    extension (update: Update)
        def addWhere(condition: SqlExpr): Update =
            update.copy(where = update.where.map(SqlExpr.Binary(_, SqlBinaryOperator.And, condition)).orElse(Some(condition)))

enum SqlQuery:
    case Select(
        param: Option[SqlParam] = None,
        select: List[SqlSelectItem],
        from: List[SqlTable],
        where: Option[SqlExpr] = None,
        groupBy: List[SqlGroupItem] = Nil,
        having: Option[SqlExpr] = None,
        orderBy: List[SqlOrderItem] = Nil,
        limit: Option[SqlLimit] = None
    )
    case Union(
        left: SqlQuery,
        unionType: SqlUnionType,
        right: SqlQuery,
        orderBy: List[SqlOrderItem] = Nil,
        limit: Option[SqlLimit] = None
    )
    case Values(values: List[List[SqlExpr]])
    case Cte(queryItems: List[SqlWithItem], recursive: Boolean, query: SqlQuery)

object SqlQuery:
    extension (select: Select)
        def addSelectItem(item: SqlSelectItem): Select =
            select.copy(select = select.select.appended(item))

        def addWhere(condition: SqlExpr): Select =
            select.copy(where = select.where.map(SqlExpr.Binary(_, SqlBinaryOperator.And, condition)).orElse(Some(condition)))

        def addHaving(condition: SqlExpr): Select =
            select.copy(having = select.having.map(SqlExpr.Binary(_, SqlBinaryOperator.And, condition)).orElse(Some(condition)))