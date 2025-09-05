package sqala.ast.statement

import sqala.ast.expr.{SqlBinaryOperator, SqlExpr}
import sqala.ast.limit.SqlLimit
import sqala.ast.order.SqlOrderingItem
import sqala.ast.quantifier.SqlQuantifier
import sqala.ast.table.{SqlJoinCondition, SqlTable}
import sqala.ast.group.SqlGroupBy

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

enum SqlQuery(val lock: Option[SqlLock]):
    case Select(
        quantifier: Option[SqlQuantifier],
        select: List[SqlSelectItem],
        from: List[SqlTable],
        where: Option[SqlExpr],
        groupBy: Option[SqlGroupBy],
        having: Option[SqlExpr],
        orderBy: List[SqlOrderingItem],
        limit: Option[SqlLimit],
        override val lock: Option[SqlLock]
    ) extends SqlQuery(lock)
    case Set(
        left: SqlQuery,
        operator: SqlSetOperator,
        right: SqlQuery,
        orderBy: List[SqlOrderingItem],
        limit: Option[SqlLimit],
        override val lock: Option[SqlLock]
    ) extends SqlQuery(lock)
    case Values(
        values: List[List[SqlExpr]], 
        override val lock: Option[SqlLock]
    ) extends SqlQuery(lock)
    case Cte(
        queryItems: List[SqlWithItem], 
        recursive: Boolean, 
        query: SqlQuery, 
        override val lock: Option[SqlLock]
    ) extends SqlQuery(lock)

object SqlQuery:
    extension (select: Select)
        def addSelectItem(item: SqlSelectItem): Select =
            select.copy(select = select.select.appended(item))

        def addWhere(condition: SqlExpr): Select =
            select.copy(where = select.where.map(SqlExpr.Binary(_, SqlBinaryOperator.And, condition)).orElse(Some(condition)))

        def addHaving(condition: SqlExpr): Select =
            select.copy(having = select.having.map(SqlExpr.Binary(_, SqlBinaryOperator.And, condition)).orElse(Some(condition)))

        def setJoinOnCondition(condition: SqlExpr): Select =
            select.from.last match
                case SqlTable.Join(left, joinType, right, _, alias) => 
                    val newTable = SqlTable.Join(left, joinType, right, Some(SqlJoinCondition.On(condition)), alias)
                    select.copy(from = select.from.init :+ newTable)
                case _ => select