package sqala.ast.statement

import sqala.ast.expr.{SqlBinaryOperator, SqlExpr}
import sqala.ast.group.SqlGroupBy
import sqala.ast.limit.SqlLimit
import sqala.ast.order.SqlOrderingItem
import sqala.ast.quantifier.SqlQuantifier
import sqala.ast.table.SqlTable

case class SqlUpdateSetPair(column: String, value: SqlExpr)

enum SqlStatement:
    case Delete(table: SqlTable.Ident, where: Option[SqlExpr])
    case Insert(table: SqlTable.Ident, columns: List[String], values: List[List[SqlExpr]], query: Option[SqlQuery])
    case Update(table: SqlTable.Ident, setList: List[SqlUpdateSetPair], where: Option[SqlExpr])
    case Truncate(table: SqlTable.Ident)
    case Upsert(table: SqlTable.Ident, columns: List[String], values: List[SqlExpr], pkList: List[String], updateList: List[String])

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
        recursive: Boolean,
        queryItems: List[SqlWithItem],
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