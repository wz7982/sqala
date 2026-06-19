package sqala.ast.statement

import sqala.ast.expr.{SqlBinaryOperator, SqlExpr}
import sqala.ast.group.SqlGroup
import sqala.ast.limit.SqlLimit
import sqala.ast.order.SqlOrderingItem
import sqala.ast.quantifier.SqlQuantifier
import sqala.ast.table.SqlTable
import sqala.util.NonEmptyList

/**
 * Insert data source mode.
 */
enum SqlInsertMode:
    /**
     * Insert literal values.
     *
     * Renders as `VALUES (expr [, ...]) [, ...]`.
     */
    case Values(values: NonEmptyList[NonEmptyList[SqlExpr]])

    /**
     * Insert from a subquery.
     *
     * Renders as `(query)`.
     */
    case Subquery(query: SqlQuery)

/**
 * A `SET` assignment pair in an `UPDATE` statement.
 *
 * Renders as `"column" = expr`.
 */
case class SqlUpdateSetPair(column: String, value: SqlExpr)

/**
 * Top-level DML statements.
 */
enum SqlStatement:
    /**
     * A `DELETE` statement.
     *
     * Renders as `DELETE FROM table [WHERE expr]`.
     */
    case Delete(table: SqlTable.Ident, where: Option[SqlExpr])

    /**
     * An `INSERT` statement.
     *
     * Renders as `INSERT INTO table [("column" [, ...])] VALUES (expr [, ...])|(query)`.
     */
    case Insert(table: SqlTable.Ident, columns: List[String], mode: SqlInsertMode)

    /**
     * An `UPDATE` statement.
     *
     * Renders as `UPDATE table SET "column" = expr [, ...] [WHERE expr]`.
     */
    case Update(table: SqlTable.Ident, setPairs: NonEmptyList[SqlUpdateSetPair], where: Option[SqlExpr])

    /**
     * A `TRUNCATE` statement.
     *
     * Renders as `TRUNCATE TABLE table`.
     */
    case Truncate(table: SqlTable.Ident)

    /**
     * An `UPSERT` statement.
     *
     * Renders as
     * `MERGE INTO ...`.
     */
    case Upsert(
        table: SqlTable.Ident,
        columns: NonEmptyList[String],
        values: NonEmptyList[SqlExpr],
        primaryKeys: NonEmptyList[String],
        updateColumns: NonEmptyList[String]
    )

object SqlStatement:
    extension (delete: Delete)
        /**
         * Returns a copy with the given condition added to the `WHERE` clause
         * via `AND`.
         */
        def addWhere(condition: SqlExpr): Delete =
            delete.copy(where = delete.where.map(SqlExpr.Binary(_, SqlBinaryOperator.And, condition)).orElse(Some(condition)))

    extension (update: Update)
        /**
         * Returns a copy with the given condition added to the `WHERE` clause
         * via `AND`.
         */
        def addWhere(condition: SqlExpr): Update =
            update.copy(where = update.where.map(SqlExpr.Binary(_, SqlBinaryOperator.And, condition)).orElse(Some(condition)))

/**
 * Query statement, optionally with a row-level lock.
 */
enum SqlQuery(val lock: Option[SqlLock]):
    /**
     * A `SELECT` query.
     *
     * Renders as
     * `SELECT [DISTINCT|ALL] select_item [, ...]
     *   FROM table [, ...]
     *   [WHERE expr]
     *   [GROUP BY [DISTINCT|ALL] grouping_item [, ...]]
     *   [HAVING expr]
     *   [ORDER BY ordering_item [, ...]]
     *   [OFFSET expr [ROW|ROWS]] [FETCH FIRST|NEXT expr [PERCENT] ROW|ROWS ONLY|WITH TIES]
     *   [FOR UPDATE|SHARE]`.
     */
    case Select(
        quantifier: Option[SqlQuantifier],
        select: List[SqlSelectItem],
        from: List[SqlTable],
        where: Option[SqlExpr],
        groupBy: Option[SqlGroup],
        having: Option[SqlExpr],
        orderBy: List[SqlOrderingItem],
        limit: Option[SqlLimit],
        override val lock: Option[SqlLock]
    ) extends SqlQuery(lock)

    /**
     * A set operation combining two queries.
     *
     * Renders as
     * `query UNION|EXCEPT|INTERSECT [DISTINCT|ALL] query
     *   [ORDER BY ordering [, ...]]
     *   [OFFSET expr [ROW|ROWS]] [FETCH FIRST|NEXT expr [PERCENT] ROW|ROWS ONLY|WITH TIES]
     *   [FOR UPDATE|SHARE]`.
     */
    case Set(
        left: SqlQuery,
        operator: SqlSetOperator,
        right: SqlQuery,
        orderBy: List[SqlOrderingItem],
        limit: Option[SqlLimit],
        override val lock: Option[SqlLock]
    ) extends SqlQuery(lock)

    /**
     * A `VALUES` clause used as a query.
     *
     * Renders as `VALUES (expr [, ...]) [, ...] [FOR UPDATE|SHARE]`.
     */
    case Values(
        values: NonEmptyList[NonEmptyList[SqlExpr]],
        override val lock: Option[SqlLock]
    ) extends SqlQuery(lock)

    /**
     * A common table expression (CTE) query.
     *
     * Renders as `WITH [RECURSIVE] with_item [, ...] query [FOR UPDATE|SHARE]`.
     */
    case With(
        withRecursive: Boolean,
        withItems: NonEmptyList[SqlWithItem],
        query: SqlQuery,
        override val lock: Option[SqlLock]
    ) extends SqlQuery(lock)

object SqlQuery:
    extension (select: Select)
        /**
         * Returns a copy with the given item appended to the select list.
         */
        def addSelectItem(item: SqlSelectItem): Select =
            select.copy(select = select.select.appended(item))

        /**
         * Returns a copy with the given condition added to the `WHERE` clause
         * via `AND`.
         */
        def addWhere(condition: SqlExpr): Select =
            select.copy(where = select.where.map(SqlExpr.Binary(_, SqlBinaryOperator.And, condition)).orElse(Some(condition)))

        /**
         * Returns a copy with the given condition added to the `HAVING` clause
         * via `AND`.
         */
        def addHaving(condition: SqlExpr): Select =
            select.copy(having = select.having.map(SqlExpr.Binary(_, SqlBinaryOperator.And, condition)).orElse(Some(condition)))