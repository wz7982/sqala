package sqala.ast.statement

import sqala.ast.expr.{SqlBinaryOperator, SqlExpr}
import sqala.ast.group.SqlGroupBy
import sqala.ast.limit.SqlLimit
import sqala.ast.order.SqlOrderingItem
import sqala.ast.quantifier.SqlQuantifier
import sqala.ast.table.SqlTable

/**
 * Insert data source mode.
 */
enum SqlInsertMode:
    /**
     * Insert literal values.
     *
     * Renders as `VALUES (expr, ...), ...`.
     *
     * @param values the list of row value lists.
     */
    case Values(values: List[List[SqlExpr]])

    /**
     * Insert from a subquery.
     *
     * Renders as `(SELECT ...)`.
     *
     * @param query the source query.
     */
    case Subquery(query: SqlQuery)

/**
 * A `SET` assignment pair in an `UPDATE` statement.
 *
 * Renders as `column = value`.
 *
 * @param column the column name.
 * @param value the value expression.
 */
case class SqlUpdateSetPair(column: String, value: SqlExpr)

/**
 * Top-level DML statements.
 */
enum SqlStatement:
    /**
     * A `DELETE` statement.
     *
     * Renders as `DELETE FROM table [WHERE where]`.
     *
     * @param table the target table.
     * @param where optional filter condition.
     */
    case Delete(table: SqlTable.Ident, where: Option[SqlExpr])

    /**
     * An `INSERT` statement.
     *
     * Renders as `INSERT INTO table [(column, ...)] VALUES (...)|(SELECT ...)`.
     *
     * @param table the target table.
     * @param columns optional column list.
     * @param mode the insert mode (VALUES or subquery).
     */
    case Insert(table: SqlTable.Ident, columns: List[String], mode: SqlInsertMode)

    /**
     * An `UPDATE` statement.
     *
     * Renders as `UPDATE table SET column = value, ... [WHERE where]`.
     *
     * @param table the target table.
     * @param setList the SET assignments.
     * @param where optional filter condition.
     */
    case Update(table: SqlTable.Ident, setList: List[SqlUpdateSetPair], where: Option[SqlExpr])

    /**
     * A `TRUNCATE` statement.
     *
     * Renders as `TRUNCATE TABLE table`.
     *
     * @param table the target table.
     */
    case Truncate(table: SqlTable.Ident)

    /**
     * An `UPSERT` statement.
     *
     * Renders as
     * `MERGE INTO ...`.
     *
     * @param table the target table.
     * @param columns the column list.
     * @param values the value expressions.
     * @param pkList the primary key columns.
     * @param updateList the columns to update on duplicate.
     */
    case Upsert(table: SqlTable.Ident, columns: List[String], values: List[SqlExpr], pkList: List[String], updateList: List[String])

object SqlStatement:
    extension (delete: Delete)
        def addWhere(condition: SqlExpr): Delete =
            delete.copy(where = delete.where.map(SqlExpr.Binary(_, SqlBinaryOperator.And, condition)).orElse(Some(condition)))

    extension (update: Update)
        def addWhere(condition: SqlExpr): Update =
            update.copy(where = update.where.map(SqlExpr.Binary(_, SqlBinaryOperator.And, condition)).orElse(Some(condition)))

/**
 * Query statement, optionally with a row-level lock.
 *
 * @param lock optional `FOR UPDATE|SHARE` lock clause.
 */
enum SqlQuery(val lock: Option[SqlLock]):
    /**
     * A `SELECT` query.
     *
     * Renders as
     * `SELECT [DISTINCT|ALL] select, ...
     *   FROM table, ...
     *   [WHERE where]
     *   [GROUP BY group, ...]
     *   [HAVING having]
     *   [ORDER BY order, ...]
     *   [OFFSET n [ROW|ROWS]] [FETCH FIRST|NEXT n [PERCENT] ROW|ROWS ONLY|WITH TIES]
     *   [FOR UPDATE|SHARE]`.
     *
     * @param quantifier optional `DISTINCT` or `ALL`.
     * @param select the select list items.
     * @param from the table sources.
     * @param where optional filter condition.
     * @param groupBy optional `GROUP BY` clause.
     * @param having optional `HAVING` filter.
     * @param orderBy the `ORDER BY` items.
     * @param limit optional `LIMIT`/`OFFSET` clause.
     * @param lock optional row-level lock clause.
     */
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

    /**
     * A set operation combining two queries.
     *
     * Renders as
     * `left UNION|EXCEPT|INTERSECT [DISTINCT|ALL] right [ORDER BY order, ...] [LIMIT ...] [FOR UPDATE|SHARE]`.
     *
     * @param left the left query.
     * @param operator the set operator.
     * @param right the right query.
     * @param orderBy the `ORDER BY` items.
     * @param limit optional `LIMIT`/`OFFSET` clause.
     * @param lock optional row-level lock clause.
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
     * Renders as `VALUES (expr, ...) [, (expr, ...)] [FOR UPDATE|SHARE]`.
     *
     * @param values the list of row value lists.
     * @param lock optional row-level lock clause.
     */
    case Values(
        values: List[List[SqlExpr]],
        override val lock: Option[SqlLock]
    ) extends SqlQuery(lock)

    /**
     * A common table expression (CTE) query.
     *
     * Renders as `WITH [RECURSIVE] cte, ... query [FOR UPDATE|SHARE]`.
     *
     * @param recursive when `true`, adds `RECURSIVE` keyword.
     * @param queryItems the CTE definitions.
     * @param query the main query.
     * @param lock optional row-level lock clause.
     */
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