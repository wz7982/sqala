package sqala.dynamic.dsl

import sqala.ast.group.{SqlGroup, SqlGroupingItem}
import sqala.ast.limit.{SqlFetch, SqlFetchMode, SqlFetchUnit, SqlLimit}
import sqala.ast.quantifier.SqlQuantifier
import sqala.ast.statement.{SqlQuery, SqlSetOperator}
import sqala.dialect.Dialect
import sqala.util.{NonEmptyList, queryToString}

/**
 * A dynamic SQL query. Every builder method returns a new immutable
 * query, so a `Query` can be safely reused and branched.
 *
 * Instances are created with the `select` helper and refined with the
 * methods on SelectQuery / SetQuery.
 *
 * {{{
 * select(column("id").as("id"))
 *     .from(table("user"))
 *     .where(column("age") > value(18))
 *     .sql(PostgresqlDialect, true)
 * }}}
 */
sealed class Query(private[sqala] val tree: SqlQuery):
    /**
     * Returns the underlying SQL query AST.
     *
     * {{{
     * select(column("id").as("id")).ast
     * }}}
     */
    def ast: SqlQuery =
        tree

    /**
     * Renders the query to a SQL string for the given dialect.
     *
     * `standardEscapeStrings` controls string literal escaping: `true`
     * treats backslashes literally (standard behavior, e.g. PostgreSQL,
     * Oracle); `false` uses backslashes as escape characters (e.g. MySQL).
     *
     * {{{
     * q.sql(PostgresqlDialect, true)
     * }}}
     */
    def sql(dialect: Dialect, standardEscapeStrings: Boolean): String =
        queryToString(tree, dialect, standardEscapeStrings)

    /**
     * Combines with another query using `UNION`, removing duplicates.
     * Maps to `query UNION query`.
     *
     * {{{
     * select(...).union(select(...))
     * }}}
     */
    def union(that: Query): SetQuery =
        SetQuery(SqlQuery.Set(tree, SqlSetOperator.Union(None), None, that.tree, Nil, None))

    /**
     * Combines with another query using `UNION ALL`, keeping duplicates.
     * Maps to `query UNION ALL query`.
     *
     * {{{
     * select(...).unionAll(select(...))
     * }}}
     */
    def unionAll(that: Query): SetQuery =
        SetQuery(SqlQuery.Set(tree, SqlSetOperator.Union(Some(SqlQuantifier.All)), None, that.tree, Nil, None))

    /**
     * Removes rows of this query that are also in `that`. Maps to
     * `query EXCEPT query`.
     *
     * {{{
     * select(...).except(select(...))
     * }}}
     */
    def except(that: Query): SetQuery =
        SetQuery(SqlQuery.Set(tree, SqlSetOperator.Except(None), None, that.tree, Nil, None))

    /**
     * Removes rows of this query that are also in `that`, keeping
     * duplicates. Maps to `query EXCEPT ALL query`.
     *
     * {{{
     * select(...).exceptAll(select(...))
     * }}}
     */
    def exceptAll(that: Query): SetQuery =
        SetQuery(SqlQuery.Set(tree, SqlSetOperator.Except(Some(SqlQuantifier.All)), None, that.tree, Nil, None))

    /**
     * Returns rows present in both queries, removing duplicates. Maps to
     * `query INTERSECT query`.
     *
     * {{{
     * select(...).intersect(select(...))
     * }}}
     */
    def intersect(that: Query): SetQuery =
        SetQuery(SqlQuery.Set(tree, SqlSetOperator.Intersect(None), None, that.tree, Nil, None))

    /**
     * Returns rows present in both queries, keeping duplicates. Maps to
     * `query INTERSECT ALL query`.
     *
     * {{{
     * select(...).intersectAll(select(...))
     * }}}
     */
    def intersectAll(that: Query): SetQuery =
        SetQuery(SqlQuery.Set(tree, SqlSetOperator.Intersect(Some(SqlQuantifier.All)), None, that.tree, Nil, None))

    /**
     * Limits the number of returned rows. Maps to
     * `OFFSET ... FETCH FIRST n ROWS ONLY` (rendered per dialect).
     *
     * Only `SELECT` and set-operation queries (including those wrapped
     * in a `WITH` clause) are affected; other query shapes are returned
     * unchanged.
     *
     * {{{
     * select(...).limit(10)
     * }}}
     */
    def limit(n: Int): Query =
        val limit = tree match
            case s: SqlQuery.Select => s.limit
            case s: SqlQuery.Set => s.limit
            case SqlQuery.With(_, _, s: SqlQuery.Select) => s.limit
            case SqlQuery.With(_, _, s: SqlQuery.Set) => s.limit
            case _ => None
        val sqlLimit = limit
            .map(l => SqlLimit(l.offset, Some(SqlFetch(value(n).asSqlExpr, SqlFetchUnit.RowCount, SqlFetchMode.Only))))
            .orElse(Some(SqlLimit(None, Some(SqlFetch(value(n).asSqlExpr, SqlFetchUnit.RowCount, SqlFetchMode.Only)))))
        val newTree = tree match
            case s: SqlQuery.Select => s.copy(limit = sqlLimit)
            case s: SqlQuery.Set => s.copy(limit = sqlLimit)
            case SqlQuery.With(w, r, s: SqlQuery.Select) =>
                SqlQuery.With(w, r, s.copy(limit = sqlLimit))
            case SqlQuery.With(w, r, s: SqlQuery.Set) =>
                SqlQuery.With(w, r, s.copy(limit = sqlLimit))
            case _ => tree
        Query(newTree)

    /**
     * Skips the first `n` rows. Maps to `OFFSET n ROWS` (rendered per
     * dialect) and is usually combined with `limit`.
     *
     * Only `SELECT` and set-operation queries (including those wrapped
     * in a `WITH` clause) are affected; other query shapes are returned
     * unchanged.
     *
     * {{{
     * select(...).offset(20).limit(10)
     * }}}
     */
    def offset(n: Int): Query =
        val sqlExpr = value(n).asSqlExpr
        val limit = tree match
            case s: SqlQuery.Select => s.limit
            case s: SqlQuery.Set => s.limit
            case SqlQuery.With(_, _, s: SqlQuery.Select) => s.limit
            case SqlQuery.With(_, _, s: SqlQuery.Set) => s.limit
            case _ => None
        val sqlLimit = limit
            .map(l => SqlLimit(Some(sqlExpr), l.fetch))
            .orElse(Some(SqlLimit(Some(sqlExpr), None)))
        val newTree = tree match
            case s: SqlQuery.Select => s.copy(limit = sqlLimit)
            case s: SqlQuery.Set => s.copy(limit = sqlLimit)
            case SqlQuery.With(w, r, s: SqlQuery.Select) =>
                SqlQuery.With(w, r, s.copy(limit = sqlLimit))
            case SqlQuery.With(w, r, s: SqlQuery.Set) =>
                SqlQuery.With(w, r, s.copy(limit = sqlLimit))
            case _ => tree
        Query(newTree)

/**
 * A dynamic `SELECT` query builder.
 *
 * All builder methods return a new `SelectQuery` and can be chained in
 * any order, though the conventional order is
 * `select -> from -> where -> groupBy -> having -> orderBy -> limit`.
 */
final class SelectQuery(override private[sqala] val tree: SqlQuery.Select) extends Query(tree):
    /**
     * Appends select items to the projection.
     *
     * {{{
     * select(column("id").as("id")).select(column("name").as("name"))
     * }}}
     */
    def select(items: SelectItem*): SelectQuery =
        SelectQuery(tree.copy(select = tree.select ++ items.toList.map(_.item)))

    /**
     * List-based overload of `select`.
     *
     * {{{
     * select(column("id").as("id")).select(List(column("name").as("name")))
     * }}}
     */
    def select(items: List[SelectItem]): SelectQuery =
        select(items*)

    /**
     * Appends table sources to the `FROM` clause.
     *
     * {{{
     * select(column("id").as("id")).from(table("user"))
     * }}}
     */
    def from(tables: AnyTable*): SelectQuery =
        SelectQuery(tree.copy(from = tree.from ++ tables.toList.map(_.asSqlTable)))

    /**
     * List-based overload of `from`.
     *
     * {{{
     * select(column("id").as("id")).from(List(table("user")))
     * }}}
     */
    def from(tables: List[AnyTable]): SelectQuery =
        from(tables*)

    /**
     * Adds a condition to the `WHERE` clause. When a `WHERE` clause
     * already exists, the new condition is combined with it using `AND`.
     *
     * {{{
     * select(column("id").as("id")).from(table("user")).where(column("age") > value(18))
     * }}}
     */
    def where(expr: Expr): SelectQuery =
        SelectQuery(tree.addWhere(expr.asSqlExpr))

    /**
     * Appends items to the `ORDER BY` clause.
     *
     * {{{
     * select(column("id").as("id")).from(table("user")).orderBy(column("id").desc)
     * }}}
     */
    def orderBy(orders: Order*): SelectQuery =
        SelectQuery(tree.copy(orderBy = tree.orderBy ++ orders.toList.map(_.order)))

    /**
     * List-based overload of `orderBy`.
     *
     * {{{
     * select(column("id").as("id")).from(table("user")).orderBy(List(column("id").desc))
     * }}}
     */
    def orderBy(orders: List[Order]): SelectQuery =
        orderBy(orders*)

    /**
     * Appends expressions to the `GROUP BY` clause. Calling it with an
     * empty list is a no-op; repeated calls accumulate grouping items.
     *
     * {{{
     * select(column("author").as("author"), count().as("total"))
     *     .from(table("post"))
     *     .groupBy(column("author"))
     * }}}
     */
    def groupBy(exprs: Expr*): SelectQuery =
        exprs.toList.map(i => SqlGroupingItem.Expr(i.asSqlExpr)) match
            case Nil =>
                this
            case newItems @ (head :: tail) =>
                val items = tree.groupBy match
                    case Some(group) => group.items ++ newItems
                    case None => NonEmptyList(head, tail)
                SelectQuery(
                    tree.copy(
                        groupBy = Some(
                            SqlGroup(
                                tree.groupBy.flatMap(_.quantifier),
                                items
                            )
                        )
                    )
                )

    /**
     * List-based overload of `groupBy`.
     *
     * {{{
     * select(column("author").as("author"), count().as("total"))
     *     .from(table("post"))
     *     .groupBy(List(column("author")))
     * }}}
     */
    def groupBy(exprs: List[Expr]): SelectQuery =
        groupBy(exprs*)

    /**
     * Adds a condition to the `HAVING` clause. When a `HAVING` clause
     * already exists, the new condition is combined with it using `AND`.
     *
     * {{{
     * select(column("author").as("author"), count().as("total"))
     *     .from(table("post"))
     *     .groupBy(column("author"))
     *     .having(count() > value(10))
     * }}}
     */
    def having(expr: Expr): SelectQuery =
        SelectQuery(tree.addHaving(expr.asSqlExpr))

    /**
     * Marks the query as `SELECT DISTINCT`.
     *
     * {{{
     * select(column("author").as("author")).from(table("post")).distinct
     * }}}
     */
    def distinct: SelectQuery =
        SelectQuery(tree.copy(quantifier = Some(SqlQuantifier.Distinct)))

/**
 * A dynamic set-operation query (`UNION`, `EXCEPT`, `INTERSECT`).
 *
 * When merging queries from different dialects, note that `ORDER BY` on
 * a set operation refers to the output columns of the combined result.
 */
final class SetQuery(override private[sqala] val tree: SqlQuery.Set) extends Query(tree):
    /**
     * Appends items to the `ORDER BY` clause of the set operation.
     *
     * {{{
     * select(...).union(select(...)).orderBy(column("id").asc)
     * }}}
     */
    def orderBy(orders: Order*): SetQuery =
        SetQuery(tree.copy(orderBy = tree.orderBy ++ orders.toList.map(_.order)))

    /**
     * List-based overload of `orderBy`.
     *
     * {{{
     * select(...).union(select(...)).orderBy(List(column("id").asc))
     * }}}
     */
    def orderBy(orders: List[Order]): SetQuery =
        orderBy(orders*)