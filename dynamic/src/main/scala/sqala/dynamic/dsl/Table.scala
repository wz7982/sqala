package sqala.dynamic.dsl

import sqala.ast.statement.SqlQuery
import sqala.ast.table.{SqlJoinCondition, SqlJoinType, SqlTable, SqlTableAlias}

/**
 * Common base for everything usable as a `FROM` table source.
 */
sealed trait AnyTable:
    /**
     * Returns the underlying AST table node.
     *
     * {{{
     * table("user").ast
     * }}}
     */
    def ast: SqlTable =
        asSqlTable

    /**
     * Inner joins with another table. Follow with `on`. Maps to
     * `JOIN table ON condition`.
     *
     * {{{
     * table("user").join(table("post")).on(column("user", "id") === column("post", "user_id"))
     * }}}
     */
    def join(table: AnyTable): JoinTable =
        JoinTable(this, SqlJoinType.Inner, table, None)

    /**
     * Lateral inner join with a derived table, which may reference
     * columns from this table. Follow with `on`. Maps to
     * `JOIN LATERAL (query) ON condition`.
     *
     * {{{
     * table("user").joinLateral(table(select(column("post", "id").as("id"))).as("p"))
     * }}}
     */
    def joinLateral(table: SubqueryTable): JoinTable =
        JoinTable(this, SqlJoinType.Inner, SubqueryTable(table.query, table.alias, true), None)

    /**
     * Cross joins with another table. No `on` condition is required.
     * Maps to `CROSS JOIN table`.
     *
     * {{{
     * table("user").crossJoin(table("role"))
     * }}}
     */
    def crossJoin(table: AnyTable): JoinTable =
        JoinTable(this, SqlJoinType.Cross, table, None)

    /**
     * Lateral cross join with a derived table, which may reference
     * columns from this table. Maps to `CROSS JOIN LATERAL (query)`.
     *
     * {{{
     * table("user").crossJoinLateral(table(select(column("id").as("id"))).as("p"))
     * }}}
     */
    def crossJoinLateral(table: SubqueryTable): JoinTable =
        JoinTable(this, SqlJoinType.Cross, SubqueryTable(table.query, table.alias, true), None)

    /**
     * Left outer joins with another table. Follow with `on`. Maps to
     * `LEFT JOIN table ON condition`.
     *
     * {{{
     * table("user").leftJoin(table("post")).on(column("user", "id") === column("post", "user_id"))
     * }}}
     */
    def leftJoin(table: AnyTable): JoinTable =
        JoinTable(this, SqlJoinType.Left, table, None)

    /**
     * Lateral left outer join with a derived table, which may reference
     * columns from this table. Follow with `on`. Maps to
     * `LEFT JOIN LATERAL (query) ON condition`.
     *
     * {{{
     * table("user").leftJoinLateral(table(select(column("id").as("id"))).as("p"))
     * }}}
     */
    def leftJoinLateral(table: SubqueryTable): JoinTable =
        JoinTable(this, SqlJoinType.Left, SubqueryTable(table.query, table.alias, true), None)

    /**
     * Right outer joins with another table. Follow with `on`. Maps to
     * `RIGHT JOIN table ON condition`.
     *
     * {{{
     * table("user").rightJoin(table("post")).on(column("user", "id") === column("post", "user_id"))
     * }}}
     */
    def rightJoin(table: AnyTable): JoinTable =
        JoinTable(this, SqlJoinType.Right, table, None)

    /**
     * Lateral right outer join with a derived table, which may reference
     * columns from this table. Follow with `on`. Maps to
     * `RIGHT JOIN LATERAL (query) ON condition`.
     *
     * {{{
     * table("user").rightJoinLateral(table(select(column("id").as("id"))).as("p"))
     * }}}
     */
    def rightJoinLateral(table: SubqueryTable): JoinTable =
        JoinTable(this, SqlJoinType.Right, SubqueryTable(table.query, table.alias, true), None)

    /**
     * Full outer joins with another table. Follow with `on`. Maps to
     * `FULL JOIN table ON condition`.
     *
     * {{{
     * table("user").fullJoin(table("post")).on(column("user", "id") === column("post", "user_id"))
     * }}}
     */
    def fullJoin(table: AnyTable): JoinTable =
        JoinTable(this, SqlJoinType.Full, table, None)

    /**
     * Lateral full outer join with a derived table, which may reference
     * columns from this table. Follow with `on`. Maps to
     * `FULL JOIN LATERAL (query) ON condition`.
     *
     * {{{
     * table("user").fullJoinLateral(table(select(column("id").as("id"))).as("p"))
     * }}}
     */
    def fullJoinLateral(table: SubqueryTable): JoinTable =
        JoinTable(this, SqlJoinType.Full, SubqueryTable(table.query, table.alias, true), None)

    /**
     * Converts this table source into its AST node:
     */
    private[sqala] def asSqlTable: SqlTable =
        this match
            case t: Table =>
                SqlTable.Ident(t.name, t.alias.map(a => SqlTableAlias(a, Nil)), None, None, None)
            case t: SubqueryTable =>
                SqlTable.Subquery(t.lateral, t.query, t.alias.map(a => SqlTableAlias(a, Nil)), None)
            case t: JoinTable =>
                SqlTable.Join(t.left.asSqlTable, t.joinType, t.right.asSqlTable, t.condition.map(c => SqlJoinCondition.On(c.asSqlExpr)))

/**
 * A named table source. Create it with `table(name)` and optionally
 * alias it with `as`.
 *
 * {{{
 * table("user").as("u")
 * }}}
 */
final class Table(
    private[sqala] val name: String,
    private[sqala] val alias: Option[String]
) extends AnyTable:
    /**
     * Assigns an alias to the table. Maps to `table AS "alias"`.
     *
     * {{{
     * table("user").as("u")
     * }}}
     */
    def as(alias: String): Table =
        Table(name, Some(alias))

/**
 * A join between two table sources. Created by the join methods on
 * AnyTable and completed with `on` (cross joins may skip it).
 */
final class JoinTable(
    private[sqala] val left: AnyTable,
    private[sqala] val joinType: SqlJoinType,
    private[sqala] val right: AnyTable,
    private[sqala] val condition: Option[Expr]
) extends AnyTable:
    /**
     * Sets the join condition. Maps to `... ON expr`.
     *
     * {{{
     * table("user").join(table("post")).on(column("user", "id") === column("post", "user_id"))
     * }}}
     */
    def on(expr: Expr): JoinTable =
        JoinTable(left, joinType, right, Some(expr))

/**
 * A derived (subquery) table source. Create it with `table(query)` and
 * optionally alias it with `as`. Lateral joins wrap the derived table
 * with the `LATERAL` keyword automatically.
 *
 * {{{
 * table(select(column("id").as("id"))).as("sub")
 * }}}
 */
final class SubqueryTable(
    private[sqala] val query: SqlQuery,
    private[sqala] val alias: Option[String],
    private[sqala] val lateral: Boolean
) extends AnyTable:
    /**
     * Assigns an alias to the derived table. Maps to
     * `(query) AS "alias"`.
     *
     * {{{
     * table(select(column("id").as("id"))).as("sub")
     * }}}
     */
    def as(alias: String): SubqueryTable =
        SubqueryTable(query, Some(alias), lateral)