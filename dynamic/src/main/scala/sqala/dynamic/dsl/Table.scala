package sqala.dynamic.dsl

import sqala.ast.statement.SqlQuery
import sqala.ast.table.{SqlJoinCondition, SqlJoinType, SqlTable, SqlTableAlias}

sealed trait AnyTable:
    def ast: SqlTable =
        asSqlTable

    def join(table: AnyTable): JoinTable =
        JoinTable(this, SqlJoinType.Inner, table, None)

    def joinLateral(table: SubqueryTable): JoinTable =
        JoinTable(this, SqlJoinType.Inner, table.copy(lateral = true), None)

    def crossJoin(table: AnyTable): JoinTable =
        JoinTable(this, SqlJoinType.Cross, table, None)

    def crossJoinLateral(table: SubqueryTable): JoinTable =
        JoinTable(this, SqlJoinType.Cross, table.copy(lateral = true), None)

    def leftJoin(table: AnyTable): JoinTable =
        JoinTable(this, SqlJoinType.Left, table, None)

    def leftJoinLateral(table: SubqueryTable): JoinTable =
        JoinTable(this, SqlJoinType.Left, table.copy(lateral = true), None)

    def rightJoin(table: AnyTable): JoinTable =
        JoinTable(this, SqlJoinType.Right, table, None)

    def rightJoinLateral(table: SubqueryTable): JoinTable =
        JoinTable(this, SqlJoinType.Right, table.copy(lateral = true), None)

    def fullJoin(table: AnyTable): JoinTable =
        JoinTable(this, SqlJoinType.Full, table, None)

    def fullJoinLateral(table: SubqueryTable): JoinTable =
        JoinTable(this, SqlJoinType.Full, table.copy(lateral = true), None)

    private[sqala] def asSqlTable: SqlTable =
        this match
            case Table(name, alias) =>
                SqlTable.Ident(name, alias.map(a => SqlTableAlias(a, Nil)), None, None, None)
            case SubqueryTable(query, alias, lateral) =>
                SqlTable.Subquery(lateral, query, alias.map(a => SqlTableAlias(a, Nil)), None)
            case JoinTable(left, joinType, right, condition) =>
                SqlTable.Join(left.asSqlTable, joinType, right.asSqlTable, condition.map(c => SqlJoinCondition.On(c.asSqlExpr)))

final case class Table(
    private[sqala] val name: String,
    private[sqala] val alias: Option[String]
) extends AnyTable:
    def as(name: String): Table =
        copy(alias = Some(name))

case class JoinTable(
    private[sqala] val left: AnyTable,
    private[sqala] val joinType: SqlJoinType,
    private[sqala] val right: AnyTable,
    private[sqala] val condition: Option[Expr]
) extends AnyTable:
    def on(expr: Expr): JoinTable =
        copy(condition = Some(expr))

case class SubqueryTable(
    private[sqala] val query: SqlQuery,
    private[sqala] val alias: Option[String],
    private[sqala] val lateral: Boolean
) extends AnyTable:
    def as(name: String): SubqueryTable =
        copy(alias = Some(name))