package sqala.dynamic

import sqala.ast.table.{SqlJoinCondition, SqlJoinType, SqlTableAlias, SqlTable}

sealed trait AnyTable:
    infix def join(table: AnyTable): JoinTable = JoinTable(this, SqlJoinType.Inner, table, None)

    infix def leftJoin(table: AnyTable): JoinTable = JoinTable(this, SqlJoinType.Left, table, None)

    infix def rightJoin(table: AnyTable): JoinTable = JoinTable(this, SqlJoinType.Right, table, None)

    infix def fullJoin(table: AnyTable): JoinTable = JoinTable(this, SqlJoinType.Full, table, None)

    def toSqlTable: SqlTable = this match
        case Table(name, alias) => SqlTable.Range(name, alias.map(a => SqlTableAlias(a)))
        case SubQueryTable(query, alias, lateral) => SqlTable.SubQuery(query.ast, lateral, Some(SqlTableAlias(alias, Nil)))
        case JoinTable(left, joinType, right, condition) => 
            SqlTable.Join(left.toSqlTable, joinType, right.toSqlTable, condition.map(c => SqlJoinCondition.On(c.sqlExpr)), None)

case class Table(name: String, alias: Option[String]) extends AnyTable:
    infix def as(name: String): Table = copy(alias = Some(name))

case class JoinTable(left: AnyTable, joinType: SqlJoinType, right: AnyTable, condition: Option[Expr]) extends AnyTable:
    infix def on(expr: Expr): JoinTable = copy(condition = Some(expr))

case class SubQueryTable(query: Query, alias: String, lateral: Boolean) extends AnyTable