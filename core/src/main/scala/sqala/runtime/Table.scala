package sqala.runtime

import sqala.ast.table.*
import sqala.runtime.statement.query.Query

sealed trait AnyTable:
    infix def join(table: AnyTable): JoinTable = JoinTable(this, SqlJoinType.InnerJoin, table, None)

    infix def leftJoin(table: AnyTable): JoinTable = JoinTable(this, SqlJoinType.LeftJoin, table, None)

    infix def rightJoin(table: AnyTable): JoinTable = JoinTable(this, SqlJoinType.RightJoin, table, None)

    infix def fullJoin(table: AnyTable): JoinTable = JoinTable(this, SqlJoinType.FullJoin, table, None)

    def toSqlTable: SqlTable = this match
        case Table(name, alias) => SqlTable.IdentTable(name, alias)
        case SubQueryTable(query, alias, lateral) => SqlTable.SubQueryTable(query.ast, lateral, alias)
        case JoinTable(left, joinType, right, condition) => 
            SqlTable.JoinTable(left.toSqlTable, joinType, right.toSqlTable, condition.map(c => SqlJoinCondition.On(c.sqlExpr)))

case class Table(name: String, alias: Option[String]) extends AnyTable:
    infix def as(name: String): Table = copy(alias = Some(name))

case class JoinTable(left: AnyTable, joinType: SqlJoinType, right: AnyTable, condition: Option[Expr]) extends AnyTable:
    infix def on(expr: Expr): JoinTable = copy(condition = Some(expr))

case class SubQueryTable(query: Query, alias: String, lateral: Boolean) extends AnyTable