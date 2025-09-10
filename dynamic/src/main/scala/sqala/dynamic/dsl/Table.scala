package sqala.dynamic.dsl

import sqala.ast.expr.SqlExpr
import sqala.ast.table.{SqlJoinCondition, SqlJoinType, SqlTable, SqlTableAlias}
import sqala.dynamic.parser.SqlParser

import scala.language.dynamics

sealed trait AnyTable:
    infix def join(table: AnyTable): JoinTable = JoinTable(this, SqlJoinType.Inner, table, None)

    infix def leftJoin(table: AnyTable): JoinTable = JoinTable(this, SqlJoinType.Left, table, None)

    infix def rightJoin(table: AnyTable): JoinTable = JoinTable(this, SqlJoinType.Right, table, None)

    infix def fullJoin(table: AnyTable): JoinTable = JoinTable(this, SqlJoinType.Full, table, None)

    def toSqlTable: SqlTable = this match
        case Table(name, alias) => 
            SqlTable.Standard(name, alias.map(a => SqlTableAlias(a, Nil)), None, None)
        case SubQueryTable(query, alias, lateral) => 
            SqlTable.SubQuery(query.tree, lateral, Some(SqlTableAlias(alias, Nil)), None)
        case JoinTable(left, joinType, right, condition) => 
            SqlTable.Join(left.toSqlTable, joinType, right.toSqlTable, condition.map(c => SqlJoinCondition.On(c.sqlExpr)))

case class Table(
    private[sqala] val name: String, 
    private[sqala] val alias: Option[String]
) extends AnyTable:
    infix def as(name: String): Table =
        new SqlParser().parseIdent(name)
        copy(alias = Some(name))

case class JoinTable(
    private[sqala] val left: AnyTable, 
    private[sqala] val joinType: SqlJoinType, 
    private[sqala] val right: AnyTable, 
    private[sqala] val condition: Option[Expr]
) extends AnyTable:
    infix def on(expr: Expr): JoinTable = copy(condition = Some(expr))

case class SubQueryTable(
    private[sqala] val __query__ : DynamicQuery, 
    private[sqala] val __alias__ : String, 
    private[sqala] val __lateral__ : Boolean
) extends AnyTable with Dynamic:
    def selectDynamic(name: String): Expr =
        Expr(SqlExpr.Column(Some(__alias__), name))