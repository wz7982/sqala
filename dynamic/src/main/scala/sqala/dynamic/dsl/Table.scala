package sqala.dynamic.dsl

import sqala.ast.expr.SqlExpr
import sqala.ast.table.{SqlJoinCondition, SqlJoinType, SqlTable, SqlTableAlias}
import sqala.dynamic.parser.SqlParser
import sqala.metadata.TableMetaData

import scala.language.dynamics

sealed trait AnyTable:
    infix def join(table: AnyTable): JoinTable = JoinTable(this, SqlJoinType.Inner, table, None)

    infix def leftJoin(table: AnyTable): JoinTable = JoinTable(this, SqlJoinType.Left, table, None)

    infix def rightJoin(table: AnyTable): JoinTable = JoinTable(this, SqlJoinType.Right, table, None)

    infix def fullJoin(table: AnyTable): JoinTable = JoinTable(this, SqlJoinType.Full, table, None)

    def toSqlTable: SqlTable = this match
        case Table(name, alias) => SqlTable.Range(name, alias.map(a => SqlTableAlias(a)))
        case EntityTable(name, alias, _) => SqlTable.Range(name, Some(SqlTableAlias(alias)))
        case SubQueryTable(query, alias, lateral) => SqlTable.SubQuery(query.tree, lateral, Some(SqlTableAlias(alias, Nil)))
        case JoinTable(left, joinType, right, condition) => 
            SqlTable.Join(left.toSqlTable, joinType, right.toSqlTable, condition.map(c => SqlJoinCondition.On(c.sqlExpr)), None)

case class Table(
    private[sqala] val name: String, 
    private[sqala] val alias: Option[String]
) extends AnyTable:
    infix def as(name: String): Table =
        SqlParser.parseIdent(name)
        copy(alias = Some(name))

case class EntityTable(
    private[sqala] val __name__ : String, 
    private[sqala] val __alias__ : String, 
    private[sqala] val __metaData__ : TableMetaData 
) extends AnyTable with Dynamic:
    infix def as(name: String): EntityTable =
        SqlParser.parseIdent(name) 
        copy(__alias__ = name)

    def selectDynamic(name: String): Expr =
        val columnName = __metaData__.fieldNames
            .zip(__metaData__.columnNames)
            .find: (f, _) =>
                f == name
            .map(_._2)
            .get
        Expr(SqlExpr.Column(Some(__alias__), columnName))

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