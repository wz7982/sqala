package sqala.compiletime

import sqala.ast.table.{SqlJoinCondition, SqlJoinType, SqlTable}
import sqala.compiletime.macros.*
import sqala.compiletime.statement.query.Query

import scala.Tuple.*
import scala.compiletime.ops.int.*
import scala.compiletime.{erasedValue, error}
import scala.language.dynamics

sealed abstract class AnyTable[T <: Tuple, Names <: Tuple](private[sqala] val __cols: List[Column[?, ?, ?]]):
    infix def join[JoinT <: Tuple, JoinNames <: Tuple](joinTable: AnyTable[JoinT, JoinNames]): JoinTablePart[Concat[T, JoinT], Concat[Names, JoinNames], JoinTableType[this.type, joinTable.type]] =
        JoinTablePart(this, SqlJoinType.InnerJoin, joinTable)

    infix def leftJoin[JoinT <: Tuple, JoinNames <: Tuple](joinTable: AnyTable[JoinT, JoinNames]): JoinTablePart[Concat[T, FlatTupleOption[JoinT]], Concat[Names, JoinNames], JoinTableType[this.type, joinTable.type]] =
        JoinTablePart(this, SqlJoinType.LeftJoin, joinTable)

    infix def rightJoin[JoinT <: Tuple, JoinNames <: Tuple](joinTable: AnyTable[JoinT, JoinNames]): JoinTablePart[Concat[FlatTupleOption[T], JoinT], Concat[Names, JoinNames], JoinTableType[this.type, joinTable.type]] =
        JoinTablePart(this, SqlJoinType.RightJoin, joinTable)

    infix def fullJoin[JoinT <: Tuple, JoinNames <: Tuple](joinTable: AnyTable[JoinT, JoinNames]): JoinTablePart[Concat[FlatTupleOption[T], FlatTupleOption[JoinT]], Concat[Names, JoinNames], JoinTableType[this.type, joinTable.type]] =
        JoinTablePart(this, SqlJoinType.FullJoin, joinTable)

    def toSqlTable: SqlTable = this match
        case t: Table[_, _] => 
            SqlTable.IdentTable(t.__tableName, t.__aliasName)
        case t: JoinTable[_, _, _] => 
            SqlTable.JoinTable(t.__left.toSqlTable, t.__joinType, t.__right.toSqlTable, Some(SqlJoinCondition.On(t.__onCondition.toSqlExpr)))
        case t: SubQueryTable[_, _, _] =>
            if t.__withTable then
                SqlTable.IdentTable(t.__alias, None)
            else
                SqlTable.SubQueryTable(t.__query.ast, t.__lateral, t.__alias)

case class JoinTablePart[T <: Tuple, Names <: Tuple, Tables <: Tuple](left: AnyTable[?, ?], joinType: SqlJoinType, right: AnyTable[?, ?]):
    infix def on(f: Tables => Expr[Boolean, ?]): JoinTable[T, Names, Tables] =
        val tables = (
            (left, right) match
                case (l: JoinTable[_, _, _], r: JoinTable[_, _, _]) => l.__tables ++ r.__tables
                case (l: JoinTable[_, _, _], r) => l.__tables ++ Tuple1(r)
                case (l, r: JoinTable[_, _, _]) => Tuple1(l) ++ r.__tables
                case (l, r) => (l, r)
        ).asInstanceOf[Tables]
        JoinTable(left, joinType, right, f(tables), tables)

class JoinTable[T <: Tuple, Names <: Tuple, Tables <: Tuple](
    private[sqala] val __left: AnyTable[?, ?],
    private[sqala] val __joinType: SqlJoinType,
    private[sqala] val __right: AnyTable[?, ?],
    private[sqala] val __onCondition: Expr[?, ?],
    private[sqala] val __tables: Tables
) extends AnyTable[T, Names](__left.__cols.appendedAll(__right.__cols))

class Table[E <: Product, Name <: String](
    private[sqala] val __tableName: String,
    private[sqala] val __aliasName: Option[String],
    private[sqala] override val __cols: List[Column[?, ?, ?]]
) extends AnyTable[Tuple1[E], Tuple1[Name]](__cols) with Selectable:
    transparent inline def selectDynamic(inline name: String): Expr[?, ?] =
        val columnInfo = __cols.find(_.identName == name).get
        inline exprMetaDataMacro[E](name) match
            case "pk" => PrimaryKey(columnInfo.tableName, columnInfo.columnName, columnInfo.identName)
            case _ => columnInfo

    transparent inline infix def as(name: String)(using NonEmpty[name.type] =:= true): Any =
        tableAliasMacro[E](name)

    def apply[C <: Tuple](f: this.type => C): (this.type, C) = (this, f(this))

class SubQueryTable[T <: Tuple, TableName <: String, ColumnNames <: Tuple](
    private[sqala] val __query: Query[?, ?],
    private[sqala] val __alias: TableName,
    private[sqala] val __lateral: Boolean = false,
    private[sqala] val __withTable: Boolean = false
) extends AnyTable[T, Tuple1[TableName]](__query.cols.map(_.copy(tableName = __alias))) with Dynamic:
    transparent inline def selectDynamic[ElementName <: String & Singleton](inline name: ElementName): Column[?, ?, ?] =
        inline erasedValue[ColumnNames] match
            case _: EmptyTuple => error("value " + name + " is not a column of this query")
            case _ =>
                inline erasedValue[FindTypeByName[Zip[T, ColumnNames], Size[T] - 1, ElementName]] match
                    case _: Nothing => error("value " + name + " is not a column of this query")
                    case _ =>
                        val columnName = __query.cols.find(_.identName == name).map(_.columnName).getOrElse(name)
                        Column[FindTypeByName[Zip[T, ColumnNames], Size[T] - 1, ElementName], TableName, ElementName](__alias, columnName, name)

case class TableMetaData(
    tableName: String, 
    primaryKeyFields: List[String],
    incrementKeyField: Option[String],
    columnNames: List[String],
    fieldNames: List[String]
)