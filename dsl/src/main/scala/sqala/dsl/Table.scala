package sqala.dsl

import sqala.ast.expr.SqlExpr
import sqala.ast.statement.{SqlQuery, SqlSelectItem}
import sqala.ast.table.{SqlJoinCondition, SqlJoinType, SqlTable, SqlTableAlias}

import scala.NamedTuple.{Elem, From, Map, Names}
import scala.collection.mutable.ListBuffer
import scala.compiletime.constValue
import scala.language.dynamics

case class Table[T](
    private[sqala] __tableName__ : String, 
    private[sqala] __aliasName__ : String,
    private[sqala] __metaData__ : TableMetaData
) extends Selectable:
    type Fields = Map[From[Unwrap[T, Option]], [x] =>> MapField[x, T]]

    def selectDynamic(name: String): Expr[?, ?] =
        val columnMap = __metaData__.fieldNames.zip(__metaData__.columnNames).toMap
        Expr.Column(__aliasName__, columnMap(name))

object Table:
    extension [T](table: Table[T])
        def * : table.Fields =
            val columns = table.__metaData__.columnNames
                .map(n => Expr.Column(table.__aliasName__, n))
            val columnTuple = Tuple.fromArray(columns.toArray)
            NamedTuple(columnTuple).asInstanceOf[table.Fields]

opaque type TableNames[N] = Unit

object TableNames:
    def apply[N]: TableNames[N] = ()

opaque type TableTypes[T] = Unit

object TableTypes:
    def apply[T]: TableTypes[T] = ()

sealed trait SelectTable[T <: Tuple, N <: Tuple]:
    private[sqala] def selectItems(cursor: Int): List[SqlSelectItem]

    private[sqala] def asSqlTable: SqlTable = this match
        case EntityTable(t, a, _) =>
            SqlTable.IdentTable(t, Some(SqlTableAlias(a)))
        case JoinTable(l, t, r, c) =>
            SqlTable.JoinTable(l.asSqlTable, t, r.asSqlTable, Some(c))
        case SubQueryTable(ast, a, l) =>
            SqlTable.SubQueryTable(ast, l, SqlTableAlias(a))

    infix def join[JT <: Tuple, JN <: Tuple](joinTable: SelectTable[JT, JN])(using Repeat[N, JN] =:= false): JoinPart[Merge[T, JT], Merge[N, JN]] =
        JoinPart(this, SqlJoinType.InnerJoin, joinTable)

    infix def leftJoin[JT <: Tuple, JN <: Tuple](joinTable: SelectTable[JT, JN])(using Repeat[N, JN] =:= false): JoinPart[Merge[T, SelectTypeMapOption[JT]], Merge[N, JN]] =
        JoinPart(this, SqlJoinType.LeftJoin, joinTable)

    infix def rightJoin[JT <: Tuple, JN <: Tuple](joinTable: SelectTable[JT, JN])(using Repeat[N, JN] =:= false): JoinPart[Merge[SelectTypeMapOption[T], JT], Merge[N, JN]] =
        JoinPart(this, SqlJoinType.RightJoin, joinTable)

case class EntityTable[T, A <: String](
    private[sqala] __tableName__ : String,
    private[sqala] __aliasName__ : A,
    private[sqala] __metaData__ : TableMetaData
) extends Dynamic with SelectTable[T *: EmptyTuple, A *: EmptyTuple]:
    private[sqala] override def selectItems(cursor: Int): List[SqlSelectItem] =
        var tmpCursor = cursor
        val items = ListBuffer[SqlSelectItem]()
        for field <- __metaData__.columnNames do
            items.addOne(SqlSelectItem(SqlExpr.Column(Some(__aliasName__), field), Some(s"c${tmpCursor}")))
            tmpCursor += 1
        items.toList

    inline def selectDynamic[TT, TN <: Tuple](name: String)(using TableTypes[TT], TableNames[TN]): Expr[Field[Elem[From[T], Index[Names[From[T]], name.type, 0]], Fetch[TN, TT, A]], ColumnKind] =
        val columnMap = __metaData__.fieldNames.zip(__metaData__.columnNames).toMap
        Expr.Column(__aliasName__, columnMap(name))

    inline infix def as(name: String)(using NonEmpty[name.type] =:= true): EntityTable[T, name.type] =
        copy(__aliasName__ = name)

case class JoinTable[T <: Tuple, N <: Tuple](
    private[sqala] left: SelectTable[?, ?],
    private[sqala] joinType: SqlJoinType,
    private[sqala] right: SelectTable[?, ?],
    private[sqala] condition: SqlJoinCondition,
) extends SelectTable[T, N]:
    private[sqala] override def selectItems(cursor: Int): List[SqlSelectItem] =
        left.selectItems(cursor) ++ right.selectItems(cursor + left.selectItems(cursor).size)

case class JoinPart[T <: Tuple, N <: Tuple](
    private[sqala] left: SelectTable[?, ?],
    private[sqala] joinType: SqlJoinType,
    private[sqala] right: SelectTable[?, ?]
):
    infix def on[K <: SimpleKind](cond: TableNames[N] ?=> TableTypes[T] ?=> Expr[Boolean, K]): JoinTable[T, N] =
        given TableNames[N] = TableNames[N]
        given TableTypes[T] = TableTypes[T]
        JoinTable(left, joinType, right, SqlJoinCondition.On(cond.asSqlExpr))

case class SubQueryTable[N <: Tuple, V <: Tuple, A <: String](
    private[sqala] __ast__ : SqlQuery,
    private[sqala] __aliasName__ : A,
    private[sqala] __lateral__ : Boolean
) extends Dynamic with SelectTable[NamedResult[N, UnwrapExpr[V]] *: EmptyTuple, A *: EmptyTuple]:
    inline def selectDynamic[TT, TN <: Tuple](name: String)(using TableTypes[TT], TableNames[TN]): Expr[Field[Tuple.Elem[UnwrapExpr[V], Index[N, name.type, 0]], Fetch[TN, TT, A]], ColumnKind] =
        val items = selectItems(0)
        val index = constValue[Index[N, name.type, 0]]
        val item = items(index)
        Expr.Column(__aliasName__, item.alias.get)

    private[sqala] override def selectItems(cursor: Int): List[SqlSelectItem] =
        def queryItems(ast: SqlQuery): List[SqlSelectItem] = ast match
            case SqlQuery.Select(_, s, _, _, _, _, _, _, _) => s
            case SqlQuery.Union(l, _, _) => queryItems(l)
            case _ => Nil
        var tmpCursor = cursor
        val items = ListBuffer[SqlSelectItem]()
        for field <- queryItems(__ast__).map(_.alias.get) do
            items.addOne(SqlSelectItem(SqlExpr.Column(Some(__aliasName__), field), Some(s"c${tmpCursor}")))
            tmpCursor += 1
        items.toList

case class LateralSubQuery[N <: Tuple, V <: Tuple](
    private[sqala] ast: SqlQuery
):
    infix def as(name: String)(using AsExpr[V], NonEmpty[name.type] =:= true): SubQueryTable[N, V, name.type] =
        SubQueryTable(ast, name, true)

case class TableMetaData(
    tableName: String,
    primaryKeyFields: List[String],
    incrementField: Option[String],
    columnNames: List[String],
    fieldNames: List[String]
)