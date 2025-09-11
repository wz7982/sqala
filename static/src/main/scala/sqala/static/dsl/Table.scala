package sqala.static.dsl

import sqala.ast.expr.SqlExpr
import sqala.static.metadata.{TableMacro, TableMetaData}

import scala.NamedTuple.{DropNames, From, NamedTuple, Names}
import sqala.ast.table.SqlTable
import sqala.ast.table.SqlTableAlias
import sqala.static.metadata.SqlBoolean
import sqala.ast.table.SqlJoinCondition
import scala.compiletime.constValue
import sqala.ast.expr.SqlJsonTableColumn
import sqala.ast.expr.SqlType
import sqala.static.dsl.statement.query.Query

// TODO 允许别名为None
// TODO insert表别名为None update也要加表别名
// TODO matchRecognize的partition by里和order by里表别名为None
// TODO 允许别名为None之后，就可以做表查询的union了
case class Table[T](
    private[sqala] val __aliasName__ : Option[String],
    private[sqala] val __metaData__ : TableMetaData,
    private[sqala] val __sqlTable__ : SqlTable.Standard
) extends Selectable:
    type Fields =
        NamedTuple[
            Names[From[Unwrap[T, Option]]],
            Tuple.Map[DropNames[From[Unwrap[T, Option]]], [x] =>> MapField[x, T]]
        ]

    def selectDynamic(name: String): Expr[?] =
        val index = __metaData__.fieldNames.indexWhere(f => f == name)
        Expr(SqlExpr.Column(__aliasName__, __metaData__.columnNames(index)))

case class JoinTable[T](
    private[sqala] val params: T,
    private[sqala] val sqlTable: SqlTable.Join
)

case class JoinPart[T](
    private[sqala] val params: T,
    private[sqala] val sqlTable: SqlTable.Join
):
    infix def on[F: AsExpr as a](f: T => F)(using SqlBoolean[a.R]): JoinTable[T] =
        val cond = a.asExpr(f(params))
        JoinTable(
            params,
            sqlTable.copy(condition = Some(SqlJoinCondition.On(cond.asSqlExpr)))
        )

// TODO result asSelect asFrom toOption等类型类都需要支持，可能可以去掉，共用Table
case class FuncTable[T](
    private[sqala] val __aliasName__ : Option[String],
    private[sqala] val __fieldNames__ : List[String],
    private[sqala] val __columnNames__ : List[String],
    private[sqala] val __sqlTable__ : SqlTable.Func
) extends Selectable:
    type Fields =
        NamedTuple[
            Names[From[Unwrap[T, Option]]],
            Tuple.Map[DropNames[From[Unwrap[T, Option]]], [x] =>> MapField[x, T]]
        ]

    def selectDynamic(name: String): Expr[?] =
        val index = __fieldNames__.indexWhere(f => f == name)
        Expr(SqlExpr.Column(__aliasName__, __columnNames__(index)))

case class JsonTable[N <: Tuple, V <: Tuple](
    private[sqala] val __alias__ : Option[String],
    private[sqala] val __items__ : V,
    private[sqala] val __sqlTable__ : SqlTable.Json
) extends Selectable:
    type Fields = NamedTuple[N, V]

    inline def selectDynamic(name: String): Any =
        val index = constValue[Index[N, name.type, 0]]
        __items__.toList(index)

object JsonTable:
    def apply[N <: Tuple, V <: Tuple](
        expr: SqlExpr,
        path: SqlExpr,
        alias: Option[String], 
        columns: JsonTableColumns[N, V]
    )(using
        p: AsTableParam[JsonTableColumnFlatten[V]]
    ): JsonTable[JsonTableColumnNameFlatten[N, V], JsonTableColumnFlatten[V]] =
        var index = 0

        def toSqlColumns(columns: List[JsonTableColumn]): List[SqlJsonTableColumn] =
            columns.map:
                case p: JsonTablePathColumn[?] =>
                    index += 1
                    SqlJsonTableColumn.Column(s"c$index", p.`type`, None, Some(p.path), None, None, None, None)
                case o: JsonTableOrdinalColumn =>
                    index += 1
                    SqlJsonTableColumn.Ordinality(s"c$index")
                case e: JsonTableExistsColumn =>
                    index += 1
                    SqlJsonTableColumn.Exists(s"c$index", SqlType.Boolean, Some(e.path), None)
                case n: JsonTableNestedColumns[?, ?] =>
                    SqlJsonTableColumn.Nested(n.path, None, toSqlColumns(n.columns))
                
        val sqlColumns = toSqlColumns(columns.columns)
        new JsonTable(
            alias,
            p.asTableParam(alias, 1),
            SqlTable.Json(
                expr,
                path,
                None,
                Nil,
                sqlColumns,
                None,
                false,
                alias.map(SqlTableAlias(_, Nil)),
                None
            )
        )

case class JsonTableColumns[N <: Tuple, V <: Tuple](private[sqala] columns: List[JsonTableColumn])

sealed trait JsonTableColumn
case class JsonTableNestedColumns[N <: Tuple, V <: Tuple](
    private[sqala] path: SqlExpr, 
    private[sqala] columns: List[JsonTableColumn]
) extends JsonTableColumn
class JsonTableOrdinalColumn extends JsonTableColumn
case class JsonTablePathColumn[T](
    private[sqala] path: SqlExpr, 
    private[sqala] `type`: SqlType
) extends JsonTableColumn
case class JsonTableExistsColumn(private[sqala] path: SqlExpr) extends JsonTableColumn

class SubQueryTable[N <: Tuple, V <: Tuple](
    private[sqala] val __alias__ : Option[String],
    private[sqala] val __items__ : V,
    private[sqala] val __sqlTable__ : SqlTable.SubQuery
) extends Selectable:
    type Fields = NamedTuple[N, V]

    inline def selectDynamic(name: String): Any =
        val index = constValue[Index[N, name.type, 0]]
        __items__.toList(index)

object SubQuery:
    def apply[N <: Tuple, V <: Tuple](query: Query[?], lateral: Boolean, alias: Option[String])(using 
        p: AsTableParam[V]
    ): SubQueryTable[N, V] =
        new SubQueryTable(
            alias, 
            p.asTableParam(alias, 1),
            SqlTable.SubQuery(
                query.tree,
                lateral,
                alias.map(SqlTableAlias(_, Nil)),
                None
            )
        )