package sqala.static.dsl

import sqala.ast.expr.SqlExpr
import sqala.static.metadata.{TableMacro, TableMetaData}

import scala.NamedTuple.{DropNames, From, NamedTuple, Names}
import sqala.ast.table.SqlTable
import sqala.ast.table.SqlTableAlias
import sqala.static.metadata.SqlBoolean
import sqala.ast.table.SqlJoinCondition

case class Table[T](
    private[sqala] val __aliasName__ : String,
    private[sqala] val __metaData__ : TableMetaData,
    private[sqala] val __sqlTable__ : SqlTable
) extends Selectable:
    type Fields =
        NamedTuple[
            Names[From[Unwrap[T, Option]]],
            Tuple.Map[DropNames[From[Unwrap[T, Option]]], [x] =>> MapField[x, T]]
        ]

    def selectDynamic(name: String): Expr[?] =
        val index = __metaData__.fieldNames.indexWhere(f => f == name)
        Expr(SqlExpr.Column(Some(__aliasName__), __metaData__.columnNames(index)))

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

// TODO result asSelect asFrom toOption等类型类都需要支持
case class FuncTable[T](
    private[sqala] val __aliasName__ : String,
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
        Expr(SqlExpr.Column(Some(__aliasName__), __columnNames__(index)))