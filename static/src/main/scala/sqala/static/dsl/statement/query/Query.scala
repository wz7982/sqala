package sqala.static.dsl.statement.query

import sqala.ast.expr.{SqlBinaryOperator, SqlExpr, SqlSubLinkQuantifier}
import sqala.ast.group.{SqlGroupBy, SqlGroupingItem}
import sqala.ast.limit.*
import sqala.ast.quantifier.SqlQuantifier
import sqala.ast.statement.*
import sqala.ast.table.{SqlJoinCondition, SqlJoinType, SqlTable, SqlTableAlias}
import sqala.static.metadata.*
import sqala.printer.Dialect
import sqala.static.dsl.*
import sqala.util.queryToString

import scala.NamedTuple.NamedTuple
import scala.Tuple.Append

sealed class Query[T](
    private[sqala] val params: T,
    val tree: SqlQuery
)(using 
    private[sqala] val context: QueryContext
)

object Query:
    extension [T](query: Query[T])
        def sql(dialect: Dialect): (String, Array[Any]) =
            queryToString(query.tree, dialect, true)

class SelectQuery[T](
    private[sqala] override val params: T,
    override val tree: SqlQuery.Select
)(using 
    private[sqala] override val context: QueryContext
) extends Query[T](params, tree)

object SelectQuery:
    extension [T](query: SelectQuery[T])
        infix def filter[F: AsExpr as a](f: T => F)(using SqlBoolean[a.R]): SelectQuery[T] =
            val cond = a.asExpr(f(query.params))
            SelectQuery(query.params, query.tree.addWhere(cond.asSqlExpr))(using query.context)

        infix def where[F: AsExpr as a](f: T => F)(using SqlBoolean[a.R]): SelectQuery[T] =
            filter(f)

        def filterIf[F: AsExpr as a](test: => Boolean)(f: T => F)(using SqlBoolean[a.R]): SelectQuery[T] =
            if test then filter(f) else query

        def whereIf[F: AsExpr as a](test: => Boolean)(f: T => F)(using SqlBoolean[a.R]): SelectQuery[T] =
            filterIf(test)(f)

        def withFilter[F: AsExpr as a](f: T => F)(using SqlBoolean[a.R]): SelectQuery[T] =
            filter(f)