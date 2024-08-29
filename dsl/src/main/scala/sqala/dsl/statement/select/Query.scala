package sqala.dsl.statement.select

import sqala.ast.expr.SqlExpr
import sqala.ast.limit.SqlLimit
import sqala.ast.statement.*
import sqala.dsl.*

import scala.NamedTuple.NamedTuple

sealed trait Query[T] extends Queryable[T]

object Query:
    extension [N <: Tuple, V <: Tuple](query: Query[NamedTuple[N, V]])
        infix def as(name: String)(using a: AsExpr[V], s: SelectItem[V], n: NonEmpty[name.type] =:= true): SubQueryTable[N, ToTuple[s.R], name.type] =
            SubQueryTable(query.ast, name, false)

class SelectQuery[T](val ast: SqlQuery.Select) extends Query[T]:
    infix def limit(n: Int): SelectQuery[T] =
        val sqlLimit = ast.limit.map(l => SqlLimit(SqlExpr.NumberLiteral(n), l.offset)).orElse(Some(SqlLimit(SqlExpr.NumberLiteral(n), SqlExpr.NumberLiteral(0))))
        new SelectQuery(ast.copy(limit = sqlLimit))

    infix def offset(n: Int): SelectQuery[T] =
        val sqlLimit = ast.limit.map(l => SqlLimit(l.limit, SqlExpr.NumberLiteral(n))).orElse(Some(SqlLimit(SqlExpr.NumberLiteral(1), SqlExpr.NumberLiteral(n))))
        new SelectQuery(ast.copy(limit = sqlLimit))

class TableQuery[T <: Tuple, N <: Tuple](override val ast: SqlQuery.Select) extends SelectQuery[SelectTable[T, N]](ast):
    given TableNames[N] = TableNames[N]
    given TableTypes[T] = TableTypes[T]

    infix def where[K <: SimpleKind](cond: TableNames[N] ?=> TableTypes[T] ?=> Expr[Boolean, K]): TableQuery[T, N] =
        val condition = cond.asSqlExpr
        new TableQuery(ast.addWhere(condition))

    def whereIf[K <: SimpleKind](test: Boolean)(cond: TableNames[N] ?=> TableTypes[T] ?=> Expr[Boolean, K]): TableQuery[T, N] =
        if test then where(cond) else this

    infix def orderBy(items: TableNames[N] ?=> TableTypes[T] ?=> OrderBy[?]*): TableQuery[T, N] =
        val sqlOrderBy = items.toList.map(_.asSqlOrderBy)
        new TableQuery(ast.copy(orderBy = ast.orderBy ++ sqlOrderBy))

    infix def select[R](items: TableNames[N] ?=> TableTypes[T] ?=> R)(using s: SelectItem[R]): SelectQuery[s.R] =
        val selectItems = s.selectItems(items, 0)
        new SelectQuery(ast.copy(select = selectItems))

    infix def selectDistinct[R](items: TableNames[N] ?=> TableTypes[T] ?=> R)(using s: SelectItem[R]): SelectQuery[s.R] =
        val selectItems = s.selectItems(items, 0)
        new SelectQuery(ast.copy(select = selectItems, param = Some(SqlSelectParam.Distinct)))

    infix def groupBy[G](items: TableNames[N] ?=> TableTypes[T] ?=> G)(using a: AsExpr[G]): TableQuery[T, N] =
        val sqlGroupBy = a.asExprs(items).map(_.asSqlExpr)
        new TableQuery(ast.copy(groupBy = sqlGroupBy))

    infix def groupByCube[G](items: TableNames[N] ?=> TableTypes[T] ?=> G)(using a: AsExpr[G]): TableQuery[T, N] =
        val sqlGroupBy = SqlExpr.Func("CUBE", a.asExprs(items).map(_.asSqlExpr))
        new TableQuery(ast.copy(groupBy = sqlGroupBy :: Nil))

    infix def groupByRollup[G](items: TableNames[N] ?=> TableTypes[T] ?=> G)(using a: AsExpr[G]): TableQuery[T, N] =
        val sqlGroupBy = SqlExpr.Func("ROLLUP", a.asExprs(items).map(_.asSqlExpr))
        new TableQuery(ast.copy(groupBy = sqlGroupBy :: Nil))

    infix def having[K <: SimpleKind](cond: TableNames[N] ?=> TableTypes[T] ?=> Expr[Boolean, K]): TableQuery[T, N] =
        val condition = cond.asSqlExpr
        new TableQuery(ast.addHaving(condition))