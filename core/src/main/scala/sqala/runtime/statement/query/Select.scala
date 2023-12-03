package sqala.runtime.statement.query

import sqala.ast.limit.SqlLimit
import sqala.ast.order.*
import sqala.ast.statement.*
import sqala.runtime.*

class Select(val ast: SqlQuery.Select) extends Query:
    infix def from(table: AnyTable): Select = new Select(ast.copy(from = table.toSqlTable :: Nil))

    infix def select(items: (Expr | SelectItem)*): Select =
        val selectItems = items.toList.map:
            case e: Expr => SqlSelectItem(e.sqlExpr, None)
            case s: SelectItem => SqlSelectItem(s.expr.sqlExpr, Some(s.alias))
        new Select(ast.copy(select = ast.select ++ selectItems))

    infix def where(expr: Expr): Select = new Select(ast.addWhere(expr.sqlExpr))

    infix def orderBy(items: (Expr | OrderBy)*): Select =
        val orderByItems = items.toList.map:
            case e: Expr => SqlOrderBy(e.sqlExpr, None)
            case o: OrderBy => SqlOrderBy(o.expr.sqlExpr, Some(o.order))
        new Select(ast.copy(orderBy = ast.orderBy ++ orderByItems))

    infix def groupBy(items: Expr*): Select =
        val groupByItems = items.toList.map(_.sqlExpr)
        new Select(ast.copy(groupBy = ast.groupBy ++ groupByItems))

    infix def having(expr: Expr): Select = new Select(ast.addHaving(expr.sqlExpr))

    infix def limit(n: Int): Select =
        val sqlLimit = ast.limit.map(l => SqlLimit(n, l.offset)).orElse(Some(SqlLimit(n, 0)))
        new Select(ast.copy(limit = sqlLimit))

    infix def offset(n: Int): Select =
        val sqlLimit = ast.limit.map(l => SqlLimit(l.limit, n)).orElse(Some(SqlLimit(1, n)))
        new Select(ast.copy(limit = sqlLimit))

    def distinct: Select = new Select(ast.copy(param = Some("DISTINCT")))

object Select:
    def apply(): Select = new Select(SqlQuery.Select(select = Nil, from = Nil))