package sqala.test

import sqala.ast.expr.*
import sqala.ast.order.{SqlOrdering, SqlOrderingItem}
import sqala.ast.statement.*
import sqala.ast.table.SqlTable
import sqala.util.NonEmptyList

/** Shared builders for suites that need a query or a table reference. */
object QueryFixture:
    def col(n: String): SqlExpr = SqlExpr.Column(None, n)
    def qcol(t: String, n: String): SqlExpr = SqlExpr.Column(Some(t), n)
    def num(n: Int): SqlExpr = SqlExpr.NumberLiteral(n)
    def str(s: String): SqlExpr = SqlExpr.StringLiteral(s)
    def nel[A](h: A, t: A*): NonEmptyList[A] = NonEmptyList(h, t.toList)
    def ord(e: SqlExpr): SqlOrderingItem = SqlOrderingItem(e, None, None)
    def ordDesc(e: SqlExpr): SqlOrderingItem = SqlOrderingItem(e, Some(SqlOrdering.Desc), None)
    def eqTo(e: SqlExpr, v: SqlExpr): SqlExpr = SqlExpr.Binary(e, SqlBinaryOperator.Equal, v)
    def gt(e: SqlExpr, n: Int): SqlExpr = SqlExpr.Binary(e, SqlBinaryOperator.GreaterThan, num(n))

    def table(n: String): SqlTable.Ident = SqlTable.Ident(n, None, None, None, None)
    def item(e: SqlExpr): SqlSelectItem = SqlSelectItem.Expr(e, None)
    def itemAs(e: SqlExpr, a: String): SqlSelectItem = SqlSelectItem.Expr(e, Some(a))

    def select(items: NonEmptyList[SqlSelectItem], from: List[SqlTable] = Nil): SqlQuery.Select =
        SqlQuery.Select(None, items, from, None, None, None, Nil, None, Nil, None, None)

    /** `SELECT "a" FROM "t"`, the smallest query usable as a subquery. */
    def simple: SqlQuery.Select = select(nel(item(col("a"))), List(table("t")))