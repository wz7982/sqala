package sqala.dynamic.dsl

import sqala.ast.group.{SqlGroupBy, SqlGroupingItem}
import sqala.ast.limit.{SqlFetch, SqlFetchMode, SqlFetchUnit}
import sqala.ast.quantifier.SqlQuantifier
import sqala.ast.statement.{SqlQuery, SqlSetOperator}
import sqala.metadata.Dialect
import sqala.util.queryToString
import sqala.ast.limit.SqlLimit

sealed class Query(private[sqala] val tree: SqlQuery):
    def sql(dialect: Dialect, standardEscapeStrings: Boolean): String =
        queryToString(tree, dialect, standardEscapeStrings)

    def union(that: Query): UnionQuery =
        UnionQuery(SqlQuery.Set(tree, SqlSetOperator.Union(None), that.tree, Nil, None, None))

    def unionAll(that: Query): UnionQuery =
        UnionQuery(SqlQuery.Set(tree, SqlSetOperator.Union(Some(SqlQuantifier.All)), that.tree, Nil, None, None))

    def except(that: Query): UnionQuery =
        UnionQuery(SqlQuery.Set(tree, SqlSetOperator.Except(None), that.tree, Nil, None, None))

    def exceptAll(that: Query): UnionQuery =
        UnionQuery(SqlQuery.Set(tree, SqlSetOperator.Except(Some(SqlQuantifier.All)), that.tree, Nil, None, None))

    def intersect(that: Query): UnionQuery =
        UnionQuery(SqlQuery.Set(tree, SqlSetOperator.Intersect(None), that.tree, Nil, None, None))

    def intersectAll(that: Query): UnionQuery =
        UnionQuery(SqlQuery.Set(tree, SqlSetOperator.Intersect(Some(SqlQuantifier.All)), that.tree, Nil, None, None))

    def limit(n: Int): Query =
        val limit = tree match
            case s: SqlQuery.Select => s.limit
            case s: SqlQuery.Set => s.limit
            case SqlQuery.Cte(_, _, s: SqlQuery.Select, _) => s.limit
            case SqlQuery.Cte(_, _, s: SqlQuery.Set, _) => s.limit
            case _ => None
        val sqlLimit = limit
            .map(l => SqlLimit(l.offset, Some(SqlFetch(value(n).asSqlExpr, SqlFetchUnit.RowCount, SqlFetchMode.Only))))
            .orElse(Some(SqlLimit(None, Some(SqlFetch(value(n).asSqlExpr, SqlFetchUnit.RowCount, SqlFetchMode.Only)))))
        val newTree = tree match
            case s: SqlQuery.Select => s.copy(limit = sqlLimit)
            case s: SqlQuery.Set => s.copy(limit = sqlLimit)
            case SqlQuery.Cte(w, r, s: SqlQuery.Select, l) =>
                SqlQuery.Cte(w, r, s.copy(limit = sqlLimit), l)
            case SqlQuery.Cte(w, r, s: SqlQuery.Set, l) =>
                SqlQuery.Cte(w, r, s.copy(limit = sqlLimit), l)
            case _ => tree
        Query(newTree)

    def offset(n: Int): Query =
        val sqlExpr = value(n).asSqlExpr
        val limit = tree match
            case s: SqlQuery.Select => s.limit
            case s: SqlQuery.Set => s.limit
            case SqlQuery.Cte(_, _, s: SqlQuery.Select, _) => s.limit
            case SqlQuery.Cte(_, _, s: SqlQuery.Set, _) => s.limit
            case _ => None
        val sqlLimit = limit
            .map(l => SqlLimit(Some(sqlExpr), l.fetch))
            .orElse(Some(SqlLimit(Some(sqlExpr), None)))
        val newTree = tree match
            case s: SqlQuery.Select => s.copy(limit = sqlLimit)
            case s: SqlQuery.Set => s.copy(limit = sqlLimit)
            case SqlQuery.Cte(w, r, s: SqlQuery.Select, l) =>
                SqlQuery.Cte(w, r, s.copy(limit = sqlLimit), l)
            case SqlQuery.Cte(w, r, s: SqlQuery.Set, l) =>
                SqlQuery.Cte(w, r, s.copy(limit = sqlLimit), l)
            case _ => tree
        Query(newTree)

final case class SelectQuery(override private[sqala] val tree: SqlQuery.Select) extends Query(tree):
    def where(expr: Expr): SelectQuery =
        SelectQuery(tree.addWhere(expr.asSqlExpr))

    def orderBy(orders: Order*): SelectQuery =
        SelectQuery(tree.copy(orderBy = tree.orderBy ++ orders.toList.map(_.order)))

    def orderBy(orders: List[Order]): SelectQuery =
        orderBy(orders*)

    def groupBy(exprs: Expr*): SelectQuery =
        val groupItems = tree.groupBy.map(_.items).getOrElse(Nil)
        SelectQuery(
            tree.copy(
                groupBy =
                    Some(
                        SqlGroupBy(
                            tree.groupBy.flatMap(_.quantifier),
                            groupItems ++ exprs.toList.map(i => SqlGroupingItem.Expr(i.asSqlExpr))
                        )
                    )
            )
        )

    def groupBy(exprs: List[Expr]): SelectQuery =
        groupBy(exprs*)

    def having(expr: Expr): SelectQuery =
        SelectQuery(tree.addHaving(expr.asSqlExpr))

    def select(items: SelectItem*): SelectQuery =
        SelectQuery(tree.copy(select = tree.select ++ items.toList.map(_.item)))

    def select(items: List[SelectItem]): SelectQuery =
        select(items*)

    def selectDistinct(items: SelectItem*): SelectQuery =
        SelectQuery(tree.copy(quantifier = Some(SqlQuantifier.Distinct), select = tree.select ++ items.toList.map(_.item)))

    def selectDistinct(items: List[SelectItem]): SelectQuery =
        selectDistinct(items*)

final case class UnionQuery(override private[sqala] val tree: SqlQuery.Set) extends Query(tree):
    def orderBy(orders: Order*): UnionQuery =
        copy(tree = tree.copy(orderBy = tree.orderBy ++ orders.toList.map(_.order)))

    def orderBy(orders: List[Order]): UnionQuery =
        orderBy(orders*)