package sqala.dsl.statement.query

import sqala.ast.expr.SqlExpr
import sqala.ast.limit.SqlLimit
import sqala.ast.order.SqlOrderBy
import sqala.ast.statement.{SqlQuery, SqlSelectItem, SqlSelectParam, SqlUnionType}
import sqala.ast.table.{SqlJoinType, SqlSubQueryAlias, SqlTable}
import sqala.dsl.*
import sqala.dsl.macros.{tableMetaDataMacro, tableNameMacro}

import scala.NamedTuple.*
import scala.annotation.targetName
import scala.compiletime.erasedValue

sealed class Query[T](private[sqala] val queryItems: T, val ast: SqlQuery)

object Query:
    extension [T, E <: Expr[T], N <: Tuple](query: Query[NamedTupleWrapper[N, Tuple1[E]]])
        @targetName("namedTupleQueryAsExpr")
        def asExpr: Expr[T] = SubQuery(query)

    extension [T, E <: Expr[T]](query: Query[E])
        @targetName("exprQueryAsExpr")
        def asExpr: Expr[T] = SubQuery(query)

    extension [N <: Tuple, V <: Tuple, UN <: Tuple, UV <: Tuple](query: Query[NamedTupleWrapper[N, V]])
        infix def union(unionQuery: Query[NamedTupleWrapper[UN, UV]]): Query[NamedTupleWrapper[N, Union[V, UV]]] =
            UnionQuery(query, SqlUnionType.Union, unionQuery.ast)

        infix def unionAll(unionQuery: Query[NamedTupleWrapper[UN, UV]]): Query[NamedTupleWrapper[N, Union[V, UV]]] =
            UnionQuery(query, SqlUnionType.UnionAll, unionQuery.ast)

        def ++(unionQuery: Query[NamedTupleWrapper[UN, UV]]): Query[NamedTupleWrapper[N, Union[V, UV]]] =
            UnionQuery(query, SqlUnionType.UnionAll, unionQuery.ast)

        infix def except(unionQuery: Query[NamedTupleWrapper[UN, UV]]): Query[NamedTupleWrapper[N, Union[V, UV]]] =
            UnionQuery(query, SqlUnionType.Except, unionQuery.ast)

        infix def exceptAll(unionQuery: Query[NamedTupleWrapper[UN, UV]]): Query[NamedTupleWrapper[N, Union[V, UV]]] =
            UnionQuery(query, SqlUnionType.ExceptAll, unionQuery.ast)

        infix def intersect(unionQuery: Query[NamedTupleWrapper[UN, UV]]): Query[NamedTupleWrapper[N, Union[V, UV]]] =
            UnionQuery(query, SqlUnionType.Intersect, unionQuery.ast)

        infix def intersectAll(unionQuery: Query[NamedTupleWrapper[UN, UV]]): Query[NamedTupleWrapper[N, Union[V, UV]]] =
            UnionQuery(query, SqlUnionType.IntersectAll, unionQuery.ast)

    extension [N <: Tuple, V <: Tuple, L <: Product](query: Query[NamedTupleWrapper[N, V]])
        private inline def unionListClause(unionType: SqlUnionType, list: List[L]): Query[NamedTupleWrapper[N, Union[V, Tuple.Map[DropNames[From[L]], Expr]]]] =
            val instances = AsSqlExpr.summonInstances[DropNames[From[L]]]
            val values = SqlQuery.Values(
                list.map: v =>
                    val terms = v.productIterator.toList
                    terms.zip(instances).map: (term, instance) =>
                        instance.asInstanceOf[AsSqlExpr[Any]].asSqlExpr(term)
            )
            UnionQuery(query, unionType, values)

        inline infix def union(list: List[L]): Query[NamedTupleWrapper[N, Union[V, Tuple.Map[DropNames[From[L]], Expr]]]] =
            unionListClause(SqlUnionType.Union, list)

        inline infix def unionAll(list: List[L]): Query[NamedTupleWrapper[N, Union[V, Tuple.Map[DropNames[From[L]], Expr]]]] =
            unionListClause(SqlUnionType.UnionAll, list)

        inline infix def except(list: List[L]): Query[NamedTupleWrapper[N, Union[V, Tuple.Map[DropNames[From[L]], Expr]]]] =
            unionListClause(SqlUnionType.Except, list)

        inline infix def exceptAll(list: List[L]): Query[NamedTupleWrapper[N, Union[V, Tuple.Map[DropNames[From[L]], Expr]]]] =
            unionListClause(SqlUnionType.ExceptAll, list)

        inline infix def intersect(list: List[L]): Query[NamedTupleWrapper[N, Union[V, Tuple.Map[DropNames[From[L]], Expr]]]] =
            unionListClause(SqlUnionType.Intersect, list)

        inline infix def intersectAll(list: List[L]): Query[NamedTupleWrapper[N, Union[V, Tuple.Map[DropNames[From[L]], Expr]]]] =
            unionListClause(SqlUnionType.IntersectAll, list)
        
class SelectQuery[T](
    private[sqala] val items: T,
    override val ast: SqlQuery.Select
)(using q: QueryContext) extends Query[T](items, ast):
    def filter(f: T => Expr[Boolean]): SelectQuery[T] =
        val condition = f(items).asSqlExpr
        SelectQuery(items, ast.addWhere(condition))

    def filterIf(test: Boolean)(f: T => Expr[Boolean]): SelectQuery[T] =
        if test then filter(f) else this

    def withFilter(f: T => Expr[Boolean]): SelectQuery[T] =
        filter(f)

    def map[N <: Tuple, V <: Tuple](f: T => NamedTuple[N, V])(using s: SelectItem[NamedTuple[N, V]]): SelectQuery[NamedTupleWrapper[N, V]] =
        val mappedItems = f(items)
        val selectItems = s.selectItems(mappedItems, 0)
        SelectQuery(NamedTupleWrapper(mappedItems.toTuple), ast.copy(select = selectItems))

    @targetName("mapAny")
    def map[R](f: T => R)(using s: SelectItem[R]): SelectQuery[R] =
        val mappedItems = f(items)
        val selectItems = s.selectItems(mappedItems, 0)
        SelectQuery(mappedItems, ast.copy(select = selectItems))

    private inline def joinClause[JT, R](joinType: SqlJoinType)(using s: SelectItem[R]): JoinQuery[R] =
        val joinTableName = tableNameMacro[JT]
        q.tableIndex += 1
        val joinAliasName = s"t${q.tableIndex}"
        val joinTable = Table(joinTableName, joinAliasName, tableMetaDataMacro[JT])
        val tables = (
            inline items match
                case x: Tuple => x ++ Tuple1(joinTable)
                case _ => (items, joinTable)
        ).asInstanceOf[R]
        val selectItems = s.selectItems(tables, 0)
        val sqlTable: Option[SqlTable.JoinTable] = ast.from.headOption.map: i =>
            SqlTable.JoinTable(
                i,
                joinType, 
                SqlTable.IdentTable(joinTableName, Some(joinAliasName)),
                None
            )
        JoinQuery(tables, sqlTable, ast.copy(select = selectItems, from = sqlTable.toList))

    private def joinQueryClause[N <: Tuple, V <: Tuple, R](joinType: SqlJoinType, query: Query[NamedTupleWrapper[N, V]])(using s: SelectItem[R], c: Option[WithContext]): JoinQuery[R] =
        q.tableIndex += 1
        val aliasName = c.map(_.alias).getOrElse(s"t${q.tableIndex}")
        val joinQuery = query
        val rightTable = NamedQuery(joinQuery, aliasName)
        val tables = (
            items match
                case x: Tuple => x ++ Tuple1(rightTable)
                case _ => (items, rightTable)
        ).asInstanceOf[R]
        val sqlTable: Option[SqlTable.JoinTable] = ast.from.headOption.map: i =>
            SqlTable.JoinTable(
                i,
                joinType,
                c match
                    case Some(WithContext(alias)) =>
                        SqlTable.IdentTable(alias, None)
                    case None =>
                        SqlTable.SubQueryTable(joinQuery.ast, false, SqlSubQueryAlias(aliasName))
                ,
                None
            )
        JoinQuery(tables, sqlTable, ast.copy(select = s.selectItems(tables, 0), from = sqlTable.toList))

    private def joinLateralClause[N <: Tuple, V <: Tuple, R](joinType: SqlJoinType, query: T => Query[NamedTupleWrapper[N, V]])(using s: SelectItem[R]): JoinQuery[R] =
        q.tableIndex += 1
        val aliasName = s"t${q.tableIndex}"
        val joinQuery = query(items)
        val rightTable = NamedQuery(joinQuery, aliasName)
        val tables = (
            items match
                case x: Tuple => x ++ Tuple1(rightTable)
                case _ => (items, rightTable)
        ).asInstanceOf[R]
        val sqlTable: Option[SqlTable.JoinTable] = ast.from.headOption.map: i =>
            SqlTable.JoinTable(
                i,
                joinType,
                SqlTable.SubQueryTable(joinQuery.ast, true, SqlSubQueryAlias(aliasName)),
                None
            )
        JoinQuery(tables, sqlTable, ast.copy(select = s.selectItems(tables, 0), from = sqlTable.toList))

    private inline def joinListClause[L <: Product, J <: Tuple, R](joinType: SqlJoinType, list: List[L])(using s: SelectItem[R]): JoinQuery[R] =
        q.tableIndex += 1
        val aliasName = s"t${q.tableIndex}"
        val instances = AsSqlExpr.summonInstances[DropNames[From[L]]]
        val head = list.head
        val terms = head.productIterator.toList
        val selectItems = terms.zip(instances).zip(terms.indices).map: (pair, i) =>
            SqlSelectItem(pair._2.asInstanceOf[AsSqlExpr[Any]].asSqlExpr(pair._1), Some(s"c$i"))
        val columns = (
            inline erasedValue[J] match
                case _: Tuple => Tuple.fromArray(selectItems.map(i => Column(aliasName, i.alias.get)).toArray)
                case _ => Column(aliasName, selectItems.head.alias.get)
        ).asInstanceOf[J]
        val valuesAst = SqlQuery.Values(
            list.map: v =>
                val terms = v.productIterator.toList
                terms.zip(instances).map: (term, instance) =>
                    instance.asInstanceOf[AsSqlExpr[Any]].asSqlExpr(term)
        )
        val values = new Query[NamedTupleWrapper[Names[From[L]], J]](NamedTupleWrapper(columns), valuesAst)
        val rightTable = NamedQuery(values, aliasName)
        val tables = (
            inline items match
                case x: Tuple => x ++ Tuple1(rightTable)
                case _ => (items, rightTable)
        ).asInstanceOf[R]
        val sqlTable: Option[SqlTable.JoinTable] = ast.from.headOption.map: i =>
            SqlTable.JoinTable(
                i,
                joinType,
                SqlTable.SubQueryTable(valuesAst, false, SqlSubQueryAlias(aliasName, selectItems.map(i => i.alias.get))),
                None
            )
        JoinQuery(tables, sqlTable, ast.copy(select = s.selectItems(tables, 0), from = sqlTable.toList))

    inline def join[R](using SelectItem[InnerJoin[T, R]]): JoinQuery[InnerJoin[T, R]] =
        joinClause[R, InnerJoin[T, R]](SqlJoinType.InnerJoin)

    inline def join[N <: Tuple, V <: Tuple](query: Query[NamedTupleWrapper[N, V]])(using s: SelectItem[InnerJoinQuery[T, V, N]], a: AsExpr[V], c: Option[WithContext] = None): JoinQuery[InnerJoinQuery[T, V, N]] =
        joinQueryClause[N, V, InnerJoinQuery[T, V, N]](SqlJoinType.InnerJoin, query)

    inline def join[N <: Tuple, V <: Tuple](query: T => Query[NamedTupleWrapper[N, V]])(using s: SelectItem[InnerJoinQuery[T, V, N]], a: AsExpr[V]): JoinQuery[InnerJoinQuery[T, V, N]] =
        joinLateralClause[N, V, InnerJoinQuery[T, V, N]](SqlJoinType.InnerJoin, query)

    inline def join[L <: Product](list: List[L])(using SelectItem[InnerJoinQuery[T, Tuple.Map[DropNames[From[L]], Expr], Names[From[L]]]]): JoinQuery[InnerJoinQuery[T, Tuple.Map[DropNames[From[L]], Expr], Names[From[L]]]] =
        joinListClause[L, Tuple.Map[DropNames[From[L]], Expr], InnerJoinQuery[T, Tuple.Map[DropNames[From[L]], Expr], Names[From[L]]]](SqlJoinType.InnerJoin, list)

    inline def leftJoin[R](using SelectItem[LeftJoin[T, R]]): JoinQuery[LeftJoin[T, R]] =
        joinClause[R, LeftJoin[T, R]](SqlJoinType.LeftJoin)

    inline def leftJoin[N <: Tuple, V <: Tuple](query: Query[NamedTupleWrapper[N, V]])(using s: SelectItem[LeftJoinQuery[T, V, N]], a: AsExpr[V], c: Option[WithContext] = None): JoinQuery[LeftJoinQuery[T, V, N]] =
        joinQueryClause[N, V, LeftJoinQuery[T, V, N]](SqlJoinType.LeftJoin, query)

    inline def leftJoin[N <: Tuple, V <: Tuple](query: T => Query[NamedTupleWrapper[N, V]])(using s: SelectItem[LeftJoinQuery[T, V, N]], a: AsExpr[V]): JoinQuery[LeftJoinQuery[T, V, N]] =
        joinLateralClause[N, V, LeftJoinQuery[T, V, N]](SqlJoinType.LeftJoin, query)

    inline def leftJoin[L <: Product](list: List[L])(using SelectItem[LeftJoinQuery[T, Tuple.Map[DropNames[From[L]], Expr], Names[From[L]]]]): JoinQuery[LeftJoinQuery[T, Tuple.Map[DropNames[From[L]], Expr], Names[From[L]]]] =
        joinListClause[L, Tuple.Map[DropNames[From[L]], Expr], LeftJoinQuery[T, Tuple.Map[DropNames[From[L]], Expr], Names[From[L]]]](SqlJoinType.LeftJoin, list)

    inline def rightJoin[R](using SelectItem[RightJoin[T, R]]): JoinQuery[RightJoin[T, R]] =
        joinClause[R, RightJoin[T, R]](SqlJoinType.RightJoin)

    inline def rightJoin[N <: Tuple, V <: Tuple](query: Query[NamedTupleWrapper[N, V]])(using s: SelectItem[RightJoinQuery[T, V, N]], a: AsExpr[V], c: Option[WithContext] = None): JoinQuery[RightJoinQuery[T, V, N]] =
        joinQueryClause[N, V, RightJoinQuery[T, V, N]](SqlJoinType.RightJoin, query)

    inline def rightJoin[N <: Tuple, V <: Tuple](query: T => Query[NamedTupleWrapper[N, V]])(using s: SelectItem[RightJoinQuery[T, V, N]], a: AsExpr[V]): JoinQuery[RightJoinQuery[T, V, N]] =
        joinLateralClause[N, V, RightJoinQuery[T, V, N]](SqlJoinType.RightJoin, query)

    inline def rightJoin[L <: Product](list: List[L])(using SelectItem[RightJoinQuery[T, Tuple.Map[DropNames[From[L]], Expr], Names[From[L]]]]): JoinQuery[RightJoinQuery[T, Tuple.Map[DropNames[From[L]], Expr], Names[From[L]]]] =
        joinListClause[L, Tuple.Map[DropNames[From[L]], Expr], RightJoinQuery[T, Tuple.Map[DropNames[From[L]], Expr], Names[From[L]]]](SqlJoinType.RightJoin, list)

    inline def fullJoin[R](using s: SelectItem[FullJoin[T, R]]): JoinQuery[FullJoin[T, R]] =
        joinClause[R, FullJoin[T, R]](SqlJoinType.FullJoin)

    inline def fullJoin[N <: Tuple, V <: Tuple](query: Query[NamedTupleWrapper[N, V]])(using s: SelectItem[FullJoinQuery[T, V, N]], a: AsExpr[V], c: Option[WithContext] = None): JoinQuery[FullJoinQuery[T, V, N]] =
        joinQueryClause[N, V, FullJoinQuery[T, V, N]](SqlJoinType.FullJoin, query)

    inline def fullJoin[N <: Tuple, V <: Tuple](query: T => Query[NamedTupleWrapper[N, V]])(using s: SelectItem[FullJoinQuery[T, V, N]], a: AsExpr[V]): JoinQuery[FullJoinQuery[T, V, N]] =
        joinLateralClause[N, V, FullJoinQuery[T, V, N]](SqlJoinType.FullJoin, query)

    inline def fullJoin[L <: Product](list: List[L])(using SelectItem[FullJoinQuery[T, Tuple.Map[DropNames[From[L]], Expr], Names[From[L]]]]): JoinQuery[FullJoinQuery[T, Tuple.Map[DropNames[From[L]], Expr], Names[From[L]]]] =
        joinListClause[L, Tuple.Map[DropNames[From[L]], Expr], FullJoinQuery[T, Tuple.Map[DropNames[From[L]], Expr], Names[From[L]]]](SqlJoinType.FullJoin, list)

    def drop(n: Int): SelectQuery[T] =
        val sqlLimit = ast.limit.map(l => SqlLimit(l.limit, SqlExpr.NumberLiteral(n))).orElse(Some(SqlLimit(SqlExpr.NumberLiteral(1), SqlExpr.NumberLiteral(n))))
        new SelectQuery(items, ast.copy(limit = sqlLimit))

    def take(n: Int): SelectQuery[T] =
        val sqlLimit = ast.limit.map(l => SqlLimit(SqlExpr.NumberLiteral(n), l.offset)).orElse(Some(SqlLimit(SqlExpr.NumberLiteral(n), SqlExpr.NumberLiteral(0))))
        new SelectQuery(items, ast.copy(limit = sqlLimit))

    def distinct: SelectQuery[T] =
        new SelectQuery(items, ast.copy(param = Some(SqlSelectParam.Distinct)))

    def sortBy(f: T => OrderBy): SelectQuery[T] =
        val orderBy = f(items)
        val sqlOrderBy = orderBy.asSqlOrderBy
        new SelectQuery(items, ast.copy(orderBy = ast.orderBy :+ sqlOrderBy))

    def groupBy[G](f: T => G)(using a: AsExpr[G]): GroupByQuery[(G, T)] =
        val groupByItems = f(items)
        val sqlGroupBy = a.asExprs(groupByItems).map(_.asSqlExpr)
        GroupByQuery((groupByItems, items), ast.copy(groupBy = sqlGroupBy))

    def size: SelectQuery[Expr[Long]] = 
        val expr = count()
        ast match
            case SqlQuery.Select(_, _, _, _, Nil, _, _, _, _) =>
                SelectQuery(expr, ast.copy(select = SqlSelectItem(expr.asSqlExpr, None) :: Nil, limit = None))
            case _ =>
                val outerQuery: SqlQuery.Select = SqlQuery.Select(
                    select = SqlSelectItem(expr.asSqlExpr, None) :: Nil,
                    from = SqlTable.SubQueryTable(ast, false, SqlSubQueryAlias("t0")) :: Nil
                )
                SelectQuery(expr, outerQuery)

    def exists: SelectQuery[Expr[Boolean]] =
        val expr = sqala.dsl.exists(this)
        val outerQuery: SqlQuery.Select = SqlQuery.Select(
            select = SqlSelectItem(expr.asSqlExpr, None) :: Nil,
            from = Nil
        )
        SelectQuery(expr, outerQuery)

class UnionQuery[T](
    private[sqala] val left: Query[?],
    private[sqala] val unionType: SqlUnionType,
    private[sqala] val right: SqlQuery
) extends Query[T](left.queryItems.asInstanceOf[T], SqlQuery.Union(left.ast, unionType, right))