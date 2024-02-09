package sqala.dsl.statement.query

import sqala.ast.limit.SqlLimit
import sqala.ast.order.SqlOrderBy
import sqala.ast.statement.{SqlQuery, SqlSelectItem, SqlSelectParam, SqlUnionType}
import sqala.ast.table.{SqlJoinCondition, SqlJoinType, SqlSubQueryAlias, SqlTable}
import sqala.dsl.*
import sqala.dsl.`macro`.{tableMetaDataMacro, tableNameMacro}

import scala.Tuple.Elem
import scala.annotation.targetName
import scala.compiletime.erasedValue

sealed abstract class Query[T](private[sqala] val queryItems: T):
    def ast: SqlQuery

    private inline def unionListClause(unionType: SqlUnionType, list: List[InverseMap[T, Expr]]): Query[T] =
        val instances = AsSqlExpr.summonInstances[InverseMap[T, Expr]]
        val values = SqlQuery.Values(
            list.map: v =>
                val terms = inline v match
                    case t: Tuple => t.toList
                    case _ => v :: Nil
                terms.zip(instances).map: (term, instance) =>
                    instance.asInstanceOf[AsSqlExpr[Any]].asSqlExpr(term)
        )
        UnionQuery(this, unionType, values)

    infix inline def union[R](query: Query[R]): Query[Union[T, R]] = UnionQuery(this, SqlUnionType.Union, query.ast)

    infix inline def unionList(list: List[InverseMap[T, Expr]]): Query[T] =
        unionListClause(SqlUnionType.Union, list)

    infix inline def unionAll[R](query: Query[R]): Query[Union[T, R]] = UnionQuery(this, SqlUnionType.UnionAll, query.ast)

    infix inline def unionAllList(list: List[InverseMap[T, Expr]]): Query[T] =
        unionListClause(SqlUnionType.UnionAll, list)

    inline def ++[R](query: Query[R]): Query[Union[T, R]] = UnionQuery(this, SqlUnionType.UnionAll, query.ast)

    infix inline def except[R](query: Query[R]): Query[Union[T, R]] = UnionQuery(this, SqlUnionType.Except, query.ast)

    infix inline def exceptList(list: List[InverseMap[T, Expr]]): Query[T] =
        unionListClause(SqlUnionType.Except, list)

    infix inline def exceptAll[R](query: Query[R]): Query[Union[T, R]] = UnionQuery(this, SqlUnionType.ExceptAll, query.ast)

    infix inline def exceptAllList(list: List[InverseMap[T, Expr]]): Query[T] =
        unionListClause(SqlUnionType.ExceptAll, list)

    infix inline def intersect[R](query: Query[R]): Query[Union[T, R]] = UnionQuery(this, SqlUnionType.Intersect, query.ast)

    infix inline def intersectList(list: List[InverseMap[T, Expr]]): Query[T] =
        unionListClause(SqlUnionType.Intersect, list)

    infix inline def intersectAll[R](query: Query[R]): Query[Union[T, R]] = UnionQuery(this, SqlUnionType.IntersectAll, query.ast)

    infix inline def intersectAllList(list: List[InverseMap[T, Expr]]): Query[T] =
        unionListClause(SqlUnionType.IntersectAll, list)

object Query:
    extension [T](query: Query[Expr[T]])
        def asExpr: Expr[T] = SubQuery(query)

opaque type Depth = Int

object Depth:
    def apply(n: Int): Depth = n

    extension (depth: Depth)
        def asInt: Int = depth

class NamedQuery[T](private[sqala] val query: Query[T], private[sqala] val alias: String):
    def apply(n: Int)(using s: SelectItem[T]): Elem[AsTuple[T], n.type] =
        val item = s.selectItems(query.queryItems, 0)(n)
        Column(alias, item.alias.get).asInstanceOf[Elem[AsTuple[T], n.type]]

class SelectQuery[T](
    private[sqala] val depth: Int,
    private[sqala] val lastIndex: Int,
    private[sqala] val items: T,
    val ast: SqlQuery.Select
) extends Query[T](items):
    def filter(f: T => Expr[Boolean]): SelectQuery[T] =
        val condition = f(items).asSqlExpr
        SelectQuery(depth, lastIndex, items, ast.addWhere(condition))

    def filterIf(test: Boolean)(f: T => Expr[Boolean]): SelectQuery[T] =
        if test then filter(f) else this

    def filterNot(test: Boolean)(f: T => Expr[Boolean]): SelectQuery[T] =
        if !test then filter(f) else this

    def withFilter(f: T => Expr[Boolean]): SelectQuery[T] = filter(f)

    @targetName("withFilterBoolean")
    def withFilter(f: T => Boolean): SelectQuery[T] = this

    def map[R](f: T => R)(using s: SelectItem[R]): SelectQuery[QueryMap[R]] =
        val mappedItems = f(items)
        val selectItems = s.selectItems(mappedItems, 0)
        SelectQuery(depth, lastIndex, mappedItems.asInstanceOf[QueryMap[R]], ast.copy(select = selectItems))

    private inline def joinClause[JT, R](joinType: SqlJoinType)(using s: SelectItem[R]): JoinQuery[R] =
        val joinTableName = tableNameMacro[JT]
        val joinAliasName = s"d${depth}_t${lastIndex + 1}"
        val joinTable = Table(joinTableName, joinAliasName, tableMetaDataMacro[JT], depth)
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
        JoinQuery(depth, lastIndex + 1, tables, sqlTable, ast.copy(select = selectItems, from = sqlTable.toList))

    private inline def joinClause[JT, R](joinType: SqlJoinType, query: Depth ?=> Query[JT])(using s: SelectItem[R], c: Option[WithRecursiveContext]): JoinQuery[R] =
        given d: Depth = Depth(depth + 1)
        val aliasName = c.map(_.alias).getOrElse(s"d${d.asInt - 1}_t${lastIndex + 1}")
        val joinQuery = query
        val rightTable = NamedQuery(joinQuery, aliasName)
        val tables = (
            inline items match
                case x: Tuple => x ++ Tuple1(rightTable)
                case _ => (items, rightTable)
        ).asInstanceOf[R]
        val sqlTable: Option[SqlTable.JoinTable] = ast.from.headOption.map: i =>
            SqlTable.JoinTable(
                i,
                joinType,
                c match
                    case Some(WithRecursiveContext(alias)) =>
                        SqlTable.IdentTable(alias, None)
                    case None =>
                        SqlTable.SubQueryTable(joinQuery.ast, false, SqlSubQueryAlias(aliasName))
                ,
                None
            )
        JoinQuery(depth, lastIndex + 1, tables, sqlTable, ast.copy(select = s.selectItems(tables, 0), from = sqlTable.toList))

    private inline def joinLateralClause[JT, R](joinType: SqlJoinType, query: Depth ?=> T => Query[JT])(using s: SelectItem[R]): JoinQuery[R] =
        given d: Depth = Depth(depth + 1)
        val aliasName = s"d${d.asInt - 1}_t${lastIndex + 1}"
        val joinQuery = query(items)
        val rightTable = NamedQuery(joinQuery, aliasName)
        val tables = (
            inline items match
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
        JoinQuery(depth, lastIndex + 1, tables, sqlTable, ast.copy(select = s.selectItems(tables, 0), from = sqlTable.toList))

    private inline def joinListClause[LT, JT, R](joinType: SqlJoinType, list: Depth ?=> List[LT])(using s: SelectItem[R]): JoinQuery[R] =
        given d: Depth = Depth(depth + 1)
        val aliasName = s"d${d.asInt - 1}_t${lastIndex + 1}"
        val instances = AsSqlExpr.summonInstances[LT]
        val head = list.head
        val terms = inline head match
            case t: Tuple => t.toList
            case _ => head :: Nil
        val selectItems = terms.zip(instances).zip(terms.indices).map: (pair, i) =>
            SqlSelectItem(pair._2.asInstanceOf[AsSqlExpr[Any]].asSqlExpr(pair._1), Some(s"c$i"))
        val columns = (
            inline erasedValue[JT] match
                case _: Tuple => Tuple.fromArray(selectItems.map(i => Column(aliasName, i.alias.get)).toArray)
                case _ => Column(aliasName, selectItems.head.alias.get)
        ).asInstanceOf[JT]
        val valuesAst = SqlQuery.Values(
            list.map: v =>
                val terms = inline v match
                    case t: Tuple => t.toList
                    case _ => v :: Nil
                terms.zip(instances).map: (term, instance) =>
                    instance.asInstanceOf[AsSqlExpr[Any]].asSqlExpr(term)
        )
        val values = new Query[JT](columns):
            override def ast: SqlQuery = valuesAst
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
        JoinQuery(depth, lastIndex + 1, tables, sqlTable, ast.copy(select = s.selectItems(tables, 0), from = sqlTable.toList))
        
    inline def join[R](using SelectItem[InnerJoin[T, R]]): JoinQuery[InnerJoin[T, R]] =
        joinClause[R, InnerJoin[T, R]](SqlJoinType.InnerJoin)

    inline def join[R: AsExpr](query: Depth ?=> Query[R])(using s: SelectItem[InnerJoinQuery[T, R]], c: Option[WithRecursiveContext] = None): JoinQuery[InnerJoinQuery[T, R]] =
        joinClause[R, InnerJoinQuery[T, R]](SqlJoinType.InnerJoin, query)

    inline def joinLateral[R: AsExpr](query: Depth ?=> T => Query[R])(using SelectItem[InnerJoinQuery[T, R]]): JoinQuery[InnerJoinQuery[T, R]] =
        joinLateralClause[R, InnerJoinQuery[T, R]](SqlJoinType.InnerJoin, query)

    inline def joinList[R](list: Depth ?=> List[R])(using SelectItem[InnerJoinQuery[T, Map[R, Expr]]]): JoinQuery[InnerJoinQuery[T, Map[R, Expr]]] =
        joinListClause[R, Map[R, Expr], InnerJoinQuery[T, Map[R, Expr]]](SqlJoinType.InnerJoin, list)

    inline def leftJoin[R](using SelectItem[LeftJoin[T, R]]): JoinQuery[LeftJoin[T, R]] =
        joinClause[R, LeftJoin[T, R]](SqlJoinType.LeftJoin)

    inline def leftJoin[R: AsExpr](query: Depth ?=> Query[R])(using s: SelectItem[LeftJoinQuery[T, R]], c: Option[WithRecursiveContext] = None): JoinQuery[LeftJoinQuery[T, R]] =
        joinClause[R, LeftJoinQuery[T, R]](SqlJoinType.LeftJoin, query)

    inline def leftJoinLateral[R: AsExpr](query: Depth ?=> T => Query[R])(using SelectItem[LeftJoinQuery[T, R]]): JoinQuery[LeftJoinQuery[T, R]] =
        joinLateralClause[R, LeftJoinQuery[T, R]](SqlJoinType.LeftJoin, query)

    inline def leftJoinList[R](list: Depth ?=> List[R])(using SelectItem[LeftJoinQuery[T, Map[R, Expr]]]): JoinQuery[LeftJoinQuery[T, Map[R, Expr]]] =
        joinListClause[R, Map[R, Expr], LeftJoinQuery[T, Map[R, Expr]]](SqlJoinType.LeftJoin, list)

    inline def rightJoin[R](using SelectItem[RightJoin[T, R]]): JoinQuery[RightJoin[T, R]] =
        joinClause[R, RightJoin[T, R]](SqlJoinType.RightJoin)

    inline def rightJoin[R: AsExpr](query: Depth ?=> Query[R])(using s: SelectItem[RightJoinQuery[T, R]], c: Option[WithRecursiveContext] = None): JoinQuery[RightJoinQuery[T, R]] =
        joinClause[R, RightJoinQuery[T, R]](SqlJoinType.RightJoin, query)

    inline def rightJoinLateral[R: AsExpr](query: Depth ?=> T => Query[R])(using SelectItem[RightJoinQuery[T, R]]): JoinQuery[RightJoinQuery[T, R]] =
        joinLateralClause[R, RightJoinQuery[T, R]](SqlJoinType.RightJoin, query)

    inline def rightJoinList[R](list: Depth ?=> List[R])(using SelectItem[RightJoinQuery[T, Map[R, Expr]]]): JoinQuery[RightJoinQuery[T, Map[R, Expr]]] =
        joinListClause[R, Map[R, Expr], RightJoinQuery[T, Map[R, Expr]]](SqlJoinType.RightJoin, list)

    inline def fullJoin[R](using s: SelectItem[FullJoin[T, R]]): JoinQuery[FullJoin[T, R]] =
        joinClause[R, FullJoin[T, R]](SqlJoinType.FullJoin)

    inline def fullJoin[R: AsExpr](query: Depth ?=> Query[R])(using s: SelectItem[FullJoinQuery[T, R]], c: Option[WithRecursiveContext] = None): JoinQuery[FullJoinQuery[T, R]] =
        joinClause[R, FullJoinQuery[T, R]](SqlJoinType.FullJoin, query)

    inline def fullJoinLateral[R: AsExpr](query: Depth ?=> T => Query[R])(using SelectItem[FullJoinQuery[T, R]]): JoinQuery[FullJoinQuery[T, R]] =
        joinLateralClause[R, FullJoinQuery[T, R]](SqlJoinType.FullJoin, query)

    inline def fullJoinList[R](list: Depth ?=> List[R])(using SelectItem[FullJoinQuery[T, Map[R, Expr]]]): JoinQuery[FullJoinQuery[T, Map[R, Expr]]] =
        joinListClause[R, Map[R, Expr], FullJoinQuery[T, Map[R, Expr]]](SqlJoinType.FullJoin, list)

    def drop(n: Int): SelectQuery[T] =
        val sqlLimit = ast.limit.map(l => SqlLimit(l.limit, n)).orElse(Some(SqlLimit(1, n)))
        new SelectQuery(depth, lastIndex, items, ast.copy(limit = sqlLimit))

    def take(n: Int): SelectQuery[T] =
        val sqlLimit = ast.limit.map(l => SqlLimit(n, l.offset)).orElse(Some(SqlLimit(n, 0)))
        new SelectQuery(depth, lastIndex, items, ast.copy(limit = sqlLimit))

    def distinct: SelectQuery[T] =
        new SelectQuery(depth, lastIndex, items, ast.copy(param = Some(SqlSelectParam.Distinct)))

    def sortBy(f: T => OrderBy): SelectQuery[T] =
        val orderBy = f(items)
        val sqlOrderBy = SqlOrderBy(orderBy.expr.asSqlExpr, Some(orderBy.order))
        new SelectQuery(depth, lastIndex, items, ast.copy(orderBy = ast.orderBy :+ sqlOrderBy))

    def groupBy[G](f: T => G)(using a: AsExpr[G]): GroupByQuery[(G, T)] =
        val groupByItems = f(items)
        val sqlGroupBy = a.asExprs(groupByItems).map(_.asSqlExpr)
        GroupByQuery(depth, lastIndex, (groupByItems, items), ast.copy(groupBy = sqlGroupBy))

    def size: SelectQuery[Expr[Long]] = 
        val expr = count()
        ast match
            case SqlQuery.Select(_, _, _, _, Nil, _, _, _, _) =>
                SelectQuery(depth, lastIndex, expr, ast.copy(select = SqlSelectItem(expr.asSqlExpr, None) :: Nil, limit = None))
            case _ =>
                val outerQuery: SqlQuery.Select = SqlQuery.Select(
                    select = SqlSelectItem(expr.asSqlExpr, None) :: Nil,
                    from = SqlTable.SubQueryTable(ast, false, SqlSubQueryAlias("t0")) :: Nil
                )
                SelectQuery(depth, lastIndex, expr, outerQuery)

    def exists: SelectQuery[Expr[Boolean]] =
        val expr = sqala.dsl.exists(this)
        val outerQuery: SqlQuery.Select = SqlQuery.Select(
            select = SqlSelectItem(expr.asSqlExpr, None) :: Nil,
            from = Nil
        )
        SelectQuery(depth, lastIndex, expr, outerQuery)

class JoinQuery[T](
    private[sqala] val depth: Int,
    private[sqala] val lastIndex: Int,
    private[sqala] val tables: T,
    private[sqala] val table: Option[SqlTable.JoinTable],
    private[sqala] val ast: SqlQuery.Select
):
    def on(f: T => Expr[Boolean]): SelectQuery[T] =
        val sqlCondition = f(tables).asSqlExpr
        val sqlTable = table.map(_.copy(condition = Some(SqlJoinCondition.On(sqlCondition))))
        SelectQuery(depth, lastIndex, tables, ast.copy(from = sqlTable.toList))

class GroupByQuery[T](
    private[sqala] val depth: Int,
    private[sqala] val lastIndex: Int,
    private[sqala] val tables: T,
    private[sqala] val ast: SqlQuery.Select
):
    def map[R](f: T => R)(using s: SelectItem[R]): SelectQuery[R] =
        val mappedItems = f(tables)
        val selectItems = s.selectItems(mappedItems, 0)
        SelectQuery(depth, lastIndex, mappedItems, ast.copy(select = selectItems))

    def having(f: T => Expr[Boolean]): GroupByQuery[T] =
        val sqlCondition = f(tables).asSqlExpr
        GroupByQuery(depth, lastIndex, tables, ast.addHaving(sqlCondition))

class UnionQuery[T](
    private[sqala] val left: Query[?],
    private[sqala] val unionType: SqlUnionType,
    private[sqala] val right: SqlQuery
) extends Query[T](left.queryItems.asInstanceOf[T]):
    override def ast: SqlQuery = SqlQuery.Union(left.ast, unionType, right)
