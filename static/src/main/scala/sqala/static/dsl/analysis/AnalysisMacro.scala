package sqala.static.dsl.analysis

import sqala.ast.expr.*
import sqala.ast.group.SqlGroupItem
import sqala.ast.limit.SqlLimit
import sqala.ast.order.{SqlOrderItem, SqlOrderNullsOption, SqlOrderOption}
import sqala.ast.param.SqlParam
import sqala.ast.statement.{SqlQuery, SqlSelectItem, SqlUnionType, SqlWithItem}
import sqala.ast.table.{SqlJoinCondition, SqlJoinType, SqlTable, SqlTableAlias}
import sqala.metadata.{AsSqlExpr, TableMacroImpl}
import sqala.printer.*
import sqala.static.dsl.Table
import sqala.static.dsl.statement.query.{Query, SubQuery}
import sqala.util.queryToString

import scala.NamedTuple.NamedTuple
import scala.quoted.{Expr, Quotes, Type}

private[sqala] object AnalysisMacro:
    inline def showQuery[D <: Dialect](inline query: Query[?]): Unit =
        ${ AnalysisMacroImpl.showQuery[D]('query) }

    inline def showSizeQuery[D <: Dialect](inline query: Query[?]): Unit =
        ${ AnalysisMacroImpl.showSizeQuery[D]('query) }

    inline def showExistsQuery[D <: Dialect](inline query: Query[?]): Unit =
        ${ AnalysisMacroImpl.showExistsQuery[D]('query) }

    inline def showLimitQuery[D <: Dialect](inline query: Query[?]): Unit =
        ${ AnalysisMacroImpl.showLimitQuery[D]('query) }

    inline def showPageQuery[D <: Dialect](inline query: Query[?]): Unit =
        ${ AnalysisMacroImpl.showPageQuery[D]('query) }

private[sqala] object AnalysisMacroImpl:
    case class ExprInfo(
        expr: SqlExpr,
        hasAgg: Boolean,
        isAgg: Boolean,
        hasWindow: Boolean,
        isValue: Boolean,
        ungroupedPaths: List[List[String]],
        notInAggPaths: List[List[String]],
        code: String = ""
    )

    case class GroupInfo(
        currentTables: List[String],
        groupedExprs: List[SqlExpr]
    )

    enum ResultSize:
        case ZeroOrOne
        case Many

    case class QueryContext(var tableIndex: Int)

    case class QueryInfo(
        tree: SqlQuery, 
        currentTableAlias: List[String],
        table: SqlTable, 
        selectItems: List[SqlSelectItem],
        allTables: List[(argName: String, tableName: String)],
        groupInfo: List[GroupInfo],
        resultSize: ResultSize,
        inConnectBy: Boolean = false
    )

    def removeLimitAndOrderBy(tree: SqlQuery): SqlQuery = 
        tree match
            case s: SqlQuery.Select => s.copy(limit = None, orderBy = Nil)
            case u: SqlQuery.Union => u.copy(limit = None)
            case c: SqlQuery.Cte => c.copy(query = removeLimitAndOrderBy(c.query))
            case _ => tree

    def showQuery[D <: Dialect : Type](query: Expr[Query[?]])(using q: Quotes): Expr[Unit] =
        import q.reflect.*

        val term = removeEmptyInline(query.asTerm)

        val queryTerm = term match
            case Inlined(_, _, Typed(Block(_, Block(_, Inlined(_, _, t))), _)) =>
                Some(t)
            case _ =>
                None

        for q <- queryTerm do
            given QueryContext = QueryContext(0)

            val tree = try
                Some(createTree(q, Nil, Nil).tree)
            catch 
                case e: Exception => 
                    None

            val dialect: Option[Dialect] = Type.of[D] match
                case '[MysqlDialect.type] => Some(MysqlDialect)
                case '[PostgresqlDialect.type] => Some(PostgresqlDialect)
                case '[OracleDialect.type] => Some(OracleDialect)
                case '[SqliteDialect.type] => Some(SqliteDialect)
                case '[H2Dialect.type] => Some(H2Dialect)
                case '[MssqlDialect.type] => Some(MssqlDialect)
                case '[DB2Dialect.type] => Some(DB2Dialect)
                case _ => None

            for 
                t <- tree
                d <- dialect
            do
                report.info(queryToString(t, d, true)._1)

        '{ () }

    def showSizeQuery[D <: Dialect : Type](query: Expr[Query[?]])(using q: Quotes): Expr[Unit] =
        import q.reflect.*

        val term = removeEmptyInline(query.asTerm)

        val queryTerm = term match
            case Inlined(_, _, Typed(Block(_, Block(_, Inlined(_, _, t))), _)) =>
                Some(t)
            case _ =>
                None

        for q <- queryTerm do
            given QueryContext = QueryContext(0)

            val tree = try
                val queryTree = createTree(q, Nil, Nil).tree
                val sizeTree = queryTree match
                    case s@SqlQuery.Select(p, _, _, _, Nil, _, _, _) 
                        if p != Some(SqlParam.Distinct) 
                    =>
                        s.copy(
                            select = 
                                SqlSelectItem.Item(SqlExpr.Func("COUNT", Nil), None) :: Nil, 
                            limit = None,
                            orderBy = Nil
                        )
                    case _ =>
                        SqlQuery.Select(
                            select = SqlSelectItem.Item(SqlExpr.Func("COUNT", Nil), None) :: Nil,
                            from = SqlTable.SubQuery(removeLimitAndOrderBy(queryTree), false, Some(SqlTableAlias("t"))) :: Nil
                        )
                Some(sizeTree)
            catch 
                case e: Exception => 
                    None

            val dialect: Option[Dialect] = Type.of[D] match
                case '[MysqlDialect.type] => Some(MysqlDialect)
                case '[PostgresqlDialect.type] => Some(PostgresqlDialect)
                case '[OracleDialect.type] => Some(OracleDialect)
                case '[SqliteDialect.type] => Some(SqliteDialect)
                case '[H2Dialect.type] => Some(H2Dialect)
                case '[MssqlDialect.type] => Some(MssqlDialect)
                case '[DB2Dialect.type] => Some(DB2Dialect)
                case _ => None
                
            for 
                t <- tree
                d <- dialect
            do
                report.info(queryToString(t, d, true)._1)

        '{ () }

    def showExistsQuery[D <: Dialect : Type](query: Expr[Query[?]])(using q: Quotes): Expr[Unit] =
        import q.reflect.*

        val term = removeEmptyInline(query.asTerm)

        val queryTerm = term match
            case Inlined(_, _, Typed(Block(_, Block(_, Inlined(_, _, t))), _)) =>
                Some(t)
            case _ =>
                None

        for q <- queryTerm do
            given QueryContext = QueryContext(0)

            val tree = try
                val queryTree = createTree(q, Nil, Nil).tree
                val sizeTree =
                    val expr = SqlExpr.SubLink(queryTree, SqlSubLinkType.Exists)
                    SqlQuery.Select(
                        select = SqlSelectItem.Item(expr, None) :: Nil,
                        from = Nil
                    )
                Some(sizeTree)
            catch 
                case e: Exception => 
                    None

            val dialect: Option[Dialect] = Type.of[D] match
                case '[MysqlDialect.type] => Some(MysqlDialect)
                case '[PostgresqlDialect.type] => Some(PostgresqlDialect)
                case '[OracleDialect.type] => Some(OracleDialect)
                case '[SqliteDialect.type] => Some(SqliteDialect)
                case '[H2Dialect.type] => Some(H2Dialect)
                case '[MssqlDialect.type] => Some(MssqlDialect)
                case '[DB2Dialect.type] => Some(DB2Dialect)
                case _ => None
                
            for 
                t <- tree
                d <- dialect
            do
                report.info(queryToString(t, d, true)._1)

        '{ () }

    def showLimitQuery[D <: Dialect : Type](query: Expr[Query[?]])(using q: Quotes): Expr[Unit] =
        import q.reflect.*

        val term = removeEmptyInline(query.asTerm)

        val queryTerm = term match
            case Inlined(_, _, Typed(Block(_, Block(_, Inlined(_, _, t))), _)) =>
                Some(t)
            case _ =>
                None

        for q <- queryTerm do
            given QueryContext = QueryContext(0)

            val tree = try
                val queryTree = createTree(q, Nil, Nil).tree
                val limitTree = queryTree match
                    case s: SqlQuery.Select =>
                        s.copy(limit = Some(SqlLimit(SqlExpr.NumberLiteral(1), SqlExpr.NumberLiteral(1))))
                    case c: SqlQuery.Cte =>
                        c.copy(
                            query = 
                                c.query.asInstanceOf[SqlQuery.Select]
                                .copy(limit = Some(SqlLimit(SqlExpr.NumberLiteral(1), SqlExpr.NumberLiteral(1))))
                            )
                    case u: SqlQuery.Union =>
                        u.copy(limit = Some(SqlLimit(SqlExpr.NumberLiteral(1), SqlExpr.NumberLiteral(1))))
                    case _ => throw MatchError(())
                Some(limitTree)
            catch 
                case e: Exception => 
                    None

            val dialect: Option[Dialect] = Type.of[D] match
                case '[MysqlDialect.type] => Some(MysqlDialect)
                case '[PostgresqlDialect.type] => Some(PostgresqlDialect)
                case '[OracleDialect.type] => Some(OracleDialect)
                case '[SqliteDialect.type] => Some(SqliteDialect)
                case '[H2Dialect.type] => Some(H2Dialect)
                case '[MssqlDialect.type] => Some(MssqlDialect)
                case '[DB2Dialect.type] => Some(DB2Dialect)
                case _ => None
                
            for 
                t <- tree
                d <- dialect
            do
                report.info(queryToString(t, d, true)._1)

        '{ () }

    def showPageQuery[D <: Dialect : Type](query: Expr[Query[?]])(using q: Quotes): Expr[Unit] =
        import q.reflect.*

        val term = removeEmptyInline(query.asTerm)

        val queryTerm = term match
            case Inlined(_, _, Typed(Block(_, Block(_, Inlined(_, _, t))), _)) =>
                Some(t)
            case _ =>
                None

        for q <- queryTerm do
            given QueryContext = QueryContext(0)

            val sizeTree = try
                val queryTree = createTree(q, Nil, Nil).tree
                val tree = queryTree match
                    case s@SqlQuery.Select(p, _, _, _, Nil, _, _, _) 
                        if p != Some(SqlParam.Distinct) 
                    =>
                        s.copy(
                            select = 
                                SqlSelectItem.Item(SqlExpr.Func("COUNT", Nil), None) :: Nil, 
                            limit = None,
                            orderBy = Nil
                        )
                    case _ =>
                        SqlQuery.Select(
                            select = SqlSelectItem.Item(SqlExpr.Func("COUNT", Nil), None) :: Nil,
                            from = SqlTable.SubQuery(removeLimitAndOrderBy(queryTree), false, Some(SqlTableAlias("t"))) :: Nil
                        )
                Some(tree)
            catch 
                case e: Exception => 
                    None

            val limitTree = try
                val queryTree = createTree(q, Nil, Nil).tree
                val tree = queryTree match
                    case s: SqlQuery.Select =>
                        s.copy(limit = Some(SqlLimit(SqlExpr.NumberLiteral(1), SqlExpr.NumberLiteral(1))))
                    case c: SqlQuery.Cte =>
                        c.copy(
                            query = 
                                c.query.asInstanceOf[SqlQuery.Select]
                                .copy(limit = Some(SqlLimit(SqlExpr.NumberLiteral(1), SqlExpr.NumberLiteral(1))))
                            )
                    case u: SqlQuery.Union =>
                        u.copy(limit = Some(SqlLimit(SqlExpr.NumberLiteral(1), SqlExpr.NumberLiteral(1))))
                    case _ => throw MatchError(())
                Some(tree)
            catch 
                case e: Exception => 
                    None

            val dialect: Option[Dialect] = Type.of[D] match
                case '[MysqlDialect.type] => Some(MysqlDialect)
                case '[PostgresqlDialect.type] => Some(PostgresqlDialect)
                case '[OracleDialect.type] => Some(OracleDialect)
                case '[SqliteDialect.type] => Some(SqliteDialect)
                case '[H2Dialect.type] => Some(H2Dialect)
                case '[MssqlDialect.type] => Some(MssqlDialect)
                case '[DB2Dialect.type] => Some(DB2Dialect)
                case _ => None
                
            for 
                st <- sizeTree
                lt <- limitTree
                d <- dialect
            do
                report.info(
                    queryToString(st, d, true)._1 + 
                    "\n\n" + 
                    queryToString(lt, d, true)._1
                )

        '{ () }
        
    def createTree(using q: Quotes, c: QueryContext)(
        term: q.reflect.Term,
        allTables: List[(argName: String, tableName: String)],
        groupInfo: List[GroupInfo]
    ): QueryInfo =
        import q.reflect.*

        term match
            case Apply(Apply(Apply(TypeApply(Apply(TypeApply(Select(_, "filter" | "where" | "withFilter"), _), query :: Nil), _), cond :: Nil), _), _) =>
                createWhereClause(query, cond, allTables, groupInfo)
            case Apply(Apply(TypeApply(Apply(TypeApply(Select(_, "map" | "select"), _), query :: Nil), _), select :: Nil), _) =>
                createSelectClause(query, select, allTables, groupInfo)
            case Apply(Apply(TypeApply(Apply(TypeApply(Select(_, "groupBy"), _), query :: Nil), _), group :: Nil), _) =>
                createGroupByClause(query, group, "", allTables, groupInfo)
            case Apply(Apply(TypeApply(Apply(TypeApply(Select(_, "groupByCube"), _), query :: Nil), _), group :: Nil), _) =>
                createGroupByClause(query, group, "cube", allTables, groupInfo)
            case Apply(Apply(TypeApply(Apply(TypeApply(Select(_, "groupByRollup"), _), query :: Nil), _), group :: Nil), _) =>
                createGroupByClause(query, group, "rollup", allTables, groupInfo)
            case Apply(Apply(TypeApply(Apply(TypeApply(Select(_, "groupBySets"), _), query :: Nil), _), group :: Nil), _) =>
                createGroupByClause(query, group, "sets", allTables, groupInfo)
            case Apply(Apply(TypeApply(Apply(TypeApply(Select(_, "sortBy" | "orderBy"), _), query :: Nil), _), order :: Nil), _) =>
                createOrderByClause(query, order, allTables, groupInfo)
            case Apply(TypeApply(Select(_, "distinct"), _), query :: Nil) =>
                createDistinctClause(query, allTables, groupInfo)
            case Apply(Apply(TypeApply(Select(_, "take" | "limit"), _), query :: Nil), n :: Nil) =>
                createLimitClause(query, n, allTables, groupInfo)
            case Apply(Apply(TypeApply(Select(_, "drop" | "offset"), _), query :: Nil), _) =>
                createOffsetClause(query, allTables, groupInfo)
            case Inlined(Some(Apply(TypeApply(Ident("from"), t :: Nil), _)), _, _) =>
                createFromTableClause(t, allTables, groupInfo)
            case Inlined(Some(Apply(Apply(TypeApply(Ident("from"), _), subquery :: Nil), _)), _, _) =>
                createFromSubqueryClause(subquery, allTables, groupInfo)
            case Apply(Apply(TypeApply(Apply(TypeApply(Select(Ident(_), "union"), _), left :: Nil), _), right :: Nil), _) =>
                createUnionClause(left, "union", right, allTables, groupInfo)
            case Apply(Apply(TypeApply(Apply(TypeApply(Select(Ident(_), "unionAll" | "++"), _), left :: Nil), _), right :: Nil), _) =>
                createUnionClause(left, "unionAll", right, allTables, groupInfo)
            case Apply(Apply(TypeApply(Apply(TypeApply(Select(Ident(_), "except"), _), left :: Nil), _), right :: Nil), _) =>
                createUnionClause(left, "except", right, allTables, groupInfo)
            case Apply(Apply(TypeApply(Apply(TypeApply(Select(Ident(_), "exceptAll"), _), left :: Nil), _), right :: Nil), _) =>
                createUnionClause(left, "exceptAll", right, allTables, groupInfo)
            case Apply(Apply(TypeApply(Apply(TypeApply(Select(Ident(_), "intersect"), _), left :: Nil), _), right :: Nil), _) =>
                createUnionClause(left, "intersect", right, allTables, groupInfo)
            case Apply(Apply(TypeApply(Apply(TypeApply(Select(Ident(_), "intersectAll"), _), left :: Nil), _), right :: Nil), _) =>
                createUnionClause(left, "intersectAll", right, allTables, groupInfo)
            case Apply(Apply(Apply(TypeApply(Apply(TypeApply(Select(_, "connectBy"), _), query :: Nil), _), cond :: Nil), _), _) =>
                createConnectByClause(query, cond, allTables, groupInfo)
            case Apply(Apply(Apply(TypeApply(Apply(TypeApply(Select(_, "startWith"), _), query :: Nil), _), cond :: Nil), _), _) =>
                createStartWithClause(query, cond, allTables, groupInfo)
            case Apply(Apply(TypeApply(Apply(TypeApply(Select(_, "sortSiblingsBy" | "orderSiblingsBy"), _), query :: Nil), _), order :: Nil), _) =>
                createOrderSiblingsByClause(query, order, allTables, groupInfo)
            case Apply(Apply(TypeApply(Select(_, "maxDepth"), _), query :: Nil), _) =>
                createMaxDepthClause(query, allTables, groupInfo)
            case Apply(Apply(Apply(TypeApply(Apply(TypeApply(Select(_, "on"), _), query :: Nil), _), cond :: Nil), _), _) =>
                createOnClause(query, cond, allTables, groupInfo)
            case Inlined(Some(Apply(TypeApply(Apply(TypeApply(Select(_, "join"), _), query :: Nil), t :: Nil), _)), _, _) =>
                createJoinTableClause(query, t, "inner", allTables, groupInfo)
            case Inlined(Some(Apply(TypeApply(Apply(TypeApply(Select(_, "leftJoin"), _), query :: Nil), t :: Nil), _)), _, _) =>
                createJoinTableClause(query, t, "left", allTables, groupInfo)
            case Inlined(Some(Apply(TypeApply(Apply(TypeApply(Select(_, "rightJoin"), _), query :: Nil), t :: Nil), _)), _, _) =>
                createJoinTableClause(query, t, "right", allTables, groupInfo)
            case Inlined(Some(Apply(Apply(TypeApply(Apply(TypeApply(Select(_, "join"), _), query :: Nil), _), joinQuery :: Nil), _)), _, _) =>
                createJoinSubqueryClause(query, joinQuery, "inner", allTables, groupInfo)
            case Inlined(Some(Apply(Apply(TypeApply(Apply(TypeApply(Select(_, "leftJoin"), _), query :: Nil), _), joinQuery :: Nil), _)), _, _) =>
                createJoinSubqueryClause(query, joinQuery, "left", allTables, groupInfo)
            case Inlined(Some(Apply(Apply(TypeApply(Apply(TypeApply(Select(_, "rightJoin"), _), query :: Nil), _), joinQuery :: Nil), _)), _, _) =>
                createJoinSubqueryClause(query, joinQuery, "right", allTables, groupInfo)
            case Inlined(Some(Apply(Apply(TypeApply(Apply(TypeApply(Select(_, "joinLateral"), _), query :: Nil), _), joinQuery :: Nil), _)), _, _) =>
                createJoinLateralSubqueryClause(query, joinQuery, "inner", allTables, groupInfo)
            case Inlined(Some(Apply(Apply(TypeApply(Apply(TypeApply(Select(_, "leftJoinLateral"), _), query :: Nil), _), joinQuery :: Nil), _)), _, _) =>
                createJoinLateralSubqueryClause(query, joinQuery, "left", allTables, groupInfo)

    def createWhereClause(using q: Quotes, c: QueryContext)(
        query: q.reflect.Term,
        cond: q.reflect.Term,
        allTables: List[(argName: String, tableName: String)],
        groupInfo: List[GroupInfo]
    ): QueryInfo =
        import q.reflect.*

        val baseInfo = 
            createTree(query, allTables, groupInfo)

        val (args, body) = splitFunc(cond)

        val tables =
            args.zip(baseInfo.currentTableAlias) ++
            baseInfo.allTables

        val condInfo = createExpr(body, tables, false, baseInfo.groupInfo)

        checkUngrouped(condInfo.ungroupedPaths)

        if condInfo.hasAgg then
            report.warning("Aggregate functions are not allowed in WHERE.")

        if condInfo.hasWindow then
            report.warning("Window functions are not allowed in WHERE.")

        val newQuery =
            baseInfo.tree.asInstanceOf[SqlQuery.Select].addWhere(condInfo.expr)

        baseInfo.copy(tree = newQuery)

    def createOnClause(using q: Quotes, c: QueryContext)(
        query: q.reflect.Term,
        cond: q.reflect.Term,
        allTables: List[(argName: String, tableName: String)],
        groupInfo: List[GroupInfo]
    ): QueryInfo =
        import q.reflect.*

        val baseInfo = 
            createTree(query, allTables, groupInfo)

        val (args, body) = splitFunc(cond)

        val tables =
            args.zip(baseInfo.currentTableAlias) ++
            baseInfo.allTables

        val condInfo = createExpr(body, tables, false, baseInfo.groupInfo)

        checkUngrouped(condInfo.ungroupedPaths)

        if condInfo.hasAgg then
            report.warning("Aggregate functions are not allowed in ON.")

        if condInfo.hasWindow then
            report.warning("Window functions are not allowed in ON.")

        val baseQuery = baseInfo.tree.asInstanceOf[SqlQuery.Select]
        val newTable = 
            baseInfo.table.asInstanceOf[SqlTable.Join]
                .copy(condition = Some(SqlJoinCondition.On(condInfo.expr)))
        val newQuery =
            baseQuery.copy(
                from = newTable :: Nil
            )

        baseInfo.copy(tree = newQuery, table = newTable)

    def createConnectByClause(using q: Quotes, c: QueryContext)(
        query: q.reflect.Term,
        cond: q.reflect.Term,
        allTables: List[(argName: String, tableName: String)],
        groupInfo: List[GroupInfo]
    ): QueryInfo =
        import q.reflect.*

        val baseInfo = 
            createTree(query, allTables, groupInfo)

        val (args, body) = splitFunc(cond)

        val tables =
            args.zip(baseInfo.currentTableAlias) ++
            baseInfo.allTables

        val condInfo = createExpr(body, tables, true, baseInfo.groupInfo)

        checkUngrouped(condInfo.ungroupedPaths)

        if condInfo.hasAgg then
            report.warning("Aggregate functions are not allowed in CONNECT BY.")

        if condInfo.hasWindow then
            report.warning("Window functions are not allowed in CONNECT BY.")

        val baseTree = baseInfo.tree.asInstanceOf[SqlQuery.Select]
        val startTree = baseTree.copy(
            select = 
                baseTree.select :+ 
                SqlSelectItem.Item(SqlExpr.NumberLiteral(1), Some(columnPseudoLevel))
        )
        val joinTree = baseTree.copy(
            select =
                baseTree.select :+ 
                SqlSelectItem.Item(
                    SqlExpr.Binary(
                        SqlExpr.Column(Some(tableCte), columnPseudoLevel),
                        SqlBinaryOperator.Plus,
                        SqlExpr.NumberLiteral(1)
                    ), 
                    Some(columnPseudoLevel)
                )
            ,
            from = 
                SqlTable.Join(
                    baseTree.from.head,
                    SqlJoinType.Inner,
                    SqlTable.Range(tableCte, None),
                    Some(SqlJoinCondition.On(condInfo.expr)),
                    None
                ) :: Nil
        )
        val unionQuery = SqlQuery.Union(
            startTree, SqlUnionType.UnionAll, joinTree
        )
        val metaData = query.tpe.widen.asType match
            case '[Query[t]] =>
                TypeRepr.of[t] match
                    case AppliedType(_, tpr :: Nil) =>
                        tpr.asType match
                            case '[tpe] =>
                                TableMacroImpl.tableMetaDataMacro[tpe]
        val withItem = SqlWithItem(tableCte, unionQuery, metaData.columnNames :+ columnPseudoLevel)
        val newQuery =
            SqlQuery.Cte(
                withItem :: Nil,
                true,
                SqlQuery.Select(select = Nil, from = SqlTable.Range(tableCte, None) :: Nil)
            )

        baseInfo.copy(tree = newQuery, inConnectBy = true)

    def createStartWithClause(using q: Quotes, c: QueryContext)(
        query: q.reflect.Term,
        cond: q.reflect.Term,
        allTables: List[(argName: String, tableName: String)],
        groupInfo: List[GroupInfo]
    ): QueryInfo =
        import q.reflect.*

        val baseInfo = 
            createTree(query, allTables, groupInfo)

        val (args, body) = splitFunc(cond)

        val tables =
            args.zip(baseInfo.currentTableAlias) ++
            baseInfo.allTables

        val condInfo = createExpr(body, tables, true, baseInfo.groupInfo)

        checkUngrouped(condInfo.ungroupedPaths)

        if condInfo.hasAgg then
            report.warning("Aggregate functions are not allowed in START WITH.")

        if condInfo.hasWindow then
            report.warning("Window functions are not allowed in START WITH.")

        val baseQuery = baseInfo.tree.asInstanceOf[SqlQuery.Cte]

        val withQuery = baseQuery.queryItems.head.query.asInstanceOf[SqlQuery.Union]
        
        val newQuery = baseQuery.copy(
            queryItems = 
                baseQuery.queryItems.head.copy(
                    query =
                        withQuery.copy(left = withQuery.left.asInstanceOf[SqlQuery.Select].addWhere(condInfo.expr))
                ) :: Nil
        )

        baseInfo.copy(tree = newQuery)

    def createSelectClause(using q: Quotes, c: QueryContext)(
        query: q.reflect.Term,
        select: q.reflect.Term,
        allTables: List[(argName: String, tableName: String)],
        groupInfo: List[GroupInfo]
    ): QueryInfo =
        import q.reflect.*

        val baseInfo = 
            createTree(query, allTables, groupInfo)

        val (args, body) = splitFunc(select)

        val currentTables = 
            if !baseInfo.inConnectBy then args.zip(baseInfo.currentTableAlias)
            else args.zip(tableCte :: Nil)

        val tables =
            currentTables ++ baseInfo.allTables

        val infoList = body match
            case Apply(TypeApply(Select(Ident(typeName), "apply"), _), terms) 
                if typeName.startsWith("Tuple")
            =>
                terms.map(t => createExpr(t, tables, baseInfo.inConnectBy, baseInfo.groupInfo, baseInfo.inConnectBy))
            case Inlined(Some(Apply(_, Apply(TypeApply(Select(Ident(n), "apply"), _), terms) :: Nil)), _, _)
                if n.startsWith("Tuple")
            =>
                terms.map(t => createExpr(t, tables, baseInfo.inConnectBy, baseInfo.groupInfo, baseInfo.inConnectBy))
            case _ =>
                createExpr(body, tables, baseInfo.inConnectBy, baseInfo.groupInfo, baseInfo.inConnectBy) :: Nil

        checkUngrouped(infoList.flatMap(_.ungroupedPaths))

        val hasAgg = 
            infoList.map(_.hasAgg).fold(false)(_ || _)

        val notInAgg =
            infoList.flatMap(_.notInAggPaths).map(_.mkString("."))

        val inGroup = baseInfo.groupInfo
            .find(g => currentTables.map(_._2).exists(g.currentTables.contains))
            .map(g => g.groupedExprs.size > 0)
            .getOrElse(false)

        if !inGroup && hasAgg && notInAgg.nonEmpty then
            val c = notInAgg.head
            report.warning(
                s"Column \"$c\" must appear in the GROUP BY clause or be used in an aggregate function."
            )

        val resultSize = 
            if inGroup then ResultSize.Many
            else if notInAgg.isEmpty then ResultSize.ZeroOrOne
            else ResultSize.Many

        val exprs = infoList.map(_.expr)

        val selectItems = exprs.zipWithIndex
            .map((e, i) => SqlSelectItem.Item(e, Some(s"c${i + 1}")))

        val newQuery =
            baseInfo.tree match
                case s: SqlQuery.Select => s.copy(select = selectItems)
                case c: SqlQuery.Cte => c.copy(query = c.query.asInstanceOf[SqlQuery.Select].copy(select = selectItems))
                case _ => throw MatchError(())

        baseInfo.copy(tree = newQuery, selectItems = selectItems, resultSize = resultSize)

    def createGroupByClause(using q: Quotes, c: QueryContext)(
        query: q.reflect.Term,
        group: q.reflect.Term,
        groupType: "" | "cube" | "rollup" | "sets",
        allTables: List[(argName: String, tableName: String)],
        groupInfo: List[GroupInfo]
    ): QueryInfo =
        import q.reflect.*

        val baseInfo =
            createTree(query, allTables, groupInfo)

        val (args, body) = splitFunc(group)

        val currentTables =
            args.zip(baseInfo.currentTableAlias)

        val tables =
            currentTables ++ baseInfo.allTables

        val infoList = 
            body match
                case Apply(TypeApply(Select(Ident(typeName), "apply"), _), terms) 
                    if typeName.startsWith("Tuple")
                =>
                    terms.map(t => createExpr(t, tables, false, baseInfo.groupInfo))
                case _ =>
                    createExpr(body, tables, false, baseInfo.groupInfo) :: Nil

        checkUngrouped(infoList.flatMap(_.ungroupedPaths))

        val hasAgg = infoList.map(_.hasAgg).fold(false)(_ || _)
        if hasAgg then
            report.warning("Aggregate functions are not allowed in GROUP BY.")

        val hasWindow = infoList.map(_.hasWindow).fold(false)(_ || _)
        if hasWindow then
            report.warning("Window functions are not allowed in GROUP BY.")

        val hasValue = infoList.map(_.isValue).fold(false)(_ || _)
        if hasValue then
            report.warning("Constant are not allowed in GROUP BY.")

        val exprs = infoList.map(_.expr)

        val currentGroupInfo = GroupInfo(
            currentTables.map(_._2), 
            exprs.flatMap: e =>
                e match
                    case SqlExpr.Tuple(items) => items
                    case _ => e :: Nil
        )

        val groupBy = groupType match
            case "" => exprs.map(e => SqlGroupItem.Singleton(e))
            case "cube" => SqlGroupItem.Cube(exprs) :: Nil
            case "rollup" => SqlGroupItem.Rollup(exprs) :: Nil
            case "sets" => SqlGroupItem.GroupingSets(exprs) :: Nil

        val newQuery =
            baseInfo.tree.asInstanceOf[SqlQuery.Select]
                .copy(groupBy = groupBy)

        baseInfo.copy(
            tree = newQuery, 
            groupInfo = currentGroupInfo :: baseInfo.groupInfo, 
            resultSize = ResultSize.Many
        )

    def createOrderByClause(using q: Quotes, c: QueryContext)(
        query: q.reflect.Term,
        order: q.reflect.Term,
        allTables: List[(argName: String, tableName: String)],
        groupInfo: List[GroupInfo]
    ): QueryInfo =
        import q.reflect.*

        val baseInfo =
            createTree(query, allTables, groupInfo)

        val (args, body) = splitFunc(order)
        
        val currentTables = 
            if !baseInfo.inConnectBy then args.zip(baseInfo.currentTableAlias)
            else args.zip(tableCte :: Nil)
        
        val tables =
            currentTables ++ baseInfo.allTables
        
        val infoList = body match
            case Apply(TypeApply(Select(Ident(typeName), "apply"), _), terms) 
                if typeName.startsWith("Tuple")
            =>
                terms.map(t => createOrderBy(t, tables, baseInfo.inConnectBy, baseInfo.groupInfo))
            case _ =>
                createOrderBy(body, tables, baseInfo.inConnectBy, baseInfo.groupInfo) :: Nil
        
        checkUngrouped(infoList.map(_.info).flatMap(_.ungroupedPaths))
        
        val hasValue = infoList.map(_.info.isValue).fold(false)(_ || _)
        if hasValue then
            report.warning("Constant are not allowed in ORDER BY.")
        
        val orderBy = infoList
            .map(i => SqlOrderItem(i.info.expr, Some(i.order), i.nullsOrder))

        val newQuery =
            baseInfo.tree match
                case s: SqlQuery.Select => 
                    s.copy(orderBy = s.orderBy ++ orderBy)
                case c: SqlQuery.Cte => 
                    c.copy(
                        query = 
                            c.query.asInstanceOf[SqlQuery.Select]
                            .copy(
                                orderBy = c.query.asInstanceOf[SqlQuery.Select].orderBy ++ orderBy
                            )
                    )
                case _ => 
                    throw MatchError(())
        
        baseInfo.copy(tree = newQuery)

    def createOrderSiblingsByClause(using q: Quotes, c: QueryContext)(
        query: q.reflect.Term,
        order: q.reflect.Term,
        allTables: List[(argName: String, tableName: String)],
        groupInfo: List[GroupInfo]
    ): QueryInfo =
        import q.reflect.*

        val baseInfo =
            createTree(query, allTables, groupInfo)

        val (args, body) = splitFunc(order)
        
        val currentTables =
            args.zip(baseInfo.currentTableAlias)
        
        val tables =
            currentTables ++ baseInfo.allTables
        
        val infoList = body match
            case Apply(TypeApply(Select(Ident(typeName), "apply"), _), terms) 
                if typeName.startsWith("Tuple")
            =>
                terms.map(t => createOrderBy(t, tables, baseInfo.inConnectBy, baseInfo.groupInfo))
            case _ =>
                createOrderBy(body, tables, baseInfo.inConnectBy, baseInfo.groupInfo) :: Nil
        
        checkUngrouped(infoList.map(_.info).flatMap(_.ungroupedPaths))
        
        val hasValue = infoList.map(_.info.isValue).fold(false)(_ || _)
        if hasValue then
            report.warning("Constant are not allowed in ORDER SIBLINGS BY.")
        
        val orderBy = infoList
            .map(i => SqlOrderItem(i.info.expr, Some(i.order), i.nullsOrder))

        val baseQuery = baseInfo.tree.asInstanceOf[SqlQuery.Cte]

        val withQuery = baseQuery.queryItems.head.query.asInstanceOf[SqlQuery.Union]

        val rightQuery = withQuery.right.asInstanceOf[SqlQuery.Select]
        
        val newQuery = baseQuery.copy(
            queryItems = 
                baseQuery.queryItems.head.copy(
                    query =
                        withQuery.copy(
                            right = 
                                rightQuery.copy(orderBy = rightQuery.orderBy ++ orderBy)
                        )
                ) :: Nil
        )
        
        baseInfo.copy(tree = newQuery)

    def createDistinctClause(using q: Quotes, c: QueryContext)(
        query: q.reflect.Term,
        allTables: List[(argName: String, tableName: String)],
        groupInfo: List[GroupInfo]
    ): QueryInfo =
        import q.reflect.*

        val baseInfo =
            createTree(query, allTables, groupInfo)

        val selectItems = baseInfo.tree match
            case s: SqlQuery.Select => s.select
            case c: SqlQuery.Cte => c.query.asInstanceOf[SqlQuery.Select].select
            case _ => throw MatchError(())

        val select = selectItems.flatMap:
            case SqlSelectItem.Item(expr, _) => expr :: Nil
            case _ => Nil

        val orderBy = baseInfo.tree match
            case s: SqlQuery.Select => s.orderBy
            case c: SqlQuery.Cte => c.query.asInstanceOf[SqlQuery.Select].orderBy
            case _ => throw MatchError(())
        
        val notInSelect = orderBy.map(_.expr).find(o => !select.contains(o))
        if notInSelect.isDefined then
            report.warning(s"For SELECT DISTINCT, ORDER BY expressions must appear in select list.")
        
        val newQuery =
            baseInfo.tree match
                case s: SqlQuery.Select => 
                    s.copy(param = Some(SqlParam.Distinct))
                case c: SqlQuery.Cte => 
                    c.copy(
                        query = 
                            c.query.asInstanceOf[SqlQuery.Select]
                            .copy(
                                param = Some(SqlParam.Distinct)
                            )
                    )
                case _ => 
                    throw MatchError(())

        baseInfo.copy(tree = newQuery)

    def createLimitClause(using q: Quotes, c: QueryContext)(
        query: q.reflect.Term,
        n: q.reflect.Term,
        allTables: List[(argName: String, tableName: String)],
        groupInfo: List[GroupInfo]
    ): QueryInfo =
        val baseInfo =
            createTree(query, allTables, groupInfo)
        
        val resultSize = n.asExprOf[Int].value match
            case Some(0 | 1) => ResultSize.ZeroOrOne
            case _ => ResultSize.Many
        
        val newQuery = baseInfo.tree match
            case s: SqlQuery.Select =>
                s.copy(limit = Some(SqlLimit(SqlExpr.NumberLiteral(1), SqlExpr.NumberLiteral(1))))
            case c: SqlQuery.Cte =>
                c.copy(
                    query = 
                        c.query.asInstanceOf[SqlQuery.Select]
                        .copy(limit = Some(SqlLimit(SqlExpr.NumberLiteral(1), SqlExpr.NumberLiteral(1))))
                    )
            case u: SqlQuery.Union =>
                u.copy(limit = Some(SqlLimit(SqlExpr.NumberLiteral(1), SqlExpr.NumberLiteral(1))))
            case _ => throw MatchError(())

        baseInfo.copy(tree = newQuery, resultSize = resultSize)

    def createOffsetClause(using q: Quotes, c: QueryContext)(
        query: q.reflect.Term,
        allTables: List[(argName: String, tableName: String)],
        groupInfo: List[GroupInfo]
    ): QueryInfo =
        val baseInfo =
            createTree(query, allTables, groupInfo)
        
        val newQuery = baseInfo.tree match
            case s: SqlQuery.Select =>
                s.copy(limit = Some(SqlLimit(SqlExpr.NumberLiteral(1), SqlExpr.NumberLiteral(1))))
            case c: SqlQuery.Cte =>
                c.copy(
                    query = 
                        c.query.asInstanceOf[SqlQuery.Select]
                        .copy(limit = Some(SqlLimit(SqlExpr.NumberLiteral(1), SqlExpr.NumberLiteral(1))))
                    )
            case u: SqlQuery.Union =>
                u.copy(limit = Some(SqlLimit(SqlExpr.NumberLiteral(1), SqlExpr.NumberLiteral(1))))
            case _ => throw MatchError(())
        
        baseInfo.copy(tree = newQuery)

    def createMaxDepthClause(using q: Quotes, c: QueryContext)(
        query: q.reflect.Term,
        allTables: List[(argName: String, tableName: String)],
        groupInfo: List[GroupInfo]
    ): QueryInfo =
        val baseInfo =
            createTree(query, allTables, groupInfo)

        val cond = SqlExpr.Binary(
            SqlExpr.Column(Some(tableCte), columnPseudoLevel),
            SqlBinaryOperator.LessThan,
            SqlExpr.NumberLiteral(1)
        )
        
        val baseQuery = baseInfo.tree.asInstanceOf[SqlQuery.Cte]

        val withQuery = baseQuery.queryItems.head.query.asInstanceOf[SqlQuery.Union]
        
        val newQuery = baseQuery.copy(
            queryItems = 
                baseQuery.queryItems.head.copy(
                    query =
                        withQuery.copy(right = withQuery.right.asInstanceOf[SqlQuery.Select].addWhere(cond))
                ) :: Nil
        )
        
        baseInfo.copy(tree = newQuery)

    def createFromTableClause(using q: Quotes, c: QueryContext)(
        typeTree: q.reflect.TypeTree,
        allTables: List[(argName: String, tableName: String)],
        groupInfo: List[GroupInfo]
    ): QueryInfo =
        typeTree.tpe.widen.asType match
            case '[t] =>
                val metaData = TableMacroImpl.tableMetaDataMacro[t]

                c.tableIndex += 1
                val alias = s"t${c.tableIndex}"

                val selectItems = metaData.columnNames.zipWithIndex.map: (c, i) =>
                    SqlSelectItem.Item(SqlExpr.Column(Some(alias), c), Some(s"c${i + 1}"))
                
                val table = SqlTable.Range(metaData.tableName, Some(SqlTableAlias(alias, Nil)))
                
                val query = SqlQuery.Select(
                    select = selectItems,
                    from = table :: Nil
                )

                QueryInfo(query, alias :: Nil, table, selectItems, allTables, groupInfo, ResultSize.Many)

    def createFromSubqueryClause(using q: Quotes, c: QueryContext)(
        subquery: q.reflect.Term,
        allTables: List[(argName: String, tableName: String)],
        groupInfo: List[GroupInfo]
    ): QueryInfo =
        subquery.tpe.widen.asType match
            case '[Query[t]] =>
                val innerQuery = createTree(subquery, allTables, groupInfo)
                
                c.tableIndex += 1
                val alias = s"t${c.tableIndex}"
                
                val selectItems = (1 to selectItemSize(innerQuery.tree)).map: i =>
                    SqlSelectItem.Item(SqlExpr.Column(Some(alias), s"c$i"), Some(s"c$i"))
                .toList
                
                val table = SqlTable.SubQuery(innerQuery.tree, false, Some(SqlTableAlias(alias)))
                
                val query = SqlQuery.Select(
                    select = selectItems,
                    from = table :: Nil
                )
                
                QueryInfo(query, alias :: Nil, table, selectItems, allTables, groupInfo, ResultSize.Many)
            case '[type t; Seq[t]] => 
                val metaData =  TableMacroImpl.tableMetaDataMacro[t]
                
                c.tableIndex += 1
                val alias = s"t${c.tableIndex}"
                
                val innerQuery = SqlQuery.Values(metaData.fieldNames.map(_ => SqlExpr.StringLiteral("")) :: Nil)
                
                val selectItems = metaData.columnNames.zipWithIndex.map: (c, i) =>
                    SqlSelectItem.Item(SqlExpr.Column(Some(alias), c), Some(s"c${i + 1}"))
                .toList
                
                val table = SqlTable.SubQuery(
                    innerQuery, 
                    false, 
                    Some(
                        SqlTableAlias(
                            alias, 
                            metaData.columnNames
                        )
                    )
                )
                
                val query = SqlQuery.Select(
                    select = selectItems,
                    from = table :: Nil
                )
                
                QueryInfo(query, alias :: Nil, table, selectItems, allTables, groupInfo, ResultSize.Many)

    def createJoinTableClause(using q: Quotes, c: QueryContext)(
        query: q.reflect.Term,
        typeTree: q.reflect.TypeTree,
        joinType: "inner" | "left" | "right",
        allTables: List[(argName: String, tableName: String)],
        groupInfo: List[GroupInfo]
    ): QueryInfo =
        val sqlJoinType = joinType match
            case "inner" => SqlJoinType.Inner
            case "left" => SqlJoinType.Left
            case "right" => SqlJoinType.Right

        val queryInfo = createTree(query, allTables, groupInfo)

        typeTree.tpe.widen.asType match
            case '[t] =>
                val metaData = TableMacroImpl.tableMetaDataMacro[t]
                
                c.tableIndex += 1
                val alias = s"t${c.tableIndex}"
                
                val baseQuerySize = queryInfo.selectItems.size
                val joinSelectItems = metaData.columnNames.zipWithIndex.map: (c, i) =>
                    SqlSelectItem.Item(SqlExpr.Column(Some(alias), c), Some(s"c${i + baseQuerySize + 1}"))
                val selectItems = queryInfo.selectItems ++ joinSelectItems
                
                val joinTable = SqlTable.Range(metaData.tableName, Some(SqlTableAlias(alias, Nil)))
                val table = SqlTable.Join(
                    queryInfo.table,
                    sqlJoinType,
                    joinTable,
                    None,
                    None
                )
                
                val newQuery = SqlQuery.Select(
                    select = selectItems,
                    from = table :: Nil
                )
                
                QueryInfo(newQuery, queryInfo.currentTableAlias.appended(alias), table, selectItems, allTables, groupInfo, ResultSize.Many)

    def createJoinSubqueryClause(using q: Quotes, c: QueryContext)(
        query: q.reflect.Term,
        subquery: q.reflect.Term,
        joinType: "inner" | "left" | "right",
        allTables: List[(argName: String, tableName: String)],
        groupInfo: List[GroupInfo]
    ): QueryInfo =
        val sqlJoinType = joinType match
            case "inner" => SqlJoinType.Inner
            case "left" => SqlJoinType.Left
            case "right" => SqlJoinType.Right

        val queryInfo = createTree(query, allTables, groupInfo)
        
        val joinQueryInfo = createTree(subquery, allTables, groupInfo)
        
        c.tableIndex += 1
        val alias = s"t${c.tableIndex}"
        
        val baseQuerySize = queryInfo.selectItems.size
        val joinSelectItems = (1 + baseQuerySize to selectItemSize(joinQueryInfo.tree) + baseQuerySize).map: i =>
            SqlSelectItem.Item(SqlExpr.Column(Some(alias), s"c${i - baseQuerySize}"), Some(s"c$i"))
        .toList
        val selectItems = queryInfo.selectItems ++ joinSelectItems
        
        val joinTable = SqlTable.SubQuery(joinQueryInfo.tree, false, Some(SqlTableAlias(alias, Nil)))
        
        val table = SqlTable.Join(
            queryInfo.table,
            sqlJoinType,
            joinTable,
            None,
            None
        )
        
        val newQuery = SqlQuery.Select(
            select = selectItems,
            from = table :: Nil
        )
        
        QueryInfo(newQuery, queryInfo.currentTableAlias.appended(alias), table, selectItems, allTables, groupInfo, ResultSize.Many)

    def createJoinLateralSubqueryClause(using q: Quotes, c: QueryContext)(
        query: q.reflect.Term,
        subquery: q.reflect.Term,
        joinType: "inner" | "left",
        allTables: List[(argName: String, tableName: String)],
        groupInfo: List[GroupInfo]
    ): QueryInfo =
        val sqlJoinType = joinType match
            case "inner" => SqlJoinType.Inner
            case "left" => SqlJoinType.Left

        val queryInfo = createTree(query, allTables, groupInfo)

        val (args, body) = splitFunc(subquery)

        val tables =
            args.zip(queryInfo.currentTableAlias) ++
            allTables
        
        val joinQueryInfo = createTree(body, tables, groupInfo)

        c.tableIndex += 1
        val alias = s"t${c.tableIndex}"

        val baseQuerySize = queryInfo.selectItems.size
        val joinSelectItems = (1 + baseQuerySize to selectItemSize(joinQueryInfo.tree) + baseQuerySize).map: i =>
            SqlSelectItem.Item(SqlExpr.Column(Some(alias), s"c${i - baseQuerySize}"), Some(s"c$i"))
        .toList
        val selectItems = queryInfo.selectItems ++ joinSelectItems
        
        val joinTable = SqlTable.SubQuery(joinQueryInfo.tree, true, Some(SqlTableAlias(alias, Nil)))
        val table = SqlTable.Join(
            queryInfo.table,
            sqlJoinType,
            joinTable,
            None,
            None
        )

        val newQuery = SqlQuery.Select(
            select = selectItems,
            from = table :: Nil
        )

        QueryInfo(newQuery, queryInfo.currentTableAlias.appended(alias), table, selectItems, allTables, groupInfo, ResultSize.Many)

    def createUnionClause(using q: Quotes, c: QueryContext)(
        left: q.reflect.Term,
        unionType: "union" | "unionAll" | "except" | "exceptAll" | "intersect" | "intersectAll",
        right: q.reflect.Term,
        allTables: List[(argName: String, tableName: String)],
        groupInfo: List[GroupInfo]
    ): QueryInfo =
        val leftInfo = createTree(left, allTables, groupInfo)

        val sqlUnionType = unionType match
            case "union" => SqlUnionType.Union
            case "unionAll" => SqlUnionType.UnionAll
            case "except" => SqlUnionType.Except
            case "exceptAll" => SqlUnionType.ExceptAll
            case "intersect" => SqlUnionType.Intersect
            case "intersectAll" => SqlUnionType.IntersectAll

        val rightInfo = createTree(right, allTables, groupInfo)

        val newQuery = SqlQuery.Union(leftInfo.tree, sqlUnionType, rightInfo.tree)

        QueryInfo(newQuery, Nil, leftInfo.table, leftInfo.selectItems, allTables, groupInfo, ResultSize.Many)

    def selectItemSize(query: SqlQuery): Int = 
        query match
            case s: SqlQuery.Select => s.select.size
            case u: SqlQuery.Union => selectItemSize(u.left)
            case c: SqlQuery.Cte => selectItemSize(c.query)
            case v: SqlQuery.Values => v.values.head.size

    def checkUngrouped(ungrouped: List[List[String]])(using q: Quotes): Unit =
        import q.reflect.*

        if ungrouped.nonEmpty then
            val path = ungrouped.head.mkString(".")
            report.warning(s"Column \"$path\" must appear in the GROUP BY clause or be used in an aggregate function.")

    def splitFunc(using q: Quotes)(term: q.reflect.Term): (List[String], q.reflect.Term) =
        import q.reflect.*

        removeEmptyInline(term) match
            case Block(DefDef(_, _, _, Some(Block(args, body))) :: Nil, _) =>
                val valDefList = args.asInstanceOf[List[ValDef]]
                (valDefList.map(_.name), body)
            case Block(DefDef(_, args :: Nil, _, Some(body)) :: Nil, _) =>
                val valDefList = args.asInstanceOf[List[ValDef]]
                (valDefList.map(_.name), body)

    def removeEmptyInline(using q: Quotes)(term: q.reflect.Term): q.reflect.Term =
        import q.reflect.*

        term match
            case Inlined(_, Nil, t) =>
                removeEmptyInline(t)
            case Typed(t, _) => t
            case _ => term

    def binaryOperators: List[String] =
        List(
            "==", "!=", "===", "<>", ">", ">=", "<", "<=", "&&", "||",
            "+", "-", "*", "/", "%", "->", "->>",
            "like", "contains", "startsWith", "endsWith"
        )

    def unaryOperators: List[String] =
        List("unary_+", "unary_-", "unary_!", "prior")

    def tableCte = "__cte__"

    def columnPseudoLevel = "__pseudo__level__"

    def validateDiv(using q: Quotes)(term: q.reflect.Term): Unit =
        import q.reflect.*

        val isZero = term match
            case Literal(IntConstant(0)) => true
            case Literal(LongConstant(0L)) => true
            case Literal(FloatConstant(0F)) => true
            case Literal(DoubleConstant(0D)) => true
            case _ => false
        if isZero then report.warning("Division by zero.")

    def createExpr(using q: Quotes, c: QueryContext)(
        term: q.reflect.Term,
        allTables: List[(argName: String, tableName: String)],
        inConnectBy: Boolean,
        groupInfo: List[GroupInfo],
        inConnectByMap: Boolean = false
    ): ExprInfo =
        import q.reflect.*

        val info = term match
            case TypeApply(
                Select(
                    Apply(
                        Select(o@Ident(objectName), "selectDynamic"), 
                        Literal(StringConstant(valName)) :: Nil
                    ),
                    "$asInstanceOf$"
                ),
                _
            ) =>
                createColumnExpr(o, objectName, valName, allTables, groupInfo)
            case TypeApply(
                Select(
                    Inlined(
                        Some(
                            Apply(
                                Select(o@Ident(objectName), "selectDynamic"), 
                                Literal(StringConstant(valName)) :: Nil
                            )
                        ),
                        _,
                        _
                    ),
                    "$asInstanceOf$"
                ),
                _
            ) =>
                createSubqueryColumnExpr(o, objectName, valName, allTables, groupInfo)
            case Apply(Apply(Apply(TypeApply(Ident(op), _), expr :: Nil), _), _) 
                if unaryOperators.contains(op)
            => 
                createUnaryExpr(expr, op, allTables, inConnectBy, groupInfo)
            case Apply(Apply(TypeApply(Ident("prior"), _), expr :: Nil), _) =>
                createUnaryExpr(expr, "prior", allTables, inConnectBy, groupInfo)
            case Apply(Apply(TypeApply(Select(left, op), _), right :: Nil), _) 
                if binaryOperators.contains(op)
            =>
                createBinaryExpr(left, op, right, allTables, inConnectBy, groupInfo)
            case Apply(Apply(Apply(TypeApply(Apply(TypeApply(Ident(op), _), left :: Nil), _), right :: Nil), _), _) 
                if binaryOperators.contains(op)
            =>
                createBinaryExpr(left, op, right, allTables, inConnectBy, groupInfo)
            case Apply(Apply(Apply(Apply(TypeApply(Ident(op), _), left :: Nil), right :: Nil), _), _) 
                if binaryOperators.contains(op)
            =>
                createBinaryExpr(left, op, right, allTables, inConnectBy, groupInfo)
            case Apply(Apply(Apply(TypeApply(Apply(TypeApply(Ident("in"), _), left :: Nil), _), right :: Nil), _), _) =>
                createInExpr(left, right, allTables, inConnectBy, groupInfo)
            case Apply(Apply(Apply(TypeApply(Apply(TypeApply(Ident("between"), _), left :: Nil), _), start :: end :: Nil), _), _) =>
                createBetweenExpr(left, start, end, allTables, inConnectBy, groupInfo)
            case Apply(Apply(TypeApply(Ident("asExpr"), _), expr :: Nil), _) =>
                createExpr(expr, allTables, inConnectBy, groupInfo)
            case Apply(TypeApply(Select(Ident(typeName), "apply"), _), terms) 
                if typeName.startsWith("Tuple")
            =>
                createTupleExpr(terms, allTables, inConnectBy, groupInfo)
            case Apply(Apply(Ident("interval"), Apply(Apply(Ident(op), v :: Nil), _) :: Nil), _) =>
                createIntervalExpr(v, op)
            case Apply(Apply(TypeApply(Ident("extract"), _), Apply(Apply(TypeApply(Select(Apply(Ident(op), _), "from"), _), expr :: Nil), _) :: Nil), _) =>
                createExtractExpr(expr, op, allTables, inConnectBy, groupInfo)
            case _ 
                if term.symbol.annotations.exists:
                    case Apply(Select(New(TypeIdent("agg")), _), _) => true
                    case _ => false
            =>
                val name = term.symbol.annotations.collectFirst:
                    case Apply(Select(New(TypeIdent("agg")), _), Literal(StringConstant(n)) :: Nil) => n
                .get
                createAggExpr(name, term, allTables, inConnectBy, groupInfo)
            case _ 
                if term.symbol.annotations.exists:
                    case Apply(Select(New(TypeIdent("function")), _), _) => true
                    case _ => false
            =>
                val name = term.symbol.annotations.collectFirst:
                    case Apply(Select(New(TypeIdent("function")), _), Literal(StringConstant(n)) :: Nil) => n
                .get
                createFuncExpr(name, term, false, allTables, inConnectBy, groupInfo)
            case _ 
                if term.symbol.annotations.exists:
                    case Apply(Select(New(TypeIdent("window")), _), _) => true
                    case _ => false
            =>
                val name = term.symbol.annotations.collectFirst:
                    case Apply(Select(New(TypeIdent("window")), _), Literal(StringConstant(n)) :: Nil) => n
                .get
                report.warning(s"Window function \"$name\" requires an OVER clause.")
                createFuncExpr(name, term, true, allTables, inConnectBy, groupInfo)
            case Apply(Apply(Select(expr, "over"), over :: Nil), _) =>
                createWindowExpr(expr, over, allTables, inConnectBy, groupInfo)
            case Apply(Apply(TypeApply(Ident(op), _), query :: Nil), _) 
                if List("exists", "any", "all").contains(op)
            =>
                createSubLinkExpr(query, op, allTables, groupInfo)
            case t@Apply(Apply(Apply(TypeApply(Select(_, "else"), _), _), _), _) =>
                createCaseExpr(t, allTables, inConnectBy, groupInfo)
            case Apply(Apply(Ident("level"), _), _) =>
                if !inConnectByMap then
                    report.warning("LEVEL can only be used in the SELECT clause of CONNECT BY.")
                ExprInfo(
                    expr = SqlExpr.Column(Some(tableCte), columnPseudoLevel),
                    hasAgg = false,
                    isAgg = false,
                    hasWindow = false,
                    isValue = false,
                    ungroupedPaths = Nil,
                    notInAggPaths = Nil
                )
            case _ =>
                term.tpe.widen.asType match
                    case '[Query[t]] =>
                        createSubqueryExpr(term, allTables, groupInfo)
                    case '[Unit] =>
                        ExprInfo(
                            expr = SqlExpr.Tuple(Nil),
                            hasAgg = false,
                            isAgg = false,
                            hasWindow = false,
                            isValue = true,
                            ungroupedPaths = Nil,
                            notInAggPaths = Nil
                        )
                    case '[None.type] =>
                        ExprInfo(
                            expr = SqlExpr.Null,
                            hasAgg = false,
                            isAgg = false,
                            hasWindow = false,
                            isValue = true,
                            ungroupedPaths = Nil,
                            notInAggPaths = Nil
                        )
                    case '[t] =>
                        val asSqlExpr = Expr.summon[AsSqlExpr[t]]
                        val expr = term.asExpr
                        asSqlExpr match
                            case Some(_) =>
                                ExprInfo(
                                    expr = SqlExpr.StringLiteral(expr.show),
                                    hasAgg = false,
                                    isAgg = false,
                                    hasWindow = false,
                                    isValue = true,
                                    ungroupedPaths = Nil,
                                    notInAggPaths = Nil
                                )
                            case _ => throw MatchError(())

        val ungroupedPaths = 
            if groupInfo.flatMap(_.groupedExprs).contains(info.expr) then Nil
            else info.ungroupedPaths
        
        val notInAggPaths =
            if groupInfo.flatMap(_.groupedExprs).contains(info.expr) then Nil
            else info.notInAggPaths

        info.copy(ungroupedPaths = ungroupedPaths, notInAggPaths = notInAggPaths, code = term.pos.sourceCode.get)

    def createColumnExpr(using q: Quotes, c: QueryContext)(
        term: q.reflect.Term,
        objectName: String,
        valName: String,
        allTables: List[(argName: String, tableName: String)],
        groupInfo: List[GroupInfo]
    ): ExprInfo =
        term.tpe.widen.asType match
            case '[Table[t]] =>
                val metaData = term.tpe.asType match
                    case '[Table[Option[t]]] =>
                        TableMacroImpl.tableMetaDataMacro[t]
                    case '[Table[t]] =>
                        TableMacroImpl.tableMetaDataMacro[t]
                val tableName  = allTables.find(t => t.argName == objectName).map(_.tableName).get
                val columnName = metaData.fieldNames.zip(metaData.columnNames).find((f, _) => f == valName).map(_._2).get
                val sqlExpr =
                    SqlExpr.Column(Some(tableName), columnName)
                val path = objectName :: valName :: Nil
                val inGroup = groupInfo
                    .find(g => g.currentTables.contains(tableName))
                    .map(g => g.groupedExprs.size > 0)
                    .getOrElse(false)
                val ungroupedPaths = 
                    if inGroup then path :: Nil
                    else Nil
                ExprInfo(
                    expr = sqlExpr,
                    hasAgg = false,
                    isAgg = false,
                    hasWindow = false,
                    isValue = false,
                    ungroupedPaths = ungroupedPaths,
                    notInAggPaths = path :: Nil
                )

    def createSubqueryColumnExpr(using q: Quotes, c: QueryContext)(
        term: q.reflect.Term,
        objectName: String,
        valName: String,
        allTables: List[(argName: String, tableName: String)],
        groupInfo: List[GroupInfo]
    ): ExprInfo =
        import q.reflect.*

        term.tpe.widen.asType match
            case '[SubQuery[n, v]] =>
                val names = TypeRepr.of[n] match
                    case AppliedType(_, ns) =>
                        ns.map:
                            case ConstantType(StringConstant(n)) => n
                val index = names.indexOf(valName)
                val tableName  = allTables.find(t => t.argName == objectName).map(_.tableName).get
                val columnName = s"c${index + 1}"
                val sqlExpr =
                    SqlExpr.Column(Some(tableName), columnName)
                val path = objectName :: valName :: Nil
                val inGroup = groupInfo
                    .find(g => g.currentTables.contains(tableName))
                    .map(g => g.groupedExprs.size > 0)
                    .getOrElse(false)
                val ungroupedPaths = 
                    if inGroup then path :: Nil
                    else Nil
                ExprInfo(
                    expr = sqlExpr,
                    hasAgg = false,
                    isAgg = false,
                    hasWindow = false,
                    isValue = false,
                    ungroupedPaths = ungroupedPaths,
                    notInAggPaths = path :: Nil
                )

    def createTupleExpr(using q: Quotes, c: QueryContext)(
        terms: List[q.reflect.Term],
        allTables: List[(argName: String, tableName: String)],
        inConnectBy: Boolean,
        groupInfo: List[GroupInfo]
    ): ExprInfo =
        val infoList = terms.map(t => createExpr(t, allTables, inConnectBy, groupInfo))
        ExprInfo(
            expr = SqlExpr.Tuple(infoList.map(_.expr)),
            hasAgg = infoList.map(_.hasAgg).fold(false)(_ || _),
            isAgg = false,
            hasWindow = infoList.map(_.hasWindow).fold(false)(_ || _),
            isValue = false,
            ungroupedPaths = infoList.flatMap(_.ungroupedPaths),
            notInAggPaths = infoList.flatMap(_.notInAggPaths)
        )

    def createIntervalExpr(using q: Quotes, c: QueryContext)(
        term: q.reflect.Term,
        op: String
    ): ExprInfo =
        val unit = op match
            case "year" => SqlTimeUnit.Year
            case "month" => SqlTimeUnit.Month
            case "week" => SqlTimeUnit.Week
            case "day" => SqlTimeUnit.Day
            case "hour" => SqlTimeUnit.Hour
            case "minute" => SqlTimeUnit.Minute
            case "second" => SqlTimeUnit.Second
        ExprInfo(
            expr = SqlExpr.Interval(term.asExprOf[Double].value.getOrElse(0), unit),
            hasAgg = false,
            isAgg = false,
            hasWindow = false,
            isValue = false,
            ungroupedPaths = Nil,
            notInAggPaths = Nil
        )

    def createExtractExpr(using q: Quotes, c: QueryContext)(
        term: q.reflect.Term,
        op: String,
        allTables: List[(argName: String, tableName: String)],
        inConnectBy: Boolean,
        groupInfo: List[GroupInfo]
    ): ExprInfo =
        val unit = op match
            case "year" => SqlTimeUnit.Year
            case "month" => SqlTimeUnit.Month
            case "week" => SqlTimeUnit.Week
            case "day" => SqlTimeUnit.Day
            case "hour" => SqlTimeUnit.Hour
            case "minute" => SqlTimeUnit.Minute
            case "second" => SqlTimeUnit.Second
        val exprInfo = createExpr(term, allTables, inConnectBy, groupInfo)
        ExprInfo(
            expr = SqlExpr.Extract(exprInfo.expr, unit),
            hasAgg = exprInfo.hasAgg,
            isAgg = false,
            hasWindow = exprInfo.hasWindow,
            isValue = false,
            ungroupedPaths = exprInfo.ungroupedPaths,
            notInAggPaths = exprInfo.notInAggPaths
        )

    def createSubLinkExpr(using q: Quotes, c: QueryContext)(
        term: q.reflect.Term,
        op: String,
        allTables: List[(argName: String, tableName: String)],
        groupInfo: List[GroupInfo]
    ): ExprInfo =
        val queryInfo = createTree(term, allTables, groupInfo)
        val linkType = op match
            case "exists" => SqlSubLinkType.Exists
            case "any" => SqlSubLinkType.Any
            case "all" => SqlSubLinkType.All
        val expr = SqlExpr.SubLink(queryInfo.tree, linkType)
        ExprInfo(
            expr = expr,
            hasAgg = false,
            isAgg = false,
            hasWindow = false,
            isValue = false,
            ungroupedPaths = Nil,
            notInAggPaths = Nil
        )

    def createSubqueryExpr(using q: Quotes, c: QueryContext)(
        term: q.reflect.Term,
        allTables: List[(argName: String, tableName: String)],
        groupInfo: List[GroupInfo]
    ): ExprInfo =
        import q.reflect.*

        val queryInfo = createTree(term, allTables, groupInfo)
        if queryInfo.resultSize == ResultSize.Many then
            report.warning(s"More than one row returned by a subquery \"${term.pos.sourceCode.get}\" used as an expression.")
        val expr = SqlExpr.SubQuery(queryInfo.tree)
        ExprInfo(
            expr = expr,
            hasAgg = false,
            isAgg = false,
            hasWindow = false,
            isValue = false,
            ungroupedPaths = Nil,
            notInAggPaths = Nil
        )

    def createCaseExpr(using q: Quotes, c: QueryContext)(
        term: q.reflect.Term,
        allTables: List[(argName: String, tableName: String)],
        inConnectBy: Boolean,
        groupInfo: List[GroupInfo]
    ): ExprInfo =
        import q.reflect.*

        def matchRecursive(term: Term): List[ExprInfo] =
            term match
                case Apply(Apply(Apply(TypeApply(Select(t, "else"), _), elseTerm :: Nil), _), _) =>
                    matchRecursive(t) :+ createExpr(elseTerm, allTables, inConnectBy, groupInfo)
                case Apply(Apply(Apply(TypeApply(Select(t, "then"), _), thenTerm :: Nil), _), _) =>
                    matchRecursive(t) :+ createExpr(thenTerm, allTables, inConnectBy, groupInfo)
                case Apply(Apply(Apply(TypeApply(Select(t, "else if"), _), ifTerm :: Nil), _), _) =>
                    matchRecursive(t) :+ createExpr(ifTerm, allTables, inConnectBy, groupInfo)
                case Apply(Apply(TypeApply(Select(t, "then"), _), thenTerm :: Nil), _) =>
                   matchRecursive(t) :+ createExpr(thenTerm, allTables, inConnectBy, groupInfo)
                case Apply(Apply(Apply(TypeApply(Ident("if"), _), ifTerm :: Nil), _), _) =>
                    createExpr(ifTerm, allTables, inConnectBy, groupInfo) :: Nil

        val infoList = matchRecursive(term)
        val exprList = infoList.map(_.expr)

        val caseBranches = exprList
            .dropRight(1)
            .grouped(2)
            .map(b => SqlCase(b(0), b(1)))
            .toList

        val expr = SqlExpr.Case(caseBranches, exprList.last)

        ExprInfo(
            expr = expr,
            hasAgg = infoList.map(_.hasAgg).fold(false)(_ || _),
            isAgg = false,
            hasWindow = infoList.map(_.hasWindow).fold(false)(_ || _),
            isValue = false,
            ungroupedPaths = infoList.flatMap(_.ungroupedPaths),
            notInAggPaths = infoList.flatMap(_.notInAggPaths)
        )
    
    def createUnaryExpr(using q: Quotes, c: QueryContext)(
        term: q.reflect.Term,
        op: String,
        allTables: List[(argName: String, tableName: String)],
        inConnectBy: Boolean,
        groupInfo: List[GroupInfo]
    ): ExprInfo =
        import q.reflect.*

        val info = createExpr(term, allTables, inConnectBy, groupInfo)

        val expr =
            op match
                case "unary_+" =>
                    SqlExpr.Unary(info.expr, SqlUnaryOperator.Positive)
                case "unary_-" =>
                    SqlExpr.Unary(info.expr, SqlUnaryOperator.Negative)
                case "prior" =>
                    if !inConnectBy then
                        report.warning("PRIOR can only be used in CONNECT BY.")
                    info.expr match
                        case SqlExpr.Column(_, columnName) =>
                            SqlExpr.Column(Some(tableCte), columnName)
                        case _ => throw MatchError(())
                case "unary_!" =>
                    info.expr match
                        case SqlExpr.Binary(left, SqlBinaryOperator.Like, right) =>
                            SqlExpr.Binary(left, SqlBinaryOperator.NotLike, right)
                        case SqlExpr.Binary(left, SqlBinaryOperator.In, right) =>
                            SqlExpr.Binary(left, SqlBinaryOperator.NotIn, right)
                        case SqlExpr.Binary(left, SqlBinaryOperator.NotIn, right) =>
                            SqlExpr.Binary(left, SqlBinaryOperator.In, right)
                        case SqlExpr.NullTest(ne, not) =>
                            SqlExpr.NullTest(ne, !not)
                        case SqlExpr.Between(expr, s, e, n) =>
                            SqlExpr.Between(expr, s, e, !n)
                        case _ => SqlExpr.Unary(info.expr, SqlUnaryOperator.Not)

        ExprInfo(
            expr = expr,
            hasAgg = info.hasAgg,
            isAgg = false,
            hasWindow = info.hasWindow,
            isValue = false,
            ungroupedPaths = info.ungroupedPaths,
            notInAggPaths = info.notInAggPaths
        )
    
    def createBinaryExpr(using q: Quotes, c: QueryContext)(
        left: q.reflect.Term,
        op: String,
        right: q.reflect.Term,
        allTables: List[(argName: String, tableName: String)],
        inConnectBy: Boolean,
        groupInfo: List[GroupInfo]
    ): ExprInfo =
        if op == "/" || op == "%" then
            validateDiv(right)

        val leftInfo = createExpr(left, allTables, inConnectBy, groupInfo)
        val rightInfo = createExpr(right, allTables, inConnectBy, groupInfo)

        val leftExpr = leftInfo.expr
        val rightExpr = rightInfo.expr

        val expr =
            op match
                case "==" | "===" =>
                    rightExpr match
                        case SqlExpr.Null =>
                            SqlExpr.NullTest(leftExpr, false)
                        case _ =>
                            SqlExpr.Binary(leftExpr, SqlBinaryOperator.Equal, rightExpr)
                case "!=" | "<>" =>
                    rightExpr match
                        case SqlExpr.Null =>
                            SqlExpr.NullTest(leftExpr, true)
                        case _ =>
                            SqlExpr.Binary(
                                SqlExpr.Binary(leftExpr, SqlBinaryOperator.NotEqual, rightExpr),
                                SqlBinaryOperator.Or,
                                SqlExpr.NullTest(leftExpr, false)
                            )
                case ">" =>
                    SqlExpr.Binary(leftExpr, SqlBinaryOperator.GreaterThan, rightExpr)
                case ">=" =>
                    SqlExpr.Binary(leftExpr, SqlBinaryOperator.GreaterThanEqual, rightExpr)
                case "<" =>
                    SqlExpr.Binary(leftExpr, SqlBinaryOperator.LessThan, rightExpr)
                case "<=" =>
                    SqlExpr.Binary(leftExpr, SqlBinaryOperator.LessThanEqual, rightExpr)
                case "&&" =>
                    SqlExpr.Binary(leftExpr, SqlBinaryOperator.And, rightExpr)
                case "||" =>
                    SqlExpr.Binary(leftExpr, SqlBinaryOperator.Or, rightExpr)
                case "+" =>
                    SqlExpr.Binary(leftExpr, SqlBinaryOperator.Plus, rightExpr)
                case "-" =>
                    SqlExpr.Binary(leftExpr, SqlBinaryOperator.Minus, rightExpr)
                case "*" =>
                    SqlExpr.Binary(leftExpr, SqlBinaryOperator.Times, rightExpr)
                case "/" =>
                    SqlExpr.Binary(leftExpr, SqlBinaryOperator.Div, rightExpr)
                case "%" =>
                    SqlExpr.Binary(leftExpr, SqlBinaryOperator.Mod, rightExpr)
                case "->" =>
                    SqlExpr.Binary(leftExpr, SqlBinaryOperator.Json, rightExpr)
                case "->>" =>
                    SqlExpr.Binary(leftExpr, SqlBinaryOperator.JsonText, rightExpr)
                case "like" | "contains" | "startsWith" | "endsWith" =>
                    SqlExpr.Binary(leftExpr, SqlBinaryOperator.Like, rightExpr)

        ExprInfo(
            expr = expr,
            hasAgg = leftInfo.hasAgg || rightInfo.hasAgg,
            isAgg = false,
            hasWindow = leftInfo.hasWindow || rightInfo.hasWindow,
            isValue = false,
            ungroupedPaths = leftInfo.ungroupedPaths ++ rightInfo.ungroupedPaths,
            notInAggPaths = leftInfo.notInAggPaths ++ rightInfo.notInAggPaths
        )

    def createInExpr(using q: Quotes, c: QueryContext)(
        left: q.reflect.Term,
        right: q.reflect.Term,
        allTables: List[(argName: String, tableName: String)],
        inConnectBy: Boolean,
        groupInfo: List[GroupInfo]
    ): ExprInfo =
        val leftInfo = createExpr(left, allTables, inConnectBy, groupInfo)
        val rightInfo = right.tpe.widen.asType match
            case '[type t <: Seq[?] | Array[?]; t] =>
                ExprInfo(
                    expr = SqlExpr.Tuple(SqlExpr.NumberLiteral(1) :: Nil),
                    hasAgg = false,
                    isAgg = false,
                    hasWindow = false,
                    isValue = false,
                    ungroupedPaths = Nil,
                    notInAggPaths = Nil
                )
            case '[Query[?]] =>
                val queryInfo = createTree(right, allTables, groupInfo)
                val expr = SqlExpr.SubQuery(queryInfo.tree)
                ExprInfo(
                    expr = expr,
                    hasAgg = false,
                    isAgg = false,
                    hasWindow = false,
                    isValue = false,
                    ungroupedPaths = Nil,
                    notInAggPaths = Nil
                )
            case _ => 
                createExpr(right, allTables, inConnectBy, groupInfo)

        val leftExpr = leftInfo.expr
        val rightExpr = rightInfo.expr

        val expr = SqlExpr.Binary(leftExpr, SqlBinaryOperator.In, rightExpr)

        ExprInfo(
            expr = expr,
            hasAgg = leftInfo.hasAgg || rightInfo.hasAgg,
            isAgg = false,
            hasWindow = leftInfo.hasWindow || rightInfo.hasWindow,
            isValue = false,
            ungroupedPaths = leftInfo.ungroupedPaths ++ rightInfo.ungroupedPaths,
            notInAggPaths = leftInfo.notInAggPaths ++ rightInfo.notInAggPaths
        )

    def createBetweenExpr(using q: Quotes, c: QueryContext)(
        left: q.reflect.Term,
        start: q.reflect.Term,
        end: q.reflect.Term,
        allTables: List[(argName: String, tableName: String)],
        inConnectBy: Boolean,
        groupInfo: List[GroupInfo]
    ): ExprInfo =
        val leftInfo = createExpr(left, allTables, inConnectBy, groupInfo)
        val startInfo = createExpr(start, allTables, inConnectBy, groupInfo)
        val endInfo = createExpr(end, allTables, inConnectBy, groupInfo)

        val leftExpr = leftInfo.expr
        val startExpr = startInfo.expr
        val endExpr = endInfo.expr

        val expr = SqlExpr.Between(leftExpr, startExpr, endExpr, false)

        ExprInfo(
            expr = expr,
            hasAgg = leftInfo.hasAgg || startInfo.hasAgg || endInfo.hasAgg,
            isAgg = false,
            hasWindow = leftInfo.hasWindow || startInfo.hasWindow || endInfo.hasAgg,
            isValue = false,
            ungroupedPaths = leftInfo.ungroupedPaths ++ startInfo.ungroupedPaths ++ endInfo.ungroupedPaths,
            notInAggPaths = leftInfo.notInAggPaths ++ startInfo.notInAggPaths ++ endInfo.notInAggPaths
        )

    def createAggExpr(using q: Quotes, c: QueryContext)(
        name: String,
        term: q.reflect.Term,
        allTables: List[(argName: String, tableName: String)],
        inConnectBy: Boolean,
        groupInfo: List[GroupInfo]
    ): ExprInfo =
        import q.reflect.*

        val params = 
            term.symbol.paramSymss
                .flatMap(a => a)
                .filter(a => !a.isTypeParam && !a.flags.is(Flags.Given))

        val args = term match
            case Apply(Apply(Apply(func, args), _), _) =>
                args
            case Apply(Apply(func, args), _) =>
                args
            case Apply(func, args) =>
                args

        val argsMap =
            params.map(_.name).zip(
                args.map:
                    case NamedArg(_, arg) => arg
                    case arg => arg
            ).toMap

        val expr =
            if name == "PERCENTILE_CONT" || name == "PERCENTILE_DISC" then
                val n = argsMap.get("n").map(_.asExprOf[Double].value).flatten
                n.foreach: v =>
                    if v > 1.0 || v < 0 then
                        report.warning(s"Percentile value \"$v\" is not between 0 and 1.")
            
            val distinct = argsMap
                .get("distinct")
                .map(d => d.asExprOf[Boolean].value)
                .flatten
                .getOrElse(false)
            val param = if distinct then Some(SqlParam.Distinct) else None
            
            var hasAgg = false
            var hasWindow = false

            val orderByInfo = argsMap
                .get("orderBy")
                .orElse(argsMap.get("sortBy"))
                .map(a => createOrderBy(a, allTables, inConnectBy, groupInfo))
            for o <- orderByInfo do
                hasAgg = hasAgg || o.info.hasAgg
                hasWindow = hasWindow || o.info.hasWindow
            val orderBy = orderByInfo
                .map: o => 
                    SqlOrderItem(o.info.expr, Some(o.order), o.nullsOrder)

            val withinGroupInfo = argsMap
                .get("withinGroup")
                .map(a => createOrderBy(a, allTables, inConnectBy, groupInfo))
            for w <- withinGroupInfo do
                hasAgg = hasAgg || w.info.hasAgg
                hasWindow = hasWindow || w.info.hasWindow
            val withinGroup = withinGroupInfo 
                .map: o => 
                    SqlOrderItem(o.info.expr, Some(o.order), o.nullsOrder)

            val filterInfo = argsMap
                .get("filter")
                .map(f => createExpr(f, allTables, inConnectBy, groupInfo))
            for f <- filterInfo do
                hasAgg = hasAgg || f.hasAgg
                hasWindow = hasWindow || f.hasWindow
            val filter = filterInfo.map(_.expr)

            val otherInfo = argsMap
                .filterKeys(k => !List("distinct, orderBy", "sortBy", "withinGroup", "filter").contains(k))
                .values
                .toList
                .map(a => createExpr(a, allTables, inConnectBy, groupInfo))
            for o <- otherInfo do
                hasAgg = hasAgg || o.hasAgg
                hasWindow = hasWindow || o.hasWindow
            val funcArgs = otherInfo.map(_.expr)

            if distinct && !orderBy.map(_.expr).forall(funcArgs.contains) then
                report.warning("Aggregates with DISTINCT must be able to sort their inputs.")

            if name == "GROUPING" then
                val exprs = otherInfo.map(_.expr)
                val isGrouped =
                    exprs.forall(groupInfo.flatMap(_.groupedExprs).contains)
                if !isGrouped then
                    report.warning("Arguments to GROUPING must be grouping expressions of the associated query level.")
            
            if hasAgg then
                report.warning("Aggregate function calls cannot be nested.")
            if hasWindow then
                report.warning("Aggregate function calls cannot contain window function calls.")
            
            SqlExpr.Func(name, funcArgs, param, orderBy.toList, withinGroup.toList, filter)

        ExprInfo(
            expr = expr,
            hasAgg = true,
            isAgg = true,
            hasWindow = false,
            isValue = false,
            ungroupedPaths = Nil,
            notInAggPaths = Nil
        )

    def createFuncExpr(using q: Quotes, c: QueryContext)(
        name: String,
        term: q.reflect.Term,
        isWindow: Boolean,
        allTables: List[(argName: String, tableName: String)],
        inConnectBy: Boolean,
        groupInfo: List[GroupInfo]
    ): ExprInfo =
        import q.reflect.*

        val params = 
            term.symbol.paramSymss
                .flatMap(a => a)
                .filter(a => !a.isTypeParam && !a.flags.is(Flags.Given))

        val args = term match
            case Apply(Apply(Apply(func, args), _), _) =>
                args
            case Apply(Apply(func, args), _) =>
                args
            case Apply(func, args) =>
                args

        val argsMap =
            params.map(_.name).zip(
                args.map:
                    case NamedArg(_, arg) => arg
                    case arg => arg
            ).toMap

        val (expr, hasAgg, hasWindow, ungroupedPaths, notInAggPaths) = 
            val distinct = argsMap
                .get("distinct")
            if distinct.isDefined then
                report.warning(s"DISTINCT specified, but \"${name}\" is not an aggregate function.")
            
            val orderBy = argsMap
                .get("orderBy")
                .orElse(argsMap.get("sortBy"))
            if orderBy.isDefined then
                report.warning(s"ORDER BY specified, but \"${name}\" is not an aggregate function.")
            
            val withinGroup = argsMap
                .get("withinGroup")
            if withinGroup.isDefined then
                report.warning(s"WITHIN GROUP specified, but \"${name}\" is not an aggregate function.")
            
            val filter = argsMap
                .get("filter")
            if filter.isDefined then
                report.warning(s"FILTER specified, but \"${name}\" is not an aggregate function.")
            
            val otherInfo = argsMap
                .filterKeys(k => !List("distinct, orderBy", "sortBy", "withinGroup", "filter").contains(k))
                .values
                .toList
                .map(a => createExpr(a, allTables, inConnectBy, groupInfo))
            val funcArgs = otherInfo.map(_.expr)
            
            val ungroupedPaths = otherInfo.flatMap(_.ungroupedPaths)
            val notInAggPaths = otherInfo.flatMap(_.notInAggPaths)
            val hasAgg = otherInfo.map(_.hasAgg).fold(false)(_ || _)
            val hasWindow = otherInfo.map(_.hasWindow).fold(false)(_ || _)

            if isWindow && hasWindow then
                report.warning("Window function calls cannot be nested.")

            val expr = SqlExpr.Func(name, funcArgs, None, Nil, Nil, None)
            (expr, hasAgg, hasWindow, ungroupedPaths, notInAggPaths)

        if isWindow then
            ExprInfo(
                expr = expr,
                hasAgg = false,
                isAgg = false,
                hasWindow = true,
                isValue = false,
                ungroupedPaths = ungroupedPaths,
                notInAggPaths = notInAggPaths
            )
        else
            ExprInfo(
                expr = expr,
                hasAgg = hasAgg,
                isAgg = false,
                hasWindow = hasWindow,
                isValue = false,
                ungroupedPaths = ungroupedPaths,
                notInAggPaths = notInAggPaths
            )

    def createWindowExpr(using q: Quotes, c: QueryContext)(
        term: q.reflect.Term,
        over: q.reflect.Term,
        allTables: List[(argName: String, tableName: String)],
        inConnectBy: Boolean,
        groupInfo: List[GroupInfo]
    ): ExprInfo =
        import q.reflect.*

        val (info, isWindowOrAgg) =
            term match
                case _ 
                    if term.symbol.annotations.exists:
                        case Apply(Select(New(TypeIdent("window")), _), _) => true
                        case _ => false
                =>
                    val name = term.symbol.annotations.collectFirst:
                        case Apply(Select(New(TypeIdent("window")), _), Literal(StringConstant(n)) :: Nil) => n
                    .get
                    val info = createFuncExpr(name, term, true, allTables, inConnectBy, groupInfo)
                    (info, true)
                case _ =>
                    val info = createExpr(term, allTables, inConnectBy, groupInfo)
                    (info, info.isAgg)
        
        if !isWindowOrAgg then
            report.warning(s"OVER specified, but \"${info.code}\" is not a window function nor an aggregate function.")

        val funcUngroupedPaths = 
            if info.isAgg then Nil else info.ungroupedPaths
        val funcNotInAggPaths =
            if info.isAgg then Nil else info.notInAggPaths

        over.tpe.widen.asType match
            case '[Unit] =>
                val expr = SqlExpr.Window(info.expr, Nil, Nil, None)
                ExprInfo(
                    expr = expr,
                    hasAgg = false,
                    isAgg = false,
                    hasWindow = true,
                    isValue = false,
                    ungroupedPaths = funcUngroupedPaths,
                    notInAggPaths = funcNotInAggPaths
                )
            case _ =>
                val (frame, remaining) =
                    over match
                        case Apply(Select(o, name@("rowsBetween" | "rangeBetween" | "groupsBetween")), start :: end :: Nil) =>
                            def fetchFrameOption(item: Term): SqlWindowFrameOption =
                                item match
                                    case Apply(Apply(Ident("preceding"), n :: Nil), _) =>
                                        val value = n.asExprOf[Int].value.getOrElse(0)
                                        SqlWindowFrameOption.Preceding(value)
                                    case Apply(Apply(Ident("following"), n :: Nil), _) =>
                                        val value = n.asExprOf[Int].value.getOrElse(0)
                                        SqlWindowFrameOption.Following(value)
                                    case Apply(Ident("currentRow"), _) =>
                                        SqlWindowFrameOption.CurrentRow
                                    case Apply(Ident("unboundedPreceding"), _) =>
                                        SqlWindowFrameOption.UnboundedPreceding
                                    case Apply(Ident("unboundedFollowing"), _) =>
                                        SqlWindowFrameOption.UnboundedFollowing
                            
                            val startOption = fetchFrameOption(start)
                            val endOption = fetchFrameOption(end)

                            val frame = name match
                                case "rowsBetween" => SqlWindowFrame.Rows(startOption, endOption)
                                case "rangeBetween" => SqlWindowFrame.Range(startOption, endOption)
                                case "groupsBetween" => SqlWindowFrame.Groups(startOption, endOption)

                            (Some(frame), o)
                        case _ => (None, over)
                
                val (partition, order) =
                    remaining match
                        case Apply(Apply(TypeApply(Ident("partitionBy"), _), partition :: Nil), _) =>
                            (Some(partition), None)
                        case Apply(Apply(TypeApply(Ident("sortBy" | "orderBy"), _), order :: Nil), _) =>
                            (None, Some(order))
                        case Apply(Apply(TypeApply(Select(Apply(Apply(TypeApply(Ident("partitionBy"), _), partition :: Nil), _), "sortBy" | "orderBy"), _), order :: Nil), _) =>
                            (Some(partition), Some(order))
                    
                val partitionInfo = partition.map: p =>
                    p match
                        case Apply(TypeApply(Select(Ident(typeName), "apply"), _), terms) 
                            if typeName.startsWith("Tuple")
                        =>
                            terms.map(t => createExpr(t, allTables, inConnectBy, groupInfo))
                        case _ =>
                            createExpr(p, allTables, inConnectBy, groupInfo) :: Nil
                .getOrElse(Nil)
                val partitionList = partitionInfo
                    .map(_.expr)

                val orderInfo = order.map: o =>
                    o match
                        case Apply(TypeApply(Select(Ident(typeName), "apply"), _), terms) 
                            if typeName.startsWith("Tuple")
                        =>
                            terms.map(t => createOrderBy(t, allTables, inConnectBy, groupInfo))
                        case _ =>
                            createOrderBy(o, allTables, inConnectBy, groupInfo) :: Nil
                .getOrElse(Nil)
                val orderList = orderInfo
                    .map(o => SqlOrderItem(o.info.expr, Some(o.order), o.nullsOrder))

                val hasWindow = 
                    partitionInfo.map(_.hasWindow).fold(false)(_ || _) ||
                    orderInfo.map(_.info.hasWindow).fold(false)(_ || _)

                val ungroupedPaths =
                    partitionInfo.flatMap(_.ungroupedPaths) ++
                    orderInfo.flatMap(_.info.ungroupedPaths) ++
                    funcUngroupedPaths

                val notInAggPaths =
                    partitionInfo.flatMap(_.notInAggPaths) ++
                    orderInfo.flatMap(_.info.notInAggPaths) ++
                    funcNotInAggPaths

                if hasWindow then
                    report.warning("Window function calls cannot be nested.")

                val expr = SqlExpr.Window(info.expr, partitionList, orderList, frame)
                ExprInfo(
                    expr = expr,
                    hasAgg = false,
                    isAgg = false,
                    hasWindow = true,
                    isValue = false,
                    ungroupedPaths = ungroupedPaths,
                    notInAggPaths = notInAggPaths
                )

    def createOrderBy(using q: Quotes, c: QueryContext)(
        term: q.reflect.Term,
        allTables: List[(argName: String, tableName: String)],
        inConnectBy: Boolean,
        groupInfo: List[GroupInfo]
    ): (info: ExprInfo, order: SqlOrderOption, nullsOrder: Option[SqlOrderNullsOption]) =
        import q.reflect.*

        term match
            case Apply(Apply(Apply(TypeApply(Ident("asc"), _), expr :: Nil), _), _) => 
                (createExpr(expr, allTables, inConnectBy, groupInfo), SqlOrderOption.Asc, None)
            case Apply(Apply(Apply(TypeApply(Ident("ascNullsFirst"), _), expr :: Nil), _), _) => 
                (createExpr(expr, allTables, inConnectBy, groupInfo), SqlOrderOption.Asc, Some(SqlOrderNullsOption.First))
            case Apply(Apply(Apply(TypeApply(Ident("ascNullsLast"), _), expr :: Nil), _), _) => 
                (createExpr(expr, allTables, inConnectBy, groupInfo), SqlOrderOption.Asc, Some(SqlOrderNullsOption.Last))
            case Apply(Apply(Apply(TypeApply(Ident("desc"), _), expr :: Nil), _), _) => 
                (createExpr(expr, allTables, inConnectBy, groupInfo), SqlOrderOption.Desc, None)
            case Apply(Apply(Apply(TypeApply(Ident("descNullsFirst"), _), expr :: Nil), _), _) => 
                (createExpr(expr, allTables, inConnectBy, groupInfo), SqlOrderOption.Desc, Some(SqlOrderNullsOption.First))
            case Apply(Apply(Apply(TypeApply(Ident("descNullsLast"), _), expr :: Nil), _), _) => 
                (createExpr(expr, allTables, inConnectBy, groupInfo), SqlOrderOption.Desc, Some(SqlOrderNullsOption.Last))
            case _ =>
                (createExpr(term, allTables, inConnectBy, groupInfo), SqlOrderOption.Asc, None)