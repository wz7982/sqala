package sqala.optimizer.logic

import sqala.optimizer.*

object PullUpSubQuery:
    def pullUpAnySubLink(query: Query): Query =
        if query.filter.isDefined then
            val conjunctList = collectConjunct(query.filter.get)
            var tmpIndex = 0
            def pullUp(expr: Expr, tables: List[TableEntry], from: FromExpr): (List[TableEntry], FromExpr, List[Expr]) =
                expr match
                    case Expr.Binary(left, op, Expr.SubLink(query, SubLinkType.Any)) =>
                        // TODO 检测子连接提升的条件
                        val queryAlias = s"?any_sub_link?$tmpIndex"

                        val newTable = 
                            TableEntry.SubQuery(query, false, TableAlias(queryAlias, Nil))
                        val newTables = tables.appended(newTable)

                        val tableIndex = newTables.size - 1

                        val leftExprList = left match
                            case Expr.Vector(items) => items
                            case e => e :: Nil

                        val condition = leftExprList.zip(query.target).zipWithIndex.map: 
                            case ((l, r), i) =>
                                val rightVar = Var(tableIndex, Some(queryAlias), i, r.alias, 0)
                                Expr.Binary(l, op, Expr.VarRef(rightVar))
                        .reduce((x, y) => Expr.Binary(x, BinaryOperator.And, y))

                        val newFrom = FromExpr.JoinExpr(
                            from,
                            JoinType.Semi,
                            FromExpr.TableRef(tableIndex),
                            condition
                        )

                        tmpIndex += 1
                        
                        (newTables, newFrom, Nil)

                    case Expr.Binary(left, BinaryOperator.In, Expr.SubQuery(query)) =>
                        pullUp(Expr.Binary(left, BinaryOperator.Equal, Expr.SubLink(query, SubLinkType.Any)), tables, from)
                    // TODO not in 提升成反连接
                    case _ => (tables, from, expr :: Nil)
            // TODO on中的子连接
            // TODO其他表达式的子查询中的子连接
            val initialInfo = 
                pullUp(conjunctList.head, query.tableList, query.joinTree)

            val (tables, from, cond) = 
                if conjunctList.tail.isEmpty then initialInfo
                else
                    conjunctList.tail.foldLeft(initialInfo): (acc, e) =>  
                        val tmpInfo = pullUp(e, acc._1, acc._2)  
                        (tmpInfo._1, tmpInfo._2, acc._3.toList ++ tmpInfo._3.toList) 
            
            query.copy(
                tableList = tables, 
                joinTree = from, 
                filter = cond.reduceOption((x, y) => Expr.Binary(x, BinaryOperator.And, y))
            )
        else query

    def pullUpSubQuery(query: Query): Query =
        var tmpTableList = query.tableList
        var tmpFrom = query.joinTree
        val tableCount = query.tableList.size

        // 修改子查询的from的标号，以便放入外层查询中
        def modifySubQueryJoinTree(tree: FromExpr, subQueryTableCount: Int): FromExpr =
            def modifyExpr(expr: Expr): Expr =
                expr match
                    case Expr.VarRef(v) =>
                        Expr.VarRef(v.copy(tableIndex = v.tableIndex + tableCount))
                    case e => modifyAnalysisExpr(modifyExpr)(e)

            tree match
                case FromExpr.TableRef(tableIndex) => 
                    FromExpr.TableRef(tableIndex + subQueryTableCount)
                case FromExpr.JoinExpr(left, joinType, right, condition) => 
                    FromExpr.JoinExpr(
                        modifySubQueryJoinTree(left, subQueryTableCount),
                        joinType,
                        modifySubQueryJoinTree(right, subQueryTableCount),
                        modifyExpr(condition)
                    )

        // 修改外侧查询的join树，把子查询引用变成表引用节点
        def modifyJoinTree(index: Int, tree: FromExpr, newTree: FromExpr, subQueryTargetList: List[TargetEntry]): FromExpr =
            def modifyVar(expr: Expr): Expr = 
                expr match
                    case Expr.VarRef(v) =>
                        val tableName = tmpTableList(v.tableIndex + tableCount) match
                            case TableEntry.Relation(_, alias) => alias.map(_.alias)
                            case TableEntry.SubQuery(_, _, alias) => Some(alias.alias)
                        Expr.VarRef(v.copy(tableIndex = v.tableIndex + tableCount, tableName = tableName))
                    case e => modifyAnalysisExpr(modifyVar)(e)
            
            def modifyExpr(expr: Expr): Expr =
                expr match
                    case Expr.VarRef(v) if v.tableIndex == index =>
                        val newExpr = subQueryTargetList
                            .find(_.alias == v.varName)
                            .map(_.expr)
                            .get
                        modifyVar(newExpr)
                    case e => modifyAnalysisExpr(modifyExpr)(e)

            tree match
                case FromExpr.TableRef(tableIndex) if tableIndex == index =>
                    newTree 
                case t: FromExpr.TableRef => t
                case FromExpr.JoinExpr(left, joinType, right, condition) =>
                    FromExpr.JoinExpr(
                        modifyJoinTree(index, left, newTree, subQueryTargetList),
                        joinType,
                        modifyJoinTree(index, right, newTree, subQueryTargetList),
                        // TODO 修改条件 中的VAR
                        modifyExpr(condition)
                    )  

        // TODO 判断子查询提升的条件
        for (t, i) <- query.tableList.zipWithIndex do
            t match
                case TableEntry.SubQuery(subQuery, false, alias) =>
                    val q = pullUpSubQuery(subQuery)
                    tmpTableList = tmpTableList.appendedAll(q.tableList)
                    val subQueryFrom = modifySubQueryJoinTree(subQuery.joinTree, q.tableList.size)
                    tmpFrom = modifyJoinTree(i, tmpFrom, subQueryFrom, q.target)
                case _ =>

        
        // TODO 修改target 中的var
        // TODO 修改filter 中的var

        query.copy(tableList = tmpTableList, joinTree = tmpFrom)