package sqala.optimizer.analysis

import sqala.ast.expr.SqlExpr
import sqala.ast.statement.SqlQuery
import sqala.ast.table.*
import sqala.util.*
import sqala.optimizer.*

object AnalysisFrom:
    def analysisFrom(query: SqlQuery): SqlQuery =
        query match
            case s@SqlQuery.Select(param, select, from, where, groupBy, having, orderBy, limit) =>
                val tableList = from.map(analysisFrom)
                val aliasList = tableList.flatMap(f => fetchAlias(f))
                val groupAlias = aliasList.groupBy(i => i).mapValues(_.size)
                for (alias, count) <- groupAlias if count > 1 do
                    throw new AnalysisException(s"Table name $alias appears more than once in FROM clause.")
                val joinTree = 
                    tableList.reduceLeftOption: (x, y) => 
                        SqlTable.JoinTable(x, SqlJoinType.InnerJoin, y, Some(SqlJoinCondition.On(SqlExpr.BooleanLiteral(true))))
                s.copy(from = joinTree.toList) |> modifyQueryExpr(analysisFrom)
            case q => q |> modifyQuery(analysisFrom) |> modifyQueryExpr(analysisFrom)

    def fetchAlias(table: SqlTable): List[String] =
        table match
            case SqlTable.IdentTable(_, Some(SqlTableAlias(tableName, _))) => 
                List(tableName)
            case SqlTable.SubQueryTable(_, _, SqlTableAlias(tableName, _)) => 
                List(tableName)
            case SqlTable.JoinTable(left, _, right, _) =>
                fetchAlias(left) ++ fetchAlias(right)
            case _ => Nil

    def analysisFrom(table: SqlTable): SqlTable =
        table match
            case SqlTable.IdentTable(tableName, None) =>
                SqlTable.IdentTable(tableName, Some(SqlTableAlias(tableName, Nil)))
            case t@SqlTable.IdentTable(_, Some(SqlTableAlias(_, _))) => 
                t
            case t@SqlTable.SubQueryTable(query, _, SqlTableAlias(tableName, columnNames)) => 
                t.copy(query = analysisFrom(query))
            case t =>
                t |> modifyTable(analysisFrom) |> modifyJoinCondition(analysisFrom)

    def analysisFrom(expr: SqlExpr): SqlExpr =
        expr match
            case SqlExpr.SubQuery(query) =>
                SqlExpr.SubQuery(analysisFrom(query))
            case SqlExpr.SubLink(query, linkType) =>
                SqlExpr.SubLink(analysisFrom(query), linkType)
            case e => expr |> modidyExpr(analysisFrom)

    def validateTableName(query: SqlQuery, metaData: List[TableMetaData]): SqlQuery =
        val tableNames = fetchTableNames(query)
        val notExists = tableNames.find(n => !metaData.map(_.name).contains(n))
        notExists match
            case None => query
            case Some(n) => throw new AnalysisException(s"Table $n does not exist.")

    def fetchTableNames(query: SqlQuery): List[String] =
        query match
            case s: SqlQuery.Select =>
                s.from.flatMap(fetchTableNames) ++ 
                analysisQueryExpr(Nil)(fetchTableNames)(_ ++ _)(s)
            case q =>
                analysisQuery(Nil)(fetchTableNames)(_ ++ _)(q) ++
                analysisQueryExpr(Nil)(fetchTableNames)(_ ++ _)(q)

    def fetchTableNames(table: SqlTable): List[String] =
        table match
            case SqlTable.IdentTable(tableName, _) => 
                tableName :: Nil
            case SqlTable.SubQueryTable(query, _, _) =>
                fetchTableNames(query) ++ 
                analysisQueryExpr(Nil)(fetchTableNames)(_ ++ _)(query)
            case t => 
                analysisTable(Nil)(fetchTableNames)(_ ++ _)(t) ++ 
                analysisJoinCondition(Nil)(fetchTableNames)(_ ++ _)(t)

    def fetchTableNames(expr: SqlExpr): List[String] =
        expr match
            case SqlExpr.SubQuery(query) => fetchTableNames(query)
            case SqlExpr.SubLink(query, _) => fetchTableNames(query)
            case e => analysisExpr(Nil)(fetchTableNames)(_ ++ _)(e)