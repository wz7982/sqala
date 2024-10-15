package sqala.optimizer.analysis

import sqala.ast.expr.*
import sqala.ast.group.SqlGroupItem
import sqala.ast.order.SqlOrderBy
import sqala.ast.statement.*
import sqala.ast.table.*
import sqala.optimizer.*
import sqala.util.*

object AnalysisTarget:
    def addAlias(query: SqlQuery): SqlQuery =
        query match
            case s: SqlQuery.Select =>
                val newQuery = s.copy(
                    select = s.select.zipWithIndex.map: (item, index) =>
                        item match
                            case SqlSelectItem.Item(SqlExpr.Column(tableName, columnName), None) =>
                                SqlSelectItem.Item(SqlExpr.Column(tableName, columnName), Some(columnName))
                            case SqlSelectItem.Item(expr, None) =>
                                SqlSelectItem.Item(expr, Some(s"?column?$index"))
                            case i => i
                    ,
                    from = s.from.map(addAlias)
                )
                newQuery |> modifyQueryExpr(addAlias)
            case q => 
                q |> modifyQuery(addAlias) |> modifyQueryExpr(addAlias)

    def addAlias(table: SqlTable): SqlTable =
        table match
            case SqlTable.SubQueryTable(query, lateral, alias) =>
                SqlTable.SubQueryTable(addAlias(query), lateral, alias)
            case t =>
                t |> modifyTable(addAlias) |> modifyJoinCondition(addAlias)

    def addAlias(expr: SqlExpr): SqlExpr =
        expr match
            case SqlExpr.SubQuery(query) =>
                SqlExpr.SubQuery(addAlias(query))
            case SqlExpr.SubLink(query, linkType) =>
                SqlExpr.SubLink(addAlias(query), linkType)
            case e => modidyExpr(addAlias)(e)

    def expandStar(query: SqlQuery, metaData: List[TableMetaData]): SqlQuery =
        query match
            case s: SqlQuery.Select =>
                val expandStarTables = s.from.map(t => expandStar(t, metaData))
                val columns = expandStarTables.flatMap(t => fetchColumns(t, metaData))
                var index = 0
                val target = s.select.flatMap:
                    case i: SqlSelectItem.Item => i :: Nil
                    case SqlSelectItem.Wildcard(table) =>
                        val expandColumns = table match
                            case None => columns
                            case Some(n) => columns.filter(c => c.tableName == Some(n))
                        expandColumns.map: c =>
                            val item = SqlSelectItem.Item(c, Some(c.columnName))
                            index += 1
                            item
                val newAst = s.copy(select = target, from = expandStarTables)
                newAst |> modifyQueryExpr(e => expandStar(e, metaData))
            case q => 
                q |> 
                modifyQuery(q => expandStar(q, metaData)) |> 
                modifyQueryExpr(e => expandStar(e, metaData))
    
    def expandStar(table: SqlTable, metaData: List[TableMetaData]): SqlTable =
        table match
            case SqlTable.SubQueryTable(query, lateral, alias) =>
                SqlTable.SubQueryTable(expandStar(query, metaData), lateral, alias)
            case t => 
                t |> 
                modifyTable(x => expandStar(x, metaData)) |>
                modifyJoinCondition(x => expandStar(x, metaData))

    def expandStar(expr: SqlExpr, metaData: List[TableMetaData]): SqlExpr =
        expr match
            case SqlExpr.SubQuery(query) =>
                SqlExpr.SubQuery(expandStar(query, metaData))
            case SqlExpr.SubLink(query, linkType) =>
                SqlExpr.SubLink(expandStar(query, metaData), linkType)
            case e => modidyExpr(e => expandStar(e, metaData))(e)

    def fetchColumns(table: SqlTable, metaData: List[TableMetaData]): List[SqlExpr.Column] =
        table match
            case SqlTable.IdentTable(tableName, alias) =>
                val tableAlias = alias.map(_.tableAlias).getOrElse(tableName)
                val columnAlias = alias.map(_.columnAlias).getOrElse(Nil)
                val tableMetaData = metaData.find(m => m.name == tableName).get
                tableMetaData.columns.zipWithIndex.map: (m, i) =>
                    val columnName = columnAlias.lift(i).getOrElse(m.name)
                    SqlExpr.Column(Some(tableAlias), columnName)
            case SqlTable.SubQueryTable(query, _, alias) =>
                def fetchQueryItems(q: SqlQuery): List[SqlSelectItem.Item] =
                    q match
                        case s: SqlQuery.Select =>
                            s.select.flatMap:
                                case i: SqlSelectItem.Item => i :: Nil
                                case _ => Nil
                        case u: SqlQuery.Union =>
                            fetchQueryItems(u.left)
                        case _ => Nil
                val items = fetchQueryItems(query).filter(_.alias.isDefined).map(_.alias.get)
                for (i, l) <- items.groupBy(i => i) do
                    if l.size > 1 then throw new AnalysisException(s"Duplicate column $i.")
                items.zipWithIndex.map: (x, i) =>
                    SqlExpr.Column(Some(alias.tableAlias), alias.columnAlias.lift(i).getOrElse(x))
            case t =>
                analysisTable(Nil)(t => fetchColumns(t, metaData))(_ ++ _)(t)