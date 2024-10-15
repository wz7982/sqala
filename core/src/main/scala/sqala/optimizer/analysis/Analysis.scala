package sqala.optimizer.analysis

import sqala.ast.expr.*
import sqala.ast.group.*
import sqala.ast.order.*
import sqala.ast.statement.*
import sqala.ast.table.*
import sqala.optimizer.*
import sqala.util.*

object Analysis:
    def analysis(
        query: SqlQuery, 
        tableMetaData: List[TableMetaData], 
        funcMetaData: List[FuncMetaData]
    ): Query =
        query |>
        (q => AnalysisFrom.validateTableName(q, tableMetaData)) |>
        AnalysisFrom.analysisFrom |>
        AnalysisTarget.addAlias |>
        (q => AnalysisTarget.expandStar(q, tableMetaData)) |>
        (q => createQueryTree(1, q, Nil, tableMetaData, funcMetaData))

    case class TableColumns(level: Int, tableAlias: String, columnAlias: List[String])

    def createVar(currentLevel: Int, column: SqlExpr.Column, tableColumns: List[TableColumns]): Var =
        column match
            case SqlExpr.Column(None, columnName) =>
                var tmpVar: Option[Var] = None
                var continue = true

                for l <- currentLevel to 1 by -1 if (tmpVar eq None) && continue do
                    val currentTables = tableColumns
                        .filter(t => t.level == l)
                        .zipWithIndex
                        .filter((t, _) => t.columnAlias.contains(columnName))

                    if currentTables.size > 1
                    then throw new AnalysisException(s"Column reference $columnName is ambiguous.")

                    if currentTables.nonEmpty then
                        continue = false
                        val (t, ti) = currentTables.head
                        val column = t.columnAlias.zipWithIndex.find((c, _) => c == columnName).head
                        tmpVar = Some(Var(ti, None, column._2, column._1, currentLevel - l))

                if tmpVar eq None 
                then throw new AnalysisException(s"Column $columnName does not exist.")
                else tmpVar.get
            case SqlExpr.Column(Some(tableName), columnName) =>
                var tmpVar: Option[Var] = None
                var continue = true

                for l <- currentLevel to 1 by -1 if (tmpVar eq None) && continue do
                    val currentTables = tableColumns
                        .filter(t => t.level == l)
                        .zipWithIndex
                        .filter((t, _) => t.tableAlias == tableName)

                    if currentTables.nonEmpty then continue = false

                    for 
                        (t, ti) <- currentTables
                        (cn, ci) <- t.columnAlias.zipWithIndex if cn == columnName
                    do
                        tmpVar = Some(Var(ti, Some(t.tableAlias), ci, columnName, currentLevel - l))

                if tmpVar eq None 
                then throw new AnalysisException(s"Column $tableName.$columnName does not exist.")
                else tmpVar.get

    def createSort(
        currentLevel: Int, 
        order: SqlOrderBy,
        tableColumns: List[TableColumns],
        tableMetaData: List[TableMetaData],
        funcMetaData: List[FuncMetaData]
    ): Sort = 
        Sort(
            createExpr(currentLevel, order.expr, tableColumns, tableMetaData, funcMetaData),
            order.order match
                case None => true
                case Some(SqlOrderByOption.Asc) => true
                case _ => false
        )

    def createExpr(
        currentLevel: Int, 
        expr: SqlExpr, 
        tableColumns: List[TableColumns],
        tableMetaData: List[TableMetaData],
        funcMetaData: List[FuncMetaData]
    ): Expr =
        import SqlExpr.*

        val f = e => 
            createExpr(currentLevel, e, tableColumns, tableMetaData, funcMetaData)

        val s = o =>
            createSort(currentLevel, o, tableColumns, tableMetaData, funcMetaData)

        expr match
            case Null => 
                Expr.Null
            case c: Column => 
                Expr.VarRef(createVar(currentLevel, c, tableColumns))
            case StringLiteral(string) => 
                Expr.StringConstant(string)
            case NumberLiteral(number) => 
                Expr.NumberConstant(number)
            case BooleanLiteral(boolean) => 
                Expr.BooleanConstant(boolean)
            case Vector(items) => 
                Expr.Vector(items.map(f))
            case Unary(expr, op) =>
                val exprOp = op match
                    case SqlUnaryOperator.Positive => UnaryOperator.Positive
                    case SqlUnaryOperator.Negative => UnaryOperator.Negative
                    case SqlUnaryOperator.Not => UnaryOperator.Not
                    case SqlUnaryOperator.Custom(x) => 
                        throw new AnalysisException(s"Operator $op does not exist.")
                Expr.Unary(f(expr), exprOp)
            case Binary(left, op, right) =>
                val exprOp = op match
                    case SqlBinaryOperator.Times => BinaryOperator.Times
                    case SqlBinaryOperator.Div => BinaryOperator.Div
                    case SqlBinaryOperator.Mod => BinaryOperator.Mod
                    case SqlBinaryOperator.Plus => BinaryOperator.Plus
                    case SqlBinaryOperator.Minus => BinaryOperator.Minus
                    case SqlBinaryOperator.Equal => BinaryOperator.Equal
                    case SqlBinaryOperator.NotEqual => BinaryOperator.NotEqual
                    case SqlBinaryOperator.In => BinaryOperator.In
                    case SqlBinaryOperator.NotIn => BinaryOperator.NotIn
                    case SqlBinaryOperator.GreaterThan => BinaryOperator.GreaterThan
                    case SqlBinaryOperator.GreaterThanEqual => BinaryOperator.GreaterThanEqual
                    case SqlBinaryOperator.LessThan => BinaryOperator.LessThan
                    case SqlBinaryOperator.LessThanEqual => BinaryOperator.LessThanEqual
                    case SqlBinaryOperator.Like => BinaryOperator.Like
                    case SqlBinaryOperator.NotLike => BinaryOperator.NotLike
                    case SqlBinaryOperator.And => BinaryOperator.And
                    case SqlBinaryOperator.Or => BinaryOperator.Or
                    case x => 
                       throw new AnalysisException(s"Operator ${x.operator} does not exist.")
                Expr.Binary(f(left), exprOp, f(right))
            case NullTest(expr, not) =>
                Expr.NullTest(f(expr), not)
            case Func(name, args, distinct, orderBy, _, _) =>
                val func = funcMetaData.find(m => m.name == name).get
                if func.kind == FuncKind.Normal then
                    Expr.Func(name, args.map(f))
                else Expr.Agg(name, args.map(f), distinct, orderBy.map(s))
            case Between(expr, start, end, not) => 
                Expr.Between(f(expr), f(start), f(end), not)
            case Case(branches, default) => 
                Expr.Case(branches.map(b => f(b.whenExpr) -> f(b.thenExpr)), f(default))
            case Window(expr, partitionBy, orderBy, _) =>
                Expr.Window(f(expr), partitionBy.map(f), orderBy.map(s))
            case SubQuery(query) =>
                Expr.SubQuery(createQueryTree(currentLevel + 1, query, tableColumns, tableMetaData, funcMetaData))
            case SubLink(query, linkType) =>
                val exprLinkType = linkType match
                    case SqlSubLinkType.Some | SqlSubLinkType.Any => SubLinkType.Any
                    case SqlSubLinkType.All => SubLinkType.All
                    case SqlSubLinkType.Exists => SubLinkType.Exists
                    case SqlSubLinkType.NotExists => SubLinkType.NotExists
                Expr.SubLink(
                    createQueryTree(currentLevel + 1, query, tableColumns, tableMetaData, funcMetaData),
                    exprLinkType
                )
            case e => throw new AnalysisException("This expression is not supported.")
        
    def createQueryTree(
        currentLevel: Int,
        query: SqlQuery,
        tableColumns: List[TableColumns],
        tableMetaData: List[TableMetaData],
        funcMetaData: List[FuncMetaData]
    ): Query =
        val q = query match
            case s: SqlQuery.Select => s
            case _ =>
                // TODO union
                throw new AnalysisException("???")

        val columnRef = tableColumns ++ collectColumns(currentLevel, q, tableMetaData)

        val f = e => 
            createExpr(currentLevel, e, columnRef, tableMetaData, funcMetaData)

        val s = o =>
            createSort(currentLevel, o, columnRef, tableMetaData, funcMetaData)

        val l = (e: SqlExpr) =>
            e match
                case SqlExpr.NumberLiteral(number) => 
                    val n = BigDecimal(number.toString).setScale(0, BigDecimal.RoundingMode.HALF_UP).toInt
                    if n < 0 
                    then throw new AnalysisException(s"The value of LIMIT cannot be negative.")
                    else n
                case _ => throw new AnalysisException(s"Argument of LIMIT must not contain variables.")

        val tables = createTableEntryList(currentLevel, q, tableColumns, tableMetaData, funcMetaData)
        val fromExpr = createFromExpr(currentLevel, q, tableColumns, tableMetaData, funcMetaData)
        val distnct = q.param.map(p => p == SqlSelectParam.Distinct).getOrElse(false)
        val filter = q.where.map(f)
        val target = q.select.flatMap:
            case _: SqlSelectItem.Wildcard => Nil
            case SqlSelectItem.Item(expr, alias) => TargetEntry(f(expr), alias.get) :: Nil
        val group = q.groupBy.map:
            case SqlGroupItem.Singleton(item) => f(item)
            case _ => throw new AnalysisException(s"GROUPING SETS is not supported.")
        val having = q.having.map(f)
        val sort = q.orderBy.map(s)
        val limitAndOffset = q.limit.map: lo =>
            l(lo.limit) -> l(lo.offset)
        val queryHasAgg = 
            analysisQueryExpr(false)(e => hasAgg(e, funcMetaData))(_ || _)(q)
        val queryHasWindow =
            analysisQueryExpr(false)(hasWindow)(_ || _)(q)
        val queryHasSubLink =
            analysisQueryExpr(false)(hasSubLink)(_ || _)(q)

        Query(
            queryHasAgg,
            queryHasWindow,
            queryHasSubLink,
            distnct,
            tables,
            fromExpr,
            filter,
            target,
            group,
            having,
            sort,
            limitAndOffset
        )

    def collectColumns(currentLevel: Int, table: SqlTable, tableMetaData: List[TableMetaData]): List[TableColumns] =
        table match
            case SqlTable.IdentTable(tableName, alias) =>
                val tableAlias = alias.map(_.tableAlias).getOrElse(tableName)
                val columns = tableMetaData.find(_.name == tableName).get.columns.map(_.name)
                val tableColumnAlias = alias.map(_.columnAlias).getOrElse(Nil)
                val columnAlias = columns.zipWithIndex.map: (c, i) =>
                    tableColumnAlias.lift(i).getOrElse(c)
                TableColumns(currentLevel, tableAlias, columnAlias) :: Nil
            case SqlTable.SubQueryTable(query, _, alias) =>
                val q = query match
                    case s: SqlQuery.Select => s
                    case _ =>
                        // TODO union
                        throw new AnalysisException("???")
                val columns = q.select.flatMap:
                    case SqlSelectItem.Item(_, alias) => alias.get :: Nil
                    case _ => Nil
                val tableColumnAlias = alias.columnAlias
                val columnAlias = columns.zipWithIndex.map: (c, i) =>
                    tableColumnAlias.lift(i).getOrElse(c)
                TableColumns(currentLevel, alias.tableAlias, columnAlias) :: Nil
            case SqlTable.JoinTable(left, _, right, _) =>
                collectColumns(currentLevel, left, tableMetaData) ++ collectColumns(currentLevel, right, tableMetaData)

    def collectColumns(currentLevel: Int, query: SqlQuery.Select, tableMetaData: List[TableMetaData]): List[TableColumns] =
        query.from.flatMap(t => collectColumns(currentLevel, t, tableMetaData))

    def createTableEntryList(
        currentLevel: Int, 
        query: SqlQuery.Select,
        tableColumns: List[TableColumns],
        tableMetaData: List[TableMetaData],
        funcMetaData: List[FuncMetaData]
    ): List[TableEntry] =
        if query.from.size != 1 then throw new AnalysisException("From clause must be have one table.")

        var joinType = SqlJoinType.InnerJoin
        val columnRef = scala.collection.mutable.ListBuffer[TableColumns]()

        def createTableEntry(table: SqlTable): List[TableEntry] =
            table match
                case SqlTable.IdentTable(tableName, alias) =>
                    TableEntry.Relation(tableName, alias.map(a => TableAlias(a.tableAlias, a.columnAlias))) :: Nil
                case SqlTable.SubQueryTable(q, lateral, alias) =>
                    q match
                        case s: SqlQuery.Select => 
                            val subQuery = 
                                if lateral then
                                    columnRef.addAll(tableColumns)
                                    if joinType != SqlJoinType.InnerJoin && joinType != SqlJoinType.LeftJoin then
                                        throw new AnalysisException("In LATERAL, join type must be INNER or LEFT.")
                                    createQueryTree(currentLevel + 1, s, columnRef.toList, tableMetaData, funcMetaData)
                                else createQueryTree(currentLevel + 1, s, tableColumns, tableMetaData, funcMetaData)
                            TableEntry.SubQuery(
                                subQuery,
                                lateral,
                                TableAlias(alias.tableAlias, alias.columnAlias)
                            ) :: Nil
                        // TODO union
                        case s => throw new AnalysisException("???")
                case SqlTable.JoinTable(left, j, right, _) => 
                    joinType = j
                    columnRef.addAll(collectColumns(currentLevel, left, tableMetaData))
                    createTableEntry(left) ++ createTableEntry(right)

        createTableEntry(query.from.head)
                        
    def createFromExpr(
        currentLevel: Int, 
        query: SqlQuery.Select,
        tableColumns: List[TableColumns],
        tableMetaData: List[TableMetaData],
        funcMetaData: List[FuncMetaData]
    ): FromExpr =
        if query.from.size != 1 then throw new AnalysisException("From clause must be have one table.")

        def create(table: SqlTable, index: Int): (FromExpr, Int) =
            table match
                case _: SqlTable.IdentTable => 
                    FromExpr.TableRef(index) -> index
                case _: SqlTable.SubQueryTable => 
                    FromExpr.TableRef(index) -> index
                case SqlTable.JoinTable(left, joinType, right, condition) =>
                    val createLeft = create(left, index)
                    val createRight = create(right, createLeft._2 + 1)
                    val exprJoinType = joinType match
                        case SqlJoinType.InnerJoin => JoinType.Inner
                        case SqlJoinType.LeftJoin => JoinType.Left
                        case SqlJoinType.RightJoin => JoinType.Right
                        case t => throw new AnalysisException(s"${t.joinType} is not supported.")
                    val cond = condition match
                        case None => 
                            SqlExpr.BooleanLiteral(true)
                        case Some(SqlJoinCondition.Using(_)) =>
                            throw new AnalysisException(s"USING clause is not supported.")
                        case Some(SqlJoinCondition.On(e)) =>
                            e
                    val columnRef = 
                        tableColumns ++
                        collectColumns(currentLevel, left, tableMetaData) ++
                        collectColumns(currentLevel, right, tableMetaData)
                    FromExpr.JoinExpr(
                        createLeft._1,
                        exprJoinType,
                        createRight._1,
                        createExpr(currentLevel, cond, columnRef, tableMetaData, funcMetaData)
                    ) -> createRight._2
                    
        create(query.from.head, 0)._1