package sqala.ir

import sqala.ast.statement.SqlQuery
import sqala.ast.table.SqlTable
import sqala.ast.expr.SqlExpr
import sqala.ast.table.SqlTableAlias
import sqala.ast.statement.SqlSelectItem
import sqala.ast.quantifier.SqlQuantifier
import sqala.ast.expr.SqlInRightOperand
import sqala.ast.expr.SqlCaseBranch
import sqala.ast.table.SqlJoinCondition
import sqala.ast.order.SqlOrderingItem
import sqala.ast.window.SqlWindowFrameBound
import sqala.ast.window.SqlWindowFrame
import sqala.ast.window.SqlWindow
import sqala.ast.expr.SqlJsonObjectItem
import sqala.ast.expr.SqlJsonArrayItem
import sqala.ast.group.SqlGroupingItem
import sqala.ast.limit.SqlLimit
import sqala.ast.limit.SqlFetch
import sqala.ast.statement.SqlLock
import sqala.ast.statement.SqlLockWaitMode
import sqala.ast.group.SqlGroup
import sqala.ast.expr.SqlBinaryOperator
import sqala.util.NonEmptyList
import sqala.ast.expr.SqlType
import sqala.ast.expr.SqlUnaryOperator
import sqala.ast.statement.SqlWithItem

private[sqala] class ToAst:
    private var tableIndex = 0

    private val tableAliasMap = java.util.IdentityHashMap[IrTable, String]()

    private var cteIndex = 0

    private val cteNames = scala.collection.mutable.Stack[List[String]]()

    private def queryToAst(query: IrQuery): SqlQuery =
        query match
            case s: IrQuery.Select =>
                val fromAst = s.from.map(tableToAst).toList
                val selectAst = s.select.map(selectItemToAst)
                val whereAst = s.where.map(exprToAst)
                val groupAst = s.groupBy.map: g =>
                    SqlGroup(
                        g.quantifier,
                        g.items.map(groupingItemToAst)
                    )
                val havingAst = s.having.map(exprToAst)
                val orderAst = s.orderBy.map(orderingItemToAst)
                val limitAst = s.limit.map(limitToAst)
                val lockAst = s.lock.map(lockToAst)
                SqlQuery.Select(
                    s.quantifier,
                    selectAst,
                    fromAst,
                    whereAst,
                    groupAst,
                    havingAst,
                    Nil,
                    None,
                    orderAst,
                    limitAst,
                    lockAst
                )
            case s: IrQuery.Set =>
                val leftAst = queryToAst(s.left)
                val rightAst = queryToAst(s.right)
                val orderAst = s.orderBy.map(orderingItemToAst)
                val limitAst = s.limit.map(limitToAst)
                SqlQuery.Set(leftAst, s.operator, None, rightAst, orderAst, limitAst)
            case v: IrQuery.Values =>
                SqlQuery.Values(
                    v.values.map(row => row.map(exprToAst)),
                    v.orderBy.map(orderingItemToAst),
                    v.limit.map(limitToAst)
                )
            case w: IrQuery.With =>
                val names = w.withItems.map(_ => freshCteName())
                cteNames.push(names.toList)
                val itemsAst = w.withItems.zip(names).map: (item, name) =>
                    SqlWithItem(name, item.columns, queryToAst(item.query))
                val queryAst = queryToAst(w.query)
                cteNames.pop()
                SqlQuery.With(w.recursive, itemsAst, queryAst)

    private def tableToAst(table: IrTable): SqlTable =
        table match
            case IrTable.Ident(name, columnAliases) =>
                val alias = fetchAlias(table)
                SqlTable.Ident(name, tableAliasToAst(alias, columnAliases), None, None, None)
            case IrTable.Subquery(withLateral, query, columnAliases) =>
                val queryAst = queryToAst(query)
                val alias = fetchAlias(table)
                SqlTable.Subquery(withLateral, queryAst, tableAliasToAst(alias, columnAliases), None)
            case IrTable.Join(left, joinType, right, on) =>
                val leftAst = tableToAst(left)
                val rightAst = tableToAst(right)
                val onAst = on.map(o => SqlJoinCondition.On(exprToAst(o)))
                SqlTable.Join(leftAst, joinType, rightAst, onAst)
            case IrTable.Cte(index, columnAliases) =>
                val name = cteNames.top(index)
                SqlTable.Ident(name, tableAliasToAst(fetchAlias(table), columnAliases), None, None, None)

    private def exprToAst(expr: IrExpr): SqlExpr =
        expr match
            case IrExpr.Native(expr) =>
                expr
            case IrExpr.Column(table, name) =>
                val tableAlias = fetchAlias(table)
                SqlExpr.Column(Some(tableAlias), name)
            case IrExpr.Tuple(items) =>
                SqlExpr.Tuple(items.map(exprToAst))
            case IrExpr.Array(items) =>
                SqlExpr.Array(items.map(exprToAst))
            case IrExpr.Unary(SqlUnaryOperator.Not, expr) =>
                expr match
                    case IrExpr.Native(SqlExpr.BooleanLiteral(boolean)) =>
                        SqlExpr.BooleanLiteral(!boolean)
                    case IrExpr.Binary(left, SqlBinaryOperator.Equal, right) =>
                        SqlExpr.Binary(exprToAst(left), SqlBinaryOperator.NotEqual, exprToAst(right))
                    case IrExpr.Binary(left, SqlBinaryOperator.NotEqual, right) =>
                        SqlExpr.Binary(exprToAst(left), SqlBinaryOperator.Equal, exprToAst(right))
                    case IrExpr.Binary(left, SqlBinaryOperator.Is(not), right) =>
                        SqlExpr.Binary(exprToAst(left), SqlBinaryOperator.Is(!not), exprToAst(right))
                    case IrExpr.Like(expr, pattern, not) =>
                        SqlExpr.Like(exprToAst(expr), exprToAst(pattern), None, !not)
                    case IrExpr.In(expr, in, withNot) =>
                        exprToAst(IrExpr.In(expr, in, !withNot))
                    case IrExpr.Between(expr, between, and, not) =>
                        SqlExpr.Between(exprToAst(expr), exprToAst(between), exprToAst(and), !not)
                    case IrExpr.Unary(SqlUnaryOperator.Not, expr) =>
                        exprToAst(expr)
                    case _ =>
                        SqlExpr.Unary(SqlUnaryOperator.Not, exprToAst(expr))
            case IrExpr.Unary(operator, expr) =>
                SqlExpr.Unary(operator, exprToAst(expr))
            case IrExpr.Binary(left, SqlBinaryOperator.Div, right) =>
                SqlExpr.Binary(
                    SqlExpr.Cast(exprToAst(left), SqlType.Decimal(Some(38, 10))),
                    SqlBinaryOperator.Div,
                    SqlExpr.Cast(exprToAst(right), SqlType.Decimal(Some(38, 10)))
                )
            case IrExpr.Binary(left, operator, right) =>
                SqlExpr.Binary(exprToAst(left), operator, exprToAst(right))
            case IrExpr.In(expr, in, withNot) =>
                in match
                    case IrInRightOperand.Values(Nil) =>
                        val operatorAst =
                            if withNot then SqlBinaryOperator.NotEqual else SqlBinaryOperator.Equal
                        SqlExpr.Binary(
                            SqlExpr.NumberLiteral(1),
                            operatorAst,
                            SqlExpr.NumberLiteral(0)
                        )
                    case IrInRightOperand.Values(e :: es) =>
                        SqlExpr.In(
                            exprToAst(expr),
                            SqlInRightOperand.Values(NonEmptyList(exprToAst(e), es.map(exprToAst))),
                            withNot
                        )
                    case IrInRightOperand.Subquery(query) =>
                        SqlExpr.In(exprToAst(expr), SqlInRightOperand.Subquery(queryToAst(query)), withNot)
            case IrExpr.Between(expr, start, end, withNot) =>
                SqlExpr.Between(exprToAst(expr), exprToAst(start), exprToAst(end), withNot)
            case IrExpr.Like(expr, pattern, withNot) =>
                SqlExpr.Like(exprToAst(expr), exprToAst(pattern), None, withNot)
            case IrExpr.Case(branches, default) =>
                SqlExpr.Case(branches.map(caseBranchToAst), default.map(exprToAst))
            case IrExpr.Coalesce(items) =>
                SqlExpr.Coalesce(items.map(exprToAst))
            case IrExpr.NullIf(expr, test) =>
                SqlExpr.NullIf(exprToAst(expr), exprToAst(test))
            case IrExpr.Cast(expr, t) =>
                SqlExpr.Cast(exprToAst(expr), t)
            case IrExpr.Window(expr, partitionBy, orderBy, frame) =>
                val partitionByAst = partitionBy.map(exprToAst)
                val orderByAst = orderBy.map(orderingItemToAst)
                val frameAst = frame.map(windowFrameToAst)
                val windowAst = SqlWindow.Inlined(None, partitionByAst, orderByAst, frameAst)
                SqlExpr.Window(exprToAst(expr), windowAst)
            case IrExpr.Subquery(query) =>
                SqlExpr.Subquery(queryToAst(query))
            case IrExpr.ExistsPredicate(query) =>
                SqlExpr.ExistsPredicate(queryToAst(query))
            case IrExpr.QuantifiedComparisonPredicate(expr, operator, quantifier, query) =>
                SqlExpr.QuantifiedComparisonPredicate(
                    exprToAst(expr),
                    operator,
                    quantifier,
                    queryToAst(query)
                )
            case IrExpr.Grouping(items) =>
                SqlExpr.Grouping(items.map(exprToAst))
            case IrExpr.IdentFunc(name) =>
                SqlExpr.IdentFunc(name)
            case IrExpr.SubstringFunc(expr, from, forOpt) =>
                SqlExpr.SubstringFunc(exprToAst(expr), exprToAst(from), forOpt.map(exprToAst))
            case IrExpr.OverlayFunc(expr, placing, from, forOpt) =>
                SqlExpr.OverlayFunc(
                    exprToAst(expr),
                    exprToAst(placing),
                    exprToAst(from),
                    forOpt.map(exprToAst)
                )
            case IrExpr.PositionFunc(expr, in) =>
                SqlExpr.PositionFunc(exprToAst(expr), exprToAst(in))
            case IrExpr.ExtractFunc(unit, expr) =>
                SqlExpr.ExtractFunc(unit, exprToAst(expr))
            case IrExpr.JsonObjectAggFunc(key, value, filter) =>
                val itemAst = SqlJsonObjectItem(exprToAst(key), exprToAst(value))
                val filterAst = filter.map(exprToAst)
                SqlExpr.JsonObjectAggFunc(itemAst, None, None, None, filterAst)
            case IrExpr.JsonArrayAggFunc(value, orderBy, filter) =>
                val itemAst = SqlJsonArrayItem(exprToAst(value), None)
                val orderByAst = orderBy.map(orderingItemToAst)
                val filterAst = filter.map(exprToAst)
                SqlExpr.JsonArrayAggFunc(itemAst, orderByAst, None, None, filterAst)
            case IrExpr.CountAsteriskFunc(filter) =>
                SqlExpr.CountAsteriskFunc(None, filter.map(exprToAst))
            case IrExpr.ListAggFunc(quantifier, expr, separator, withinGroup, filter) =>
                val withinGroupAst = withinGroup.map(orderingItemToAst)
                val filterAst = filter.map(exprToAst)
                SqlExpr.ListAggFunc(quantifier, exprToAst(expr), exprToAst(separator), None, withinGroupAst, filterAst)
            case IrExpr.NullsTreatmentFunc(name, args, nullsMode) =>
                val argsAst = args.map(exprToAst)
                SqlExpr.NullsTreatmentFunc(name, argsAst, nullsMode)
            case IrExpr.NthValueFunc(expr, row, fromMode, nullsMode) =>
                SqlExpr.NthValueFunc(
                    exprToAst(expr),
                    exprToAst(row),
                    fromMode,
                    nullsMode
                )
            case IrExpr.GeneralFunc(quantifier, name, args, orderBy, withinGroup, filter) =>
                val argsAst = args.map(exprToAst)
                val orderByAst = orderBy.map(orderingItemToAst)
                val withinGroupAst = withinGroup.map(orderingItemToAst)
                val filterAst = filter.map(exprToAst)
                SqlExpr.GeneralFunc(
                    quantifier,
                    name,
                    argsAst,
                    orderByAst,
                    withinGroupAst,
                    filterAst
                )
            case IrExpr.MatchPhase(phase, expr) =>
                SqlExpr.MatchPhase(phase, exprToAst(expr))

    private def caseBranchToAst(branch: IrCaseBranch): SqlCaseBranch =
        SqlCaseBranch(exprToAst(branch.when), exprToAst(branch.`then`))

    private def orderingItemToAst(item: IrOrderingItem): SqlOrderingItem =
        SqlOrderingItem(
            exprToAst(item.expr),
            Some(item.ordering),
            item.nullsOrdering
        )

    private def windowFrameBoundToAst(bound: IrWindowFrameBound): SqlWindowFrameBound =
        bound match
            case IrWindowFrameBound.CurrentRow =>
                SqlWindowFrameBound.CurrentRow
            case IrWindowFrameBound.UnboundedPreceding =>
                SqlWindowFrameBound.UnboundedPreceding
            case IrWindowFrameBound.Preceding(n) =>
                SqlWindowFrameBound.Preceding(exprToAst(n))
            case IrWindowFrameBound.UnboundedFollowing =>
                SqlWindowFrameBound.UnboundedFollowing
            case IrWindowFrameBound.Following(n) =>
                SqlWindowFrameBound.Following(exprToAst(n))

    private def windowFrameToAst(frame: IrWindowFrame): SqlWindowFrame =
        frame match
            case IrWindowFrame.Start(unit, start, excludeMode) =>
                SqlWindowFrame.Start(
                    Nil,
                    unit,
                    windowFrameBoundToAst(start),
                    excludeMode,
                    None
                )
            case IrWindowFrame.Between(unit, start, end, excludeMode) =>
                SqlWindowFrame.Between(
                    Nil,
                    unit,
                    windowFrameBoundToAst(start),
                    windowFrameBoundToAst(end),
                    excludeMode,
                    None
                )

    private def selectItemToAst(item: IrSelectItem): SqlSelectItem =
        SqlSelectItem.Expr(exprToAst(item.expr), Some(item.alias))

    private def groupingItemToAst(item: IrGroupingItem): SqlGroupingItem =
        item match
            case IrGroupingItem.EmptyGroup =>
                SqlGroupingItem.EmptyGroup
            case IrGroupingItem.Expr(expr) =>
                SqlGroupingItem.Expr(exprToAst(expr))
            case IrGroupingItem.Cube(items) =>
                SqlGroupingItem.Cube(items.map(exprToAst))
            case IrGroupingItem.Rollup(items) =>
                SqlGroupingItem.Rollup(items.map(exprToAst))
            case IrGroupingItem.GroupingSets(items) =>
                SqlGroupingItem.GroupingSets(items.map(groupingItemToAst))

    private def limitToAst(limit: IrLimit): SqlLimit =
        SqlLimit(
            limit.offset.map(exprToAst),
            limit.fetch.map: f =>
                SqlFetch(
                    exprToAst(f.limit),
                    f.unit,
                    f.mode
                )
        )

    private def lockToAst(lock: IrLock): SqlLock =
        def modeToAst(mode: IrLockWaitMode): SqlLockWaitMode =
            mode match
                case IrLockWaitMode.NoWait =>
                    SqlLockWaitMode.NoWait
                case IrLockWaitMode.SkipLocked =>
                    SqlLockWaitMode.SkipLocked

        lock match
            case IrLock.Update(mode) =>
                SqlLock.Update(mode.map(modeToAst))
            case IrLock.Share(mode) =>
                SqlLock.Share(mode.map(modeToAst))

    private def tableAliasToAst(alias: String, columnAliases: List[String]): Option[SqlTableAlias] =
        Some(SqlTableAlias(alias, columnAliases))

    private def fetchAlias(table: IrTable): String =
        tableAliasMap.computeIfAbsent(
            table,
            _ =>
                tableIndex += 1
                s"t$tableIndex"
        )

    private def freshCteName(): String =
        cteIndex += 1
        s"cte$cteIndex"

    def toAst(expr: IrExpr): SqlExpr =
        exprToAst(expr)

    def toAst(query: IrQuery): SqlQuery =
        queryToAst(query)