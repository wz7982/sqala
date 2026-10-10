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

private[sqala] class ToAst:
    var tableIndex = 0

    val tableAliasMap = java.util.IdentityHashMap[IrTable, String]()

    private def queryToAst(query: IrQuery): SqlQuery =
        query match
            case s: IrQuery.Select =>
                val fromAst = tableToAst(s.from)
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
                    List(fromAst),
                    whereAst,
                    groupAst,
                    havingAst,
                    Nil,
                    None,
                    orderAst,
                    limitAst,
                    lockAst
                )

    private def tableToAst(table: IrTable): SqlTable =
        table match
            case IrTable.Ident(name) =>
                val alias = fetchAlias(table)
                SqlTable.Ident(name, tableAliasToAst(alias), None, None, None)
            case IrTable.Subquery(withLateral, query) =>
                val alias = fetchAlias(table)
                SqlTable.Subquery(withLateral, queryToAst(query), tableAliasToAst(alias), None)
            case IrTable.Join(left, joinType, right, on) =>
                val leftAst = tableToAst(left)
                val rightAst = tableToAst(right)
                val onAst = on.map(o => SqlJoinCondition.On(exprToAst(o)))
                SqlTable.Join(leftAst, joinType, rightAst, onAst)

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
            case IrExpr.Unary(operator, expr) =>
                SqlExpr.Unary(operator, exprToAst(expr))
            case IrExpr.Binary(left, operator, right) =>
                SqlExpr.Binary(exprToAst(left), operator, exprToAst(right))
            case IrExpr.In(expr, in, withNot) =>
                SqlExpr.In(exprToAst(expr), inRightOperandToAst(in), withNot)
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

    private def inRightOperandToAst(operand: IrInRightOperand): SqlInRightOperand =
        operand match
            case IrInRightOperand.Subquery(query) =>
                SqlInRightOperand.Subquery(queryToAst(query))
            case IrInRightOperand.Values(values) =>
                SqlInRightOperand.Values(values.map(exprToAst))

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

    private def tableAliasToAst(alias: String): Option[SqlTableAlias] =
        Some(SqlTableAlias(alias, Nil))

    private def fetchAlias(table: IrTable): String =
        tableAliasMap.computeIfAbsent(
            table,
            _ =>
                tableIndex += 1
                s"t$tableIndex"
        )

    def toAst(query: IrQuery): SqlQuery =
        queryToAst(query)