package sqala.ir

import sqala.util.NonEmptyList
import sqala.ast.expr.SqlBinaryOperator
import sqala.ast.quantifier.SqlQuantifier
import sqala.ast.statement.SqlSetOperator

enum IrQuery:
    case Select(
        quantifier: Option[SqlQuantifier],
        select: NonEmptyList[IrSelectItem],
        from: Option[IrTable],
        where: Option[IrExpr],
        groupBy: Option[IrGroup],
        having: Option[IrExpr],
        orderBy: List[IrOrderingItem],
        limit: Option[IrLimit],
        lock: Option[IrLock]
    )
    case Set(
        left: IrQuery,
        operator: SqlSetOperator,
        right: IrQuery,
        orderBy: List[IrOrderingItem],
        limit: Option[IrLimit]
    )
    case Values(
        values: NonEmptyList[NonEmptyList[IrExpr]],
        orderBy: List[IrOrderingItem],
        limit: Option[IrLimit]
    )
    case With(
        recursive: Boolean,
        withItems: NonEmptyList[IrWithItem],
        query: IrQuery
    )

object IrQuery:
    extension (select: Select)
        /**
         * Returns a copy with the given item appended to the select list.
         */
        def addSelectItem(item: IrSelectItem): Select =
            select.copy(select = select.select :+ item)

        /**
         * Returns a copy with the given condition added to the `WHERE` clause
         * via `AND`.
         */
        def addWhere(condition: IrExpr): Select =
            select.copy(where = select.where.map(IrExpr.Binary(_, SqlBinaryOperator.And, condition)).orElse(Some(condition)))

        /**
         * Returns a copy with the given condition added to the `HAVING` clause
         * via `AND`.
         */
        def addHaving(condition: IrExpr): Select =
            select.copy(having = select.having.map(IrExpr.Binary(_, SqlBinaryOperator.And, condition)).orElse(Some(condition)))