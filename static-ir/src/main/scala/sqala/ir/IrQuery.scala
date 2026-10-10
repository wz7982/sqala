package sqala.ir

import sqala.util.NonEmptyList
import sqala.ast.expr.SqlBinaryOperator
import sqala.ast.quantifier.SqlQuantifier

enum IrQuery:
    case Select(
        quantifier: Option[SqlQuantifier],
        select: NonEmptyList[IrSelectItem],
        from: IrTable,
        where: Option[IrExpr],
        groupBy: Option[IrGroup],
        having: Option[IrExpr],
        orderBy: List[IrOrderingItem],
        limit: Option[IrLimit],
        lock: Option[IrLock]
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