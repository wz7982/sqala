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
        def addSelectItem(item: IrSelectItem): Select =
            select.copy(select = select.select :+ item)

        def addWhere(condition: IrExpr): Select =
            select.copy(where = select.where.map(IrExpr.Binary(_, SqlBinaryOperator.And, condition)).orElse(Some(condition)))

        def addHaving(condition: IrExpr): Select =
            select.copy(having = select.having.map(IrExpr.Binary(_, SqlBinaryOperator.And, condition)).orElse(Some(condition)))

enum IrInsertMode:
    case Values(values: NonEmptyList[NonEmptyList[IrExpr]])
    case Subquery(query: IrQuery)

case class IrUpdateSetPair(column: String, value: IrExpr)

enum IrStatement:
    case Delete(table: IrTable.Ident, where: Option[IrExpr])
    case Insert(table: IrTable.Ident, columns: List[String], mode: IrInsertMode)
    case Update(table: IrTable.Ident, setPairs: NonEmptyList[IrUpdateSetPair], where: Option[IrExpr])
    case Upsert(
        table: IrTable.Ident,
        columns: NonEmptyList[String],
        values: NonEmptyList[IrExpr],
        primaryKeys: NonEmptyList[String],
        updateColumns: NonEmptyList[String]
    )

object IrStatement:
    extension (statement: Delete)
        def addWhere(condition: IrExpr): Delete =
            statement.copy(where = statement.where.map(IrExpr.Binary(_, SqlBinaryOperator.And, condition)).orElse(Some(condition)))

    extension (statement: Update)
        def addWhere(condition: IrExpr): Update =
            statement.copy(where = statement.where.map(IrExpr.Binary(_, SqlBinaryOperator.And, condition)).orElse(Some(condition)))