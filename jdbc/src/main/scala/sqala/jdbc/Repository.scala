package sqala.jdbc

import sqala.ast.expr.{SqlBinaryOperator, SqlExpr}
import sqala.ast.order.SqlOrdering
import sqala.ast.quantifier.SqlQuantifier
import sqala.ir.*
import sqala.metadata.{AsSqlExpr, TableMacroImpl}
import sqala.static.dsl.*
import sqala.static.dsl.statement.query.SelectQuery
import sqala.static.dsl.table.Table
import sqala.util.NonEmptyList

import scala.collection.mutable.ListBuffer
import scala.quoted.{Expr, Quotes, Type}

/**
 * A compile-time derived query, parsed from a method-name string literal.
 *
 * The name grammar is a contracted whitelist: a mode prefix
 * (`fetchBy`, `findBy`, `countBy`, `existsBy`; `fetch` also has a
 * `Distinct` variant that deduplicates whole rows),
 * an `And`/`Or` condition list (`And` binds tighter, matching
 * SQL precedence), operator suffixes (`Not`, `GreaterThan`,
 * `GreaterThanEqual`, `LessThan`, `LessThanEqual`, `Like`, `NotLike`,
 * `StartsWith`, `EndsWith`, `In`, `NotIn`, `Between`, `NotBetween`,
 * `IsNull`, `IsNotNull`), and an optional `OrderBy` tail with
 * `Asc`/`Desc` per column.
 *
 * Columns are matched against the entity metadata at compile time with
 * longest-match and backtracking; a name admitting two different parses
 * is rejected. `Option` parameters (mirroring `Option` columns) skip
 * their condition when `None`. Values are parameterized through the
 * normal `AsSqlExpr` pipeline, never inlined; `StartsWith` and
 * `EndsWith` append `%` to the value before binding it.
 *
 * The entry point is `Dynamic` on `JdbcContext`: the called method name
 * is parsed at compile time, so no string literal appears at call sites.
 * Operations without conditions take `(())` as their argument list.
 *
 * {{{
 * val rows: List[User] = db.fetchByAgeAndNameOrderByAgeDesc[User](30, "Alice")
 * val count: Long = db.countByAgeGreaterThan[User](18)
 * val orphans: List[Node] = db.fetchByParentIdIsNull[Node](())
 * }}}
 */
trait Repository[T, N <: String]:
    /** The argument type: a single value, or a tuple in condition order. */
    type Args

    /** The execution result type, fixed by the mode prefix. */
    type R

    /** Builds the filtered select, usable with any query operation. */
    def query(args: Args): SelectQuery[Table[T, Column, 1], EmptyTuple, 1, ManyRows]

    /** Executes the query with the mode's result shape. */
    def run(db: JdbcContext, args: Args)(using
        ec: JdbcExecutionContext,
        d: JdbcDecoder[T],
        l: Logger
    ): R

object Repository:
    type Aux[T, N <: String, A, O] = Repository[T, N]:
        type Args = A
        type R = O

    transparent inline given derived[T, N <: String]: Aux[T, N, ?, ?] =
        ${ derivedImpl[T, N] }

    /** OR of ANDs, dropping skipped (None) conditions and empty groups. */
    private def combine(groups: List[List[Option[IrExpr]]]): Option[IrExpr] =
        groups
            .map(_.flatten.reduceOption((left, right) => IrExpr.Binary(left, SqlBinaryOperator.And, right)))
            .flatten
            .reduceOption((left, right) => IrExpr.Binary(left, SqlBinaryOperator.Or, right))

    private case class Condition(field: String, operator: String)

    private val prefixes = List(
        "fetchDistinctBy", "fetchBy", "findBy", "countBy", "existsBy"
    )

    private val operators = List(
        "GreaterThanEqual" -> "value",
        "LessThanEqual" -> "value",
        "GreaterThan" -> "value",
        "LessThan" -> "value",
        "NotBetween" -> "pair",
        "Between" -> "pair",
        "NotIn" -> "seq",
        "In" -> "seq",
        "NotLike" -> "value",
        "StartsWith" -> "value",
        "EndsWith" -> "value",
        "Like" -> "value",
        "IsNotNull" -> "nullary",
        "IsNull" -> "nullary",
        "Not" -> "value"
    )

    private val likeOperators = List("Like", "NotLike", "StartsWith", "EndsWith")

    def derivedImpl[T: Type, N <: String : Type](using q: Quotes): Expr[Aux[T, N, ?, ?]] =
        import q.reflect.*

        val name = TypeRepr.of[N] match
            case ConstantType(StringConstant(value)) => value
            case _ => report.errorAndAbort("Unsupported repository operation.")

        val prefix = prefixes.find(name.startsWith).getOrElse(report.errorAndAbort("Unsupported repository operation."))
        val body = name.stripPrefix(prefix)
        if body.isEmpty then report.errorAndAbort("Unsupported repository operation.")
        val distinct = prefix.contains("Distinct")
        val mode = prefix.stripSuffix("DistinctBy").stripSuffix("By")

        val entitySymbol = TypeRepr.of[T].typeSymbol
        val fields = entitySymbol.caseFields.map(_.name)
        val typeByField = entitySymbol.caseFields.map(field => field.name -> TypeRepr.of[T].memberType(field)).toMap
        val metaData = TableMacroImpl.tableMetaDataMacro[T]
        val columnByField = metaData.fields.toList.zip(metaData.columns.toList).toMap

        def capitalized(field: String): String = field.head.toUpper.toString + field.tail
        def isUpperAt(text: String, index: Int): Boolean = index < text.length && text(index).isUpper
        def atBoundary(text: String, index: Int): Boolean = index == text.length || text(index).isUpper

        val orderIndex = (0 to body.length - 7).find(index => body.startsWith("OrderBy", index) && isUpperAt(body, index + 7)).getOrElse(-1)
        val conditionSection = if orderIndex < 0 then body else body.substring(0, orderIndex)
        val orderSection = if orderIndex < 0 then "" else body.substring(orderIndex + 7)
        if conditionSection.isEmpty then report.errorAndAbort("Unsupported repository operation.")

        // Enumerate every parse; keywords only cut at camelCase boundaries.
        def parseCondition(position: Int): List[(Condition, Int)] =
            for
                field <- fields if conditionSection.startsWith(capitalized(field), position)
                afterColumn = position + capitalized(field).length
                (operator, _) <- ("", "equal") :: operators.filter((keyword, _) => conditionSection.startsWith(keyword, afterColumn) && atBoundary(conditionSection, afterColumn + keyword.length))
            yield (Condition(field, operator), afterColumn + operator.length)

        def parseAndGroup(position: Int): List[(List[Condition], Int)] =
            parseCondition(position).flatMap: (condition, afterCondition) =>
                if conditionSection.startsWith("And", afterCondition) && isUpperAt(conditionSection, afterCondition + 3) then
                    parseAndGroup(afterCondition + 3).map((conditions, end) => (condition :: conditions, end))
                else
                    List((List(condition), afterCondition))

        def parseOrGroups(position: Int): List[(List[List[Condition]], Int)] =
            parseAndGroup(position).flatMap: (group, afterGroup) =>
                if conditionSection.startsWith("Or", afterGroup) && isUpperAt(conditionSection, afterGroup + 2) then
                    parseOrGroups(afterGroup + 2).map((groups, end) => (group :: groups, end))
                else
                    List((List(group), afterGroup))

        val parses = parseOrGroups(0).filter(_._2 == conditionSection.length).map(_._1).distinct
        if parses.isEmpty then report.errorAndAbort("Unknown column in repository method name.")
        if parses.size > 1 then report.errorAndAbort("Ambiguous repository method name.")
        val groups = parses.head

        def parseOrders(position: Int): List[(String, Boolean)] =
            if position == orderSection.length then Nil
            else
                val field = fields.filter(field => orderSection.startsWith(capitalized(field), position)).sortBy(field => -capitalized(field).length).headOption
                    .getOrElse(report.errorAndAbort("Unknown column in repository method name."))
                val afterColumn = position + capitalized(field).length
                val (descending, nextPosition) =
                    if orderSection.startsWith("Desc", afterColumn) && atBoundary(orderSection, afterColumn + 4) then (true, afterColumn + 4)
                    else if orderSection.startsWith("Asc", afterColumn) && atBoundary(orderSection, afterColumn + 3) then (false, afterColumn + 3)
                    else (false, afterColumn)
                (field, descending) :: parseOrders(nextPosition)

        val orders = parseOrders(0)

        def operatorKind(condition: Condition): String =
            if condition.operator.isEmpty then "value" else operators.toMap.apply(condition.operator)

        def patternFunction(operator: String): Expr[String => String] =
            operator match
                case "StartsWith" => '{ (text: String) => text + "%" }
                case "EndsWith" => '{ (text: String) => "%" + text }
                case _ => '{ (text: String) => text }

        def isOptionField(condition: Condition): Boolean =
            typeByField(condition.field).asType match
                case '[Option[t]] => true
                case '[t] => false

        val conditionLayout = groups.flatten.map: condition =>
            val kind = operatorKind(condition)
            val option = isOptionField(condition)
            val argumentCount = kind match
                case "nullary" => 0
                case "pair" => if option then 1 else 2
                case _ => 1
            (condition, kind, option, argumentCount)
        val totalArguments = conditionLayout.map(_._4).sum

        val argumentTypes = ListBuffer[TypeRepr]()
        conditionLayout.foreach: (condition, kind, _, _) =>
            if likeOperators.contains(condition.operator) then
                val elementType = typeByField(condition.field).asType match
                    case '[Option[t]] => TypeRepr.of[t]
                    case '[t] => TypeRepr.of[t]
                if !(elementType =:= TypeRepr.of[String]) then
                    report.errorAndAbort("Unsupported repository operation.")
            typeByField(condition.field).asType match
                case '[Option[t]] =>
                    kind match
                        case "nullary" => ()
                        case "seq" => argumentTypes.addOne(TypeRepr.of[Option[Seq[t]]])
                        case "pair" => argumentTypes.addOne(TypeRepr.of[Option[(t, t)]])
                        case _ => argumentTypes.addOne(TypeRepr.of[Option[t]])
                case '[t] =>
                    kind match
                        case "nullary" => ()
                        case "seq" => argumentTypes.addOne(TypeRepr.of[Seq[t]])
                        case "pair" =>
                            argumentTypes.addOne(TypeRepr.of[t])
                            argumentTypes.addOne(TypeRepr.of[t])
                        case _ => argumentTypes.addOne(TypeRepr.of[t])

        def typesToTupleType(list: List[TypeRepr]): TypeRepr =
            list match
                case head :: tail =>
                    head.asType match
                        case '[h] =>
                            typesToTupleType(tail).asType match
                                case '[type t <: Tuple; t] => TypeRepr.of[h *: t]
                case Nil => TypeRepr.of[EmptyTuple]

        val argumentType =
            if argumentTypes.isEmpty then TypeRepr.of[Unit]
            else if argumentTypes.size == 1 then argumentTypes.head
            else typesToTupleType(argumentTypes.toList)

        val resultType: TypeRepr = mode match
            case "fetch" => TypeRepr.of[List[T]]
            case "find" => TypeRepr.of[Option[T]]
            case "count" => TypeRepr.of[Long]
            case _ => TypeRepr.of[Boolean]

        val metadataExpression = TableMacroImpl.tableMetaData[T]

        argumentType.asType match
            case '[args] =>
                resultType.asType match
                    case '[r] =>
                        // Closed accessor functions: each binds its own parameter.
                        def argumentFunction(index: Int): Expr[?] =
                            if totalArguments == 1 then '{ (a: args) => a }
                            else
                                def accessor(types: List[TypeRepr], position: Int): Expr[?] =
                                    types match
                                        case head :: tail =>
                                            head.asType match
                                                case '[h] =>
                                                    typesToTupleType(tail).asType match
                                                        case '[type rest <: Tuple; rest] =>
                                                            if position == 0 then '{ (a: h *: rest) => a.head }
                                                            else
                                                                types(position).asType match
                                                                    case '[e] =>
                                                                        '{ (a: h *: rest) => ${accessor(tail, position - 1).asExprOf[rest => e]}(a.tail) }
                                        case Nil => report.errorAndAbort("Unsupported repository operation.")
                                accessor(argumentTypes.toList, index)

                        var slotIndex = 0
                        val conditionFunctions = conditionLayout.map: (condition, kind, _, argumentCount) =>
                            val index = slotIndex
                            slotIndex += argumentCount
                            val columnName = Expr(columnByField(condition.field))
                            typeByField(condition.field).asType match
                                case '[Option[t]] =>
                                    val asSqlExpr = Expr.summon[AsSqlExpr[t]].getOrElse(report.errorAndAbort("This value cannot be converted to an SQL expression."))
                                    val comparisonOperator = condition.operator match
                                        case "Not" => '{ SqlBinaryOperator.NotEqual }
                                        case "GreaterThan" => '{ SqlBinaryOperator.GreaterThan }
                                        case "GreaterThanEqual" => '{ SqlBinaryOperator.GreaterThanEqual }
                                        case "LessThan" => '{ SqlBinaryOperator.LessThan }
                                        case "LessThanEqual" => '{ SqlBinaryOperator.LessThanEqual }
                                        case _ => '{ SqlBinaryOperator.Equal }
                                    kind match
                                        case "nullary" =>
                                            '{ (table: IrTable, _: args) =>
                                                Some(IrExpr.Binary(IrExpr.Column(Some(table), $columnName), SqlBinaryOperator.Is(${Expr(condition.operator == "IsNotNull")}), IrExpr.Native(SqlExpr.NullLiteral)))
                                            }
                                        case "seq" =>
                                            val values = argumentFunction(index).asExprOf[args => Option[Seq[t]]]
                                            '{ (table: IrTable, args: args) =>
                                                $values(args).map(sequence =>
                                                    if sequence.isEmpty then IrExpr.Native(SqlExpr.InlinedLiteral(SqlExpr.BooleanLiteral(${Expr(condition.operator == "NotIn")})))
                                                    else IrExpr.In(IrExpr.Column(Some(table), $columnName), IrInRightOperand.Values(sequence.map(value => IrExpr.Native($asSqlExpr.asSqlExpr(value, false))).toList), ${Expr(condition.operator == "NotIn")})
                                                )
                                            }
                                        case "pair" =>
                                            val pair = argumentFunction(index).asExprOf[args => Option[(t, t)]]
                                            '{ (table: IrTable, args: args) =>
                                                $pair(args).map((start, end) =>
                                                    IrExpr.Between(IrExpr.Column(Some(table), $columnName), IrExpr.Native($asSqlExpr.asSqlExpr(start, false)), IrExpr.Native($asSqlExpr.asSqlExpr(end, false)), ${Expr(condition.operator == "NotBetween")})
                                                )
                                            }
                                        case _ =>
                                            if likeOperators.contains(condition.operator) then
                                                val value = argumentFunction(index).asExprOf[args => Option[String]]
                                                val stringExpr = Expr.summon[AsSqlExpr[String]].getOrElse(report.errorAndAbort("This value cannot be converted to an SQL expression."))
                                                val pattern = patternFunction(condition.operator)
                                                '{ (table: IrTable, args: args) =>
                                                    $value(args).map(text => IrExpr.Like(IrExpr.Column(Some(table), $columnName), IrExpr.Native($stringExpr.asSqlExpr($pattern(text), false)), ${Expr(condition.operator == "NotLike")}))
                                                }
                                            else
                                                val value = argumentFunction(index).asExprOf[args => Option[t]]
                                                '{ (table: IrTable, args: args) =>
                                                    $value(args).map(element => IrExpr.Binary(IrExpr.Column(Some(table), $columnName), $comparisonOperator, IrExpr.Native($asSqlExpr.asSqlExpr(element, false))))
                                                }
                                case '[t] =>
                                    val asSqlExpr = Expr.summon[AsSqlExpr[t]].getOrElse(report.errorAndAbort("This value cannot be converted to an SQL expression."))
                                    val comparisonOperator = condition.operator match
                                        case "Not" => '{ SqlBinaryOperator.NotEqual }
                                        case "GreaterThan" => '{ SqlBinaryOperator.GreaterThan }
                                        case "GreaterThanEqual" => '{ SqlBinaryOperator.GreaterThanEqual }
                                        case "LessThan" => '{ SqlBinaryOperator.LessThan }
                                        case "LessThanEqual" => '{ SqlBinaryOperator.LessThanEqual }
                                        case _ => '{ SqlBinaryOperator.Equal }
                                    kind match
                                        case "nullary" =>
                                            '{ (table: IrTable, _: args) =>
                                                Some(IrExpr.Binary(IrExpr.Column(Some(table), $columnName), SqlBinaryOperator.Is(${Expr(condition.operator == "IsNotNull")}), IrExpr.Native(SqlExpr.NullLiteral)))
                                            }
                                        case "seq" =>
                                            val values = argumentFunction(index).asExprOf[args => Seq[t]]
                                            '{ (table: IrTable, args: args) =>
                                                val sequence = $values(args)
                                                Some(
                                                    if sequence.isEmpty then IrExpr.Native(SqlExpr.InlinedLiteral(SqlExpr.BooleanLiteral(${Expr(condition.operator == "NotIn")})))
                                                    else IrExpr.In(IrExpr.Column(Some(table), $columnName), IrInRightOperand.Values(sequence.map(value => IrExpr.Native($asSqlExpr.asSqlExpr(value, false))).toList), ${Expr(condition.operator == "NotIn")})
                                                )
                                            }
                                        case "pair" =>
                                            val start = argumentFunction(index).asExprOf[args => t]
                                            val end = argumentFunction(index + 1).asExprOf[args => t]
                                            '{ (table: IrTable, args: args) =>
                                                Some(IrExpr.Between(IrExpr.Column(Some(table), $columnName), IrExpr.Native($asSqlExpr.asSqlExpr($start(args), false)), IrExpr.Native($asSqlExpr.asSqlExpr($end(args), false)), ${Expr(condition.operator == "NotBetween")}))
                                            }
                                        case _ =>
                                            if likeOperators.contains(condition.operator) then
                                                val value = argumentFunction(index).asExprOf[args => String]
                                                val stringExpr = Expr.summon[AsSqlExpr[String]].getOrElse(report.errorAndAbort("This value cannot be converted to an SQL expression."))
                                                val pattern = patternFunction(condition.operator)
                                                '{ (table: IrTable, args: args) =>
                                                    Some(IrExpr.Like(IrExpr.Column(Some(table), $columnName), IrExpr.Native($stringExpr.asSqlExpr($pattern($value(args)), false)), ${Expr(condition.operator == "NotLike")}))
                                                }
                                            else
                                                val value = argumentFunction(index).asExprOf[args => t]
                                                '{ (table: IrTable, args: args) =>
                                                    Some(IrExpr.Binary(IrExpr.Column(Some(table), $columnName), $comparisonOperator, IrExpr.Native($asSqlExpr.asSqlExpr($value(args), false))))
                                                }

                        val functionIterator = conditionFunctions.iterator
                        val conditionsFunction = '{ (table: IrTable, args: args) =>
                            ${Expr.ofList(groups.map(group => Expr.ofList(group.map(_ => '{ ${functionIterator.next()}(table, args) }))))}
                        }
                        '{
                            new Repository[T, N]:
                                type Args = args
                                type R = r

                                def query(args: Args): SelectQuery[Table[T, Column, 1], EmptyTuple, 1, ManyRows] =
                                    val metaData = $metadataExpression
                                    val irTable = IrTable.Ident(metaData.tableName, Nil)
                                    val table = Table[T, Column, 1](irTable, metaData)
                                    val selectItems = metaData.columns.toList.zipWithIndex.map((columnName, index) => IrSelectItem(IrExpr.Column(Some(irTable), columnName), "c" + (index + 1)))
                                    val whereClause = Repository.combine($conditionsFunction(irTable, args))
                                    val orderItems = ${Expr.ofList(orders.map((field, descending) =>
                                        '{ IrOrderingItem(IrExpr.Column(Some(irTable), ${Expr(columnByField(field))}), ${if descending then '{ SqlOrdering.Desc } else '{ SqlOrdering.Asc }}, None) }
                                    ))}
                                    val selectTree: IrQuery.Select = IrQuery.Select(
                                        ${if distinct then '{ Some(SqlQuantifier.Distinct) } else '{ None }},
                                        NonEmptyList(selectItems.head, selectItems.tail),
                                        Some(irTable),
                                        whereClause,
                                        None,
                                        None,
                                        orderItems,
                                        None,
                                        None
                                    )
                                    SelectQuery[Table[T, Column, 1], EmptyTuple, 1, ManyRows](table, selectTree)(using QueryContext[1]())

                                def run(db: JdbcContext, args: Args)(using
                                    ec: JdbcExecutionContext,
                                    d: JdbcDecoder[T],
                                    l: Logger
                                ): R = ${
                                    mode match
                                        case "fetch" => '{ db.fetchTo[T](query(args)) }.asExprOf[r]
                                        case "find" => '{ db.findTo[T](query(args)) }.asExprOf[r]
                                        case "count" => '{ db.fetchCount(query(args)) }.asExprOf[r]
                                        case _ => '{ db.fetchExists(query(args)) }.asExprOf[r]
                                }
                        }.asExprOf[Aux[T, N, args, r]]